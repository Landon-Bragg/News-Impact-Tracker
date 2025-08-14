import os, time, json, re, uuid, signal, logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
import yaml
import feedparser
import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Company name to ticker mapping
COMPANY_MAP = {
    "apple": "AAPL",
    "microsoft": "MSFT", 
    "google": "GOOGL", "alphabet": "GOOGL",
    "amazon": "AMZN",
    "tesla": "TSLA",
    "nvidia": "NVDA",
    "meta": "META", "facebook": "META",
    "netflix": "NFLX",
    "intel": "INTC",
    "amd": "AMD",
    "oracle": "ORCL",
    "salesforce": "CRM",
}

def add_name_matches(text: str, tickers: set):
    """Add ticker symbols based on company name mentions"""
    if not text:
        return tickers
    
    t = text.lower()
    for name, sym in COMPANY_MAP.items():
        if name in t:
            tickers.add(sym)
    return tickers

# Configuration
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_NEWS", "news_headlines")
SYMBOLS = set([s.strip().upper() for s in os.getenv("SYMBOLS","AAPL,MSFT,GOOGL").split(",") if s.strip()])

# Ticker extraction regex
TICKER_RX = re.compile(r'(?:^|[^A-Z$])\$?([A-Z]{1,5})(?=$|[^A-Z])')

def extract_tickers(text: str):
    """Extract ticker symbols from text"""
    if not text:
        return []
    
    # Find potential tickers with regex
    cands = set(m.group(1) for m in TICKER_RX.finditer(text))
    
    # Add company name matches
    cands = add_name_matches(text, cands)
    
    # Filter to known symbols if universe is defined
    if SYMBOLS:
        cands = cands & SYMBOLS
    
    return sorted(list(cands))

def load_feeds():
    """Load RSS feeds from configuration file"""
    cfg_path = os.path.join(os.path.dirname(__file__), "rss_feeds.yml")
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        feeds = cfg.get("feeds", [])
        logger.info(f"Loaded {len(feeds)} RSS feeds")
        return feeds
    except Exception as e:
        logger.error(f"Failed to load RSS feeds config: {e}")
        return []

def create_producer():
    """Create Kafka producer with compatible configuration"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Remove incompatible configurations
            producer = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                acks='all'
                # Removed: enable_idempotence=True (not supported in kafka-python 2.0.2)
            )
            logger.info(f"Connected to Kafka broker: {BROKER}")
            return producer
        except Exception as e:
            logger.warning(f"Kafka connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

def fetch_feed_with_timeout(feed_url, timeout=10):
    """Fetch RSS feed with timeout and user agent"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)'
    }
    
    try:
        response = requests.get(feed_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return feedparser.parse(response.content)
    except requests.RequestException as e:
        logger.warning(f"HTTP error fetching {feed_url}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error fetching {feed_url}: {e}")
        return None

def process_feed(feed, seen_ids, producer):
    """Process a single RSS feed"""
    feed_name = feed.get("name", "Unknown")
    feed_url = feed.get("url", "")
    
    if not feed_url:
        logger.warning(f"No URL for feed: {feed_name}")
        return 0
    
    try:
        logger.debug(f"Fetching feed: {feed_name}")
        parsed = fetch_feed_with_timeout(feed_url)
        
        if not parsed or not hasattr(parsed, 'entries'):
            logger.warning(f"No entries found in feed: {feed_name}")
            return 0
        
        new_count = 0
        for entry in parsed.entries[:50]:  # Limit to recent entries
            try:
                # Generate stable entry ID
                entry_id = (
                    getattr(entry, "id", None) or 
                    getattr(entry, "link", None) or 
                    f"{feed_name}_{hash(getattr(entry, 'title', ''))}"
                )
                
                if entry_id in seen_ids:
                    continue
                
                headline = getattr(entry, "title", "").strip()
                if not headline:
                    continue
                
                seen_ids.add(entry_id)
                
                # Extract tickers
                tickers = extract_tickers(headline)
                
                # Create message
                doc = {
                    "id": entry_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": feed_name,
                    "headline": headline,
                    "tickers": tickers
                }
                
                # Send to Kafka
                producer.send(TOPIC, doc)
                new_count += 1
                
                logger.info(f"[{feed_name}] {headline[:100]}{'...' if len(headline) > 100 else ''} | tickers={tickers}")
                
            except Exception as e:
                logger.error(f"Error processing entry from {feed_name}: {e}")
                continue
        
        return new_count
        
    except Exception as e:
        logger.error(f"Error processing feed {feed_name}: {e}")
        return 0

def cleanup_seen_ids(seen_ids, max_size=10000):
    """Keep seen_ids set from growing too large"""
    if len(seen_ids) > max_size:
        # Remove oldest half of entries (crude but effective)
        keep_count = max_size // 2
        seen_list = list(seen_ids)
        seen_ids.clear()
        seen_ids.update(seen_list[-keep_count:])
        logger.info(f"Cleaned up seen_ids, now tracking {len(seen_ids)} entries")

def main(poll_sec: int = 60):
    """Main producer loop"""
    logger.info("Starting news producer...")
    
    # Load configuration
    feeds = load_feeds()
    if not feeds:
        logger.error("No RSS feeds configured, exiting")
        return
    
    # Create Kafka producer
    try:
        producer = create_producer()
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return
    
    # Setup signal handlers
    seen_ids = set()
    running = True
    
    def handle_signal(sig, frame):
        nonlocal running
        logger.info(f"Received signal {sig}, shutting down...")
        running = False
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    logger.info(f"Producer started. Broker={BROKER}, Topic={TOPIC}, Symbols={sorted(SYMBOLS) if SYMBOLS else 'All'}")
    
    iteration = 0
    while running:
        try:
            iteration += 1
            logger.info(f"--- Poll iteration {iteration} ---")
            
            total_new = 0
            for feed in feeds:
                if not running:
                    break
                new_count = process_feed(feed, seen_ids, producer)
                total_new += new_count
                time.sleep(1)  # Small delay between feeds
            
            logger.info(f"Poll complete. New headlines: {total_new}")
            
            # Cleanup periodically
            if iteration % 10 == 0:
                cleanup_seen_ids(seen_ids)
            
            # Wait for next poll
            for _ in range(poll_sec):
                if not running:
                    break
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(10)  # Wait before retrying
    
    # Cleanup
    logger.info("Closing producer...")
    producer.flush()
    producer.close()
    logger.info("Producer stopped")

if __name__ == "__main__":
    main()