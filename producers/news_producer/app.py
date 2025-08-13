
import os, time, json, re, uuid, signal
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
import yaml
import feedparser

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_NEWS", "news_headlines")
SYMBOLS = set([s.strip().upper() for s in os.getenv("SYMBOLS","AAPL,MSFT,GOOGL").split(",") if s.strip()])

# Compile a simple cashtag/uppercase ticker regex
TICKER_RX = re.compile(r'(?:^|[^A-Z$])\$?([A-Z]{1,5})(?=$|[^A-Z])')

def extract_tickers(text: str):
    cands = set(m.group(1) for m in TICKER_RX.finditer(text or ""))
    # Filter to known universe if provided
    return sorted(list(cands & SYMBOLS)) if SYMBOLS else sorted(list(cands))

def load_feeds():
    cfg_path = os.path.join(os.path.dirname(__file__), "rss_feeds.yml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("feeds", [])

def main(poll_sec: int = 30):
    producer = KafkaProducer(bootstrap_servers=BROKER,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    feeds = load_feeds()

    seen_ids = set()
    running = True

    def handle_sig(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    print(f"[news_producer] Starting. Broker={BROKER}, Topic={TOPIC}")
    while running:
        for feed in feeds:
            try:
                parsed = feedparser.parse(feed["url"])
                for entry in parsed.entries[:50]:
                    # Build a stable-ish id
                    eid = getattr(entry, "id", None) or getattr(entry, "link", None) or str(uuid.uuid4())
                    if eid in seen_ids:
                        continue
                    seen_ids.add(eid)

                    headline = getattr(entry, "title", "") or ""
                    published = getattr(entry, "published", None)
                    ts = datetime.now(timezone.utc).isoformat()
                    doc = {
                        "id": eid,
                        "timestamp": ts,
                        "source": feed["name"],
                        "headline": headline,
                        "tickers": extract_tickers(headline)
                    }
                    producer.send(TOPIC, doc)
                    print(f"[news_producer] sent: {doc['source']} | {headline[:96]}... tickers={doc['tickers']}")
            except Exception as e:
                print(f"[news_producer] ERROR feed {feed}: {e}")
        time.sleep(poll_sec)

if __name__ == "__main__":
    main()
