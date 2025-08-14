import os
import json
import time
from datetime import datetime, timezone
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import signal
import sys

# Sentiment analysis
try:
    import nltk
    nltk.data.find('sentiment/vader_lexicon')
except:
    print("Downloading VADER lexicon...")
    import nltk
    nltk.download('vader_lexicon')

from nltk.sentiment import SentimentIntensityAnalyzer

load_dotenv()

# Configuration
BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_NEWS = os.getenv("KAFKA_TOPIC_NEWS", "news_headlines")
TOPIC_PRICES = os.getenv("KAFKA_TOPIC_PRICES", "price_ticks")
TOPIC_OUTPUT = os.getenv("KAFKA_TOPIC_IMPACT", "impact_scores")

class SimpleImpactCalculator:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
        self.price_history = defaultdict(lambda: deque(maxlen=100))  # Keep last 100 prices per symbol
        self.processed_news = set()  # Track processed news IDs
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        
    def stop(self, signum, frame):
        print("\nStopping...")
        self.running = False
        
    def get_sentiment(self, text):
        """Calculate sentiment score for text"""
        if not text:
            return 0.0
        return self.analyzer.polarity_scores(text)['compound']
    
    def update_prices(self, consumer):
        """Read latest price updates"""
        messages = consumer.poll(timeout_ms=100)
        for topic_partition, records in messages.items():
            for record in records:
                try:
                    data = json.loads(record.value.decode('utf-8'))
                    symbol = data.get('symbol')
                    price = data.get('price')
                    timestamp = data.get('timestamp')
                    
                    if symbol and price and timestamp:
                        self.price_history[symbol].append({
                            'price': float(price),
                            'timestamp': timestamp
                        })
                        
                except Exception as e:
                    print(f"Error processing price: {e}")
                    
    def calculate_price_change(self, symbol, window_minutes=10):
        """Calculate price change over time window"""
        if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
            return 0.0
            
        prices = list(self.price_history[symbol])
        
        # Get current and earlier price
        current_price = prices[-1]['price']
        
        # Find price from ~window_minutes ago (approximate)
        target_idx = max(0, len(prices) - window_minutes)
        earlier_price = prices[target_idx]['price']
        
        if earlier_price <= 0:
            return 0.0
            
        return ((current_price - earlier_price) / earlier_price) * 100.0
    
    def process_news(self, consumer, producer):
        """Process news headlines and calculate impact scores"""
        messages = consumer.poll(timeout_ms=100)
        
        for topic_partition, records in messages.items():
            for record in records:
                try:
                    data = json.loads(record.value.decode('utf-8'))
                    
                    news_id = data.get('id')
                    headline = data.get('headline', '')
                    tickers = data.get('tickers', [])
                    timestamp = data.get('timestamp')
                    
                    if news_id in self.processed_news:
                        continue
                        
                    self.processed_news.add(news_id)
                    
                    # Calculate sentiment
                    sentiment = self.get_sentiment(headline)
                    
                    print(f"📰 {headline[:80]}{'...' if len(headline) > 80 else ''}")
                    print(f"   Sentiment: {sentiment:.3f} | Tickers: {tickers}")
                    
                    # Calculate impact for each ticker
                    for symbol in tickers:
                        if symbol:
                            pct_change = self.calculate_price_change(symbol)
                            impact_score = sentiment * pct_change
                            
                            # Create impact record
                            impact_record = {
                                "symbol": symbol,
                                "window_sec": 600,
                                "sentiment": round(sentiment, 4),
                                "pct_change": round(pct_change, 4),
                                "impact_score": round(impact_score, 4),
                                "headline_id": news_id,
                                "headline": headline,
                                "event_time": datetime.now(timezone.utc).isoformat()
                            }
                            
                            # Send to output topic
                            producer.send(TOPIC_OUTPUT, json.dumps(impact_record))
                            
                            print(f"   📊 {symbol}: Price change {pct_change:+.2f}% → Impact {impact_score:+.4f}")
                    
                    print()
                    
                except Exception as e:
                    print(f"Error processing news: {e}")
                    
        # Clean up old processed news IDs periodically
        if len(self.processed_news) > 1000:
            # Keep only recent 500
            recent_ids = list(self.processed_news)[-500:]
            self.processed_news.clear()
            self.processed_news.update(recent_ids)
    
    def run(self):
        """Main processing loop"""
        print("🚀 Starting Simple Impact Calculator")
        print(f"📡 Kafka broker: {BROKER}")
        print(f"📰 News topic: {TOPIC_NEWS}")
        print(f"💰 Price topic: {TOPIC_PRICES}")
        print(f"📊 Output topic: {TOPIC_OUTPUT}")
        print()
        
        # Create consumers
        news_consumer = KafkaConsumer(
            TOPIC_NEWS,
            bootstrap_servers=BROKER,
            auto_offset_reset='latest',
            value_deserializer=lambda m: m,
            consumer_timeout_ms=1000
        )
        
        price_consumer = KafkaConsumer(
            TOPIC_PRICES,
            bootstrap_servers=BROKER,
            auto_offset_reset='latest',
            value_deserializer=lambda m: m,
            consumer_timeout_ms=1000
        )
        
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else v
        )
        
        print("✅ Kafka connections established")
        print("🔄 Processing... (Ctrl+C to stop)")
        print()
        
        try:
            while self.running:
                # Update price history
                self.update_prices(price_consumer)
                
                # Process news and calculate impact
                self.process_news(news_consumer, producer)
                
                # Small delay
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            print("\n🛑 Received interrupt signal")
            
        finally:
            print("🧹 Cleaning up...")
            news_consumer.close()
            price_consumer.close()
            producer.flush()
            producer.close()
            print("✅ Stopped successfully")

if __name__ == "__main__":
    calculator = SimpleImpactCalculator()
    calculator.run()