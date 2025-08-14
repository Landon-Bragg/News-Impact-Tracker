import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

def inject_test_data():
    """Inject test impact scores directly to Kafka"""
    
    broker = "localhost:29092"
    topic = "impact_scores"
    
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"🧪 Injecting test data to {broker}/{topic}")
    
    # Test impact scores
    test_data = [
        {
            "symbol": "AAPL",
            "window_sec": 600,
            "sentiment": 0.75,
            "pct_change": 2.3,
            "impact_score": 1.725,
            "headline_id": "test_1",
            "headline": "Apple announces breakthrough in AI technology",
            "event_time": datetime.now(timezone.utc).isoformat()
        },
        {
            "symbol": "TSLA",
            "window_sec": 600,
            "sentiment": -0.4,
            "pct_change": -1.8,
            "impact_score": 0.72,
            "headline_id": "test_2",
            "headline": "Tesla faces production delays",
            "event_time": datetime.now(timezone.utc).isoformat()
        },
        {
            "symbol": "GOOGL",
            "window_sec": 600,
            "sentiment": 0.6,
            "pct_change": 1.5,
            "impact_score": 0.9,
            "headline_id": "test_3",
            "headline": "Google Cloud wins major enterprise contract",
            "event_time": datetime.now(timezone.utc).isoformat()
        },
        {
            "symbol": "MSFT",
            "window_sec": 600,
            "sentiment": 0.3,
            "pct_change": 0.8,
            "impact_score": 0.24,
            "headline_id": "test_4",
            "headline": "Microsoft reports steady quarterly growth",
            "event_time": datetime.now(timezone.utc).isoformat()
        }
    ]
    
    for i, data in enumerate(test_data):
        producer.send(topic, data)
        print(f"✅ Sent: {data['symbol']} - {data['headline'][:50]}...")
        time.sleep(1)  # Space out the messages
    
    producer.flush()
    producer.close()
    print(f"🎉 Injected {len(test_data)} test impact scores!")
    print("📊 Now try the dashboard - click 'Fetch latest'")

if __name__ == "__main__":
    inject_test_data()