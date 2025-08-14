import os
import pandas as pd
from datetime import datetime, timezone, timedelta

def create_sample_data():
    """Create sample CSV files for the dashboard"""
    
    # Create sample_data directory if it doesn't exist
    os.makedirs("sample_data", exist_ok=True)
    
    # Create sample impact scores
    base_time = datetime.now(timezone.utc)
    
    sample_data = []
    
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]
    headlines = [
        "Apple announces breakthrough in AI chip technology",
        "Microsoft reports strong cloud revenue growth", 
        "Google unveils new quantum computing advances",
        "Amazon expands drone delivery nationwide",
        "Tesla achieves record quarterly deliveries",
        "NVIDIA partners with major automakers on AI",
        "Apple stock surges on iPhone sales beat",
        "Microsoft Teams adds innovative collaboration features",
        "Google Search integrates advanced AI capabilities", 
        "Amazon Web Services launches new data centers",
        "Tesla opens new Gigafactory in Texas",
        "NVIDIA announces next-generation graphics cards"
    ]
    
    sentiments = [0.8, 0.6, 0.7, 0.5, 0.9, 0.4, 0.75, 0.55, 0.65, 0.45, 0.85, 0.35]
    pct_changes = [2.3, 1.8, 1.5, -0.8, 3.2, -1.2, 2.8, 1.2, 1.9, -0.5, 3.5, -1.8]
    
    for i, (symbol, headline, sentiment, pct_change) in enumerate(zip(symbols * 2, headlines, sentiments, pct_changes)):
        event_time = base_time - timedelta(minutes=i*5)
        impact_score = sentiment * pct_change
        
        sample_data.append({
            "symbol": symbol,
            "window_sec": 600,
            "sentiment": round(sentiment, 2),
            "pct_change": round(pct_change, 2),
            "impact_score": round(impact_score, 2),
            "headline_id": f"sample_{i+1}",
            "headline": headline,
            "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        })
    
    # Create DataFrame and save CSV
    df = pd.DataFrame(sample_data)
    csv_path = "sample_data/impact_scores_sample.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"✅ Created {csv_path} with {len(sample_data)} sample records")
    print(f"📊 Sample data preview:")
    print(df.head(3).to_string(index=False))
    
    return csv_path

if __name__ == "__main__":
    create_sample_data()