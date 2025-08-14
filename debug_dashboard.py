import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime

st.set_page_config(page_title="Debug Dashboard", layout="wide")
st.title("🔧 Debug Dashboard")

# Test 1: Check if sample CSV exists and loads
st.header("Test 1: Sample CSV")
csv_path = "sample_data/impact_scores_sample.csv"

if os.path.exists(csv_path):
    st.success(f"✅ Sample CSV found: {csv_path}")
    try:
        df = pd.read_csv(csv_path)
        st.write(f"📊 Loaded {len(df)} rows")
        st.dataframe(df.head())
    except Exception as e:
        st.error(f"❌ Error loading CSV: {e}")
else:
    st.error(f"❌ Sample CSV not found: {csv_path}")
    st.info("Run: python create_sample_data.py")

# Test 2: Check Kafka connectivity
st.header("Test 2: Kafka Connection")

broker = st.text_input("Kafka Broker", "localhost:29092")
topic = st.text_input("Topic", "impact_scores")

if st.button("Test Kafka Connection"):
    try:
        from kafka import KafkaConsumer
        
        st.info("Attempting to connect to Kafka...")
        
        consumer = KafkaConsumer(
            bootstrap_servers=broker,
            consumer_timeout_ms=5000,
            auto_offset_reset='latest'
        )
        
        # Try to get topic metadata
        topics = consumer.topics()
        st.success(f"✅ Connected to Kafka! Available topics: {sorted(topics)}")
        
        if topic in topics:
            st.success(f"✅ Topic '{topic}' exists")
            
            # Try to read a few messages
            consumer.subscribe([topic])
            st.info("Attempting to read messages...")
            
            messages = []
            for message in consumer:
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    messages.append(data)
                    if len(messages) >= 5:  # Limit to 5 messages
                        break
                except Exception as e:
                    st.warning(f"Could not parse message: {e}")
            
            if messages:
                st.success(f"✅ Read {len(messages)} messages from Kafka")
                df_kafka = pd.DataFrame(messages)
                st.dataframe(df_kafka)
            else:
                st.warning("⚠️ No messages found in topic (topic might be empty)")
        else:
            st.error(f"❌ Topic '{topic}' not found")
        
        consumer.close()
        
    except ImportError:
        st.error("❌ kafka-python not installed")
    except Exception as e:
        st.error(f"❌ Kafka connection failed: {e}")

# Test 3: Environment check
st.header("Test 3: Environment")
env_vars = {
    "KAFKA_BROKER": os.getenv("KAFKA_BROKER", "Not set"),
    "SYMBOLS": os.getenv("SYMBOLS", "Not set"),
    "KAFKA_TOPIC_IMPACT": os.getenv("KAFKA_TOPIC_IMPACT", "Not set")
}

for key, value in env_vars.items():
    if value == "Not set":
        st.warning(f"⚠️ {key}: {value}")
    else:
        st.info(f"✅ {key}: {value}")

# Test 4: Manual data entry
st.header("Test 4: Manual Data Test")
if st.button("Create Test DataFrame"):
    test_data = [
        {"symbol": "AAPL", "impact_score": 1.5, "headline": "Apple rises on earnings", "event_time": "2025-08-14T15:00:00Z"},
        {"symbol": "MSFT", "impact_score": 0.8, "headline": "Microsoft steady growth", "event_time": "2025-08-14T15:01:00Z"},
        {"symbol": "GOOGL", "impact_score": -0.5, "headline": "Google faces headwinds", "event_time": "2025-08-14T15:02:00Z"}
    ]
    
    test_df = pd.DataFrame(test_data)
    st.success("✅ Created test DataFrame")
    st.dataframe(test_df)
    
    # Try plotting
    try:
        import plotly.express as px
        fig = px.bar(test_df, x="symbol", y="impact_score", title="Test Chart")
        st.plotly_chart(fig)
        st.success("✅ Plotting works")
    except Exception as e:
        st.error(f"❌ Plotting failed: {e}")

st.header("📋 Next Steps")
st.write("""
1. **If Sample CSV test fails**: Run `python create_sample_data.py`
2. **If Kafka test fails**: Check if your producers are running and using the right port
3. **If everything works here**: The main dashboard should work too
4. **If manual data test fails**: There's a fundamental issue with Streamlit/Pandas
""")