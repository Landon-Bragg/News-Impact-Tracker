

import os
import json
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except Exception:
    KAFKA_AVAILABLE = False

load_dotenv()

st.set_page_config(page_title="News Impact Tracker", layout="wide")
st.title("📈 Breaking News Impact Tracker (Live)")

# Sidebar configuration
mode = st.sidebar.selectbox("Data source", ["Kafka (live)", "Sample CSV (offline)"])
broker = st.sidebar.text_input("Kafka broker", os.getenv("KAFKA_BROKER", "localhost:29092"))
topic = st.sidebar.text_input("Impact topic", os.getenv("KAFKA_TOPIC_IMPACT", "impact_scores"))
max_batch = st.sidebar.number_input("Max messages per fetch", min_value=10, max_value=5000, value=500, step=10)
symbols_filter = st.sidebar.text_input("Filter symbols (comma-separated)", os.getenv("SYMBOLS","AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA"))
time_window_min = st.sidebar.slider("Analysis window (minutes)", min_value=5, max_value=120, value=60, step=5)
fetch_btn = st.sidebar.button("Fetch latest")

from_beginning = st.sidebar.checkbox("Read from beginning (earliest)", value=True)


if "df" not in st.session_state:
    st.session_state["df"] = pd.DataFrame(columns=[
        "symbol","window_sec","sentiment","pct_change","impact_score","headline_id","headline","event_time"
    ])

def parse_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if not df.empty:
        # ensure types
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
        numeric_cols = ["window_sec","sentiment","pct_change","impact_score"]
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["symbol","event_time","impact_score"])
    return df

def fetch_from_kafka(broker: str, topic: str, max_msgs: int = 1000, from_beginning: bool = True) -> pd.DataFrame:
    if not KAFKA_AVAILABLE:
        st.warning("kafka-python is not installed in this environment. Install it and rerun.")
        return pd.DataFrame()
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            auto_offset_reset="earliest" if from_beginning else "latest",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,  # give it a moment
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="news-impact-dashboard",  # stable group so offset behavior is consistent
        )
    except Exception as e:
        st.error(f"Could not connect to Kafka at {broker}: {e}")
        return pd.DataFrame()

    out, count = [], 0
    for msg in consumer:
        out.append(msg.value)
        count += 1
        if count >= max_msgs:
            break
    consumer.close()
    return parse_rows(out)


def load_sample_csv() -> pd.DataFrame:
    here = os.path.dirname(__file__)
    sample = os.path.join(here, "..", "sample_data", "impact_scores_sample.csv")
    if not os.path.exists(sample):
        st.error("Sample CSV not found.")
        return pd.DataFrame()
    return parse_rows(pd.read_csv(sample).to_dict(orient="records"))

# Fetch data on demand
if fetch_btn:
    if mode.startswith("Kafka"):
        new_df = fetch_from_kafka(broker, topic, int(max_batch), from_beginning)

    else:
        new_df = load_sample_csv()
    if not new_df.empty:
        st.session_state["df"] = pd.concat([st.session_state["df"], new_df], ignore_index=True).drop_duplicates(subset=["headline_id","event_time","symbol"], keep="last")

df = st.session_state["df"].copy()
if df.empty:
    st.info("No data loaded yet. Click **Fetch latest** in the left sidebar to load from Kafka or sample CSV.")
    st.stop()

# Filter by symbols & time window
if symbols_filter.strip():
    keep = set(s.strip().upper() for s in symbols_filter.split(",") if s.strip())
    df = df[df["symbol"].isin(keep)]

now_utc = pd.Timestamp.now(tz=timezone.utc)

cutoff = now_utc - pd.Timedelta(minutes=int(time_window_min))
dfw = df[df["event_time"] >= cutoff]

# KPI Cards
col1, col2, col3, col4 = st.columns(4)
col1.metric("Rows (window)", f"{len(dfw):,}")
if not dfw.empty:
    pos = (dfw["impact_score"] > 0).mean()
    col2.metric("% Positive Impact", f"{(pos*100):.1f}%")
    top_symbol = dfw.groupby("symbol")["impact_score"].mean().sort_values(ascending=False).head(1)
    if not top_symbol.empty:
        col3.metric("Top Symbol (avg impact)", f"{top_symbol.index[0]} ({top_symbol.iloc[0]:.2f})")
    best_headline = dfw.sort_values("impact_score", ascending=False).head(1)
    if not best_headline.empty:
        col4.metric("Max Impact (row)", f"{best_headline['impact_score'].iloc[0]:.2f}")

# Charts
left, right = st.columns((2,1))

with left:
    # Time series for selected symbol
    symbols = sorted(dfw["symbol"].dropna().unique().tolist())
    sel = st.selectbox("Symbol", symbols, index=0 if symbols else None)
    if sel:
        dfx = dfw[dfw["symbol"] == sel].sort_values("event_time")
        if not dfx.empty:
            fig = px.line(dfx, x="event_time", y="impact_score", title=f"Impact Score Over Time: {sel}")
            st.plotly_chart(fig, use_container_width=True)

with right:
    # Bar of avg impact by symbol
    bar = (dfw.groupby("symbol")["impact_score"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
        .head(15))
    if not bar.empty:
        fig2 = px.bar(bar, x="symbol", y="impact_score", title="Avg Impact by Symbol (window)")
        st.plotly_chart(fig2, use_container_width=True)

# Recent headlines table
st.subheader("Recent Headlines (window)")
show_cols = ["event_time","symbol","impact_score","pct_change","sentiment","headline"]
tbl = dfw.sort_values("event_time", ascending=False)[show_cols]
st.dataframe(tbl, use_container_width=True, hide_index=True)


