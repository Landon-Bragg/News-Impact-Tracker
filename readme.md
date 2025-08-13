# Breaking News Impact Tracker for Stock Tickers

**Stack:** Apache Kafka + Apache Spark (Structured Streaming) + Python (PySpark) + Streamlit + (optional) Postgres

This project ingests live news headlines and stock price ticks, runs sentiment analysis on the news,
joins with near-term price reactions, and computes a **News Impact Score** per ticker in real time.

---

## 🚀 Quickstart (Windows + Python 3.11)

1) **Install prerequisites**
- Docker Desktop (WSL2 backend recommended)
- Python 3.11 (with `pip`)

2) **Create and activate a venv, then install Python deps**
```powershell
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt