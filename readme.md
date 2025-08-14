![alt text](image-1.png)

Got it — here’s your README as **one single markdown block** so you can copy it in one go:

```markdown
# 📈 News Impact Tracker

A real-time pipeline that ingests breaking financial news, calculates sentiment-driven price impacts for selected stocks, and visualizes the results in an interactive dashboard.

## 🚀 Features
- Real-time data ingestion from multiple financial news RSS feeds
- Live price streaming for selected tickers
- Sentiment analysis (VADER) on news headlines
- Impact score calculation = sentiment × short-term price change
- Kafka-powered message streaming between components
- Interactive Streamlit dashboard for live monitoring
- Configurable tickers, analysis windows, and news sources

## 📂 Project Structure
```

news-impact-tracker/
├── producers/
│   ├── news\_producer/         # Fetches headlines from RSS feeds
│   ├── price\_producer/        # Streams latest stock prices
├── dashboard/
│   ├── app.py                 # Main live dashboard
│   ├── debug\_dashboard.py     # Testing & connectivity dashboard
├── simple\_impact\_calculator.py # Calculates sentiment × price change
├── docker-compose.yml          # Kafka, Spark, Kafka UI services
├── requirements.txt            # Python dependencies
└── README.md                   # This file

````

## ⚙️ Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Virtual environment (recommended)

## 🛠️ Setup

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/news-impact-tracker.git
cd news-impact-tracker
````

### 2. Start Kafka & dependencies

```powershell
docker compose up -d
```

Wait until the Kafka container is healthy:

```powershell
docker inspect -f "{{.State.Health.Status}}" kafka
```

### 3. Create & activate a virtual environment

```powershell
python -m venv .venv
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 4. Configure environment variables

Copy `.env.example` → `.env` and adjust:

```env
KAFKA_BROKER=localhost:29092
SYMBOLS=AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA
KAFKA_TOPIC_NEWS=news_headlines
KAFKA_TOPIC_PRICES=price_ticks
KAFKA_TOPIC_IMPACT=impact_scores
```

## ▶️ Running the Pipeline

Open three separate terminals (all with the venv activated):

**A) Price producer**

```powershell
$env:KAFKA_BROKER="localhost:29092"
python .\producers\price_producer\app.py
```

**B) News producer**

```powershell
$env:KAFKA_BROKER="localhost:29092"
$env:SYMBOLS="AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA"
python .\producers\news_producer\app.py
```

**C) Impact calculator**

```powershell
$env:KAFKA_BROKER="localhost:29092"
$env:KAFKA_TOPIC_NEWS="news_headlines"
$env:KAFKA_TOPIC_PRICES="price_ticks"
$env:KAFKA_TOPIC_IMPACT="impact_scores"
python .\simple_impact_calculator.py
```

## 📊 Viewing the Dashboard

Run the live dashboard:

```powershell
$env:KAFKA_BROKER="localhost:29092"
$env:KAFKA_TOPIC_IMPACT="impact_scores"
streamlit run .\dashboard\app.py
```

Open your browser at:

```
http://localhost:8501
```

**Tips:**

* Check "Read from beginning" to load historical impact scores.
* Adjust Analysis window in the sidebar to filter recent events.

## 🖼️ Architecture

```
 [RSS Feeds] → news_producer ─┐
                              ├── Kafka → simple_impact_calculator → impact_scores topic → dashboard
 [Price API] → price_producer ─┘
```

## 📝 Customization

* **Add/remove tickers:** Edit `SYMBOLS` in `.env` or when starting producers.
* **Change feeds:** Modify the RSS list in `producers/news_producer/app.py`.
* **Adjust impact formula:** Edit `simple_impact_calculator.py`.

## 📌 Notes

* Requires internet access for RSS feeds and price API.
* The included `debug_dashboard.py` can be used to verify Kafka connectivity and topic contents before running the main dashboard.

## 📄 License

MIT License © 2025 Your Name

```

Do you want me to also add a **"Quick Start" diagram** screenshot section in this same block so the README looks more visual on GitHub?
```
