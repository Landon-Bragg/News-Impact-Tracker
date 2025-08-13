
import os, time, json, signal, random
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_PRICES", "price_ticks")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS","AAPL,MSFT,GOOGL").split(",") if s.strip()]
DATA_SOURCE = os.getenv("PRICE_DATA_SOURCE", "simulate").lower()
SIM_SPEED = float(os.getenv("PRICE_SIM_SPEED", "1.0"))

def producer():
    return KafkaProducer(bootstrap_servers=BROKER,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def stream_simulated(pr):
    # Start each symbol at a random base and do a small random walk
    state = {sym: 100.0 + random.random()*50 for sym in SYMBOLS}
    print(f"[price_producer] Simulating for symbols={SYMBOLS}")
    running = True
    def stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    while running:
        for sym in SYMBOLS:
            # random walk
            delta = random.gauss(0, 0.05)
            state[sym] = max(0.01, state[sym] * (1.0 + delta/100.0))
            msg = {"symbol": sym, "timestamp": now_iso(), "price": round(state[sym], 4)}
            pr.send(TOPIC, msg)
            print(f"[price_producer] {sym} -> {msg['price']}")
        time.sleep(max(0.2, 1.0 / SIM_SPEED))

def stream_yfinance(pr):
    import yfinance as yf
    print(f"[price_producer] yfinance polling for symbols={SYMBOLS}")
    running = True
    def stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    tickers = {sym: yf.Ticker(sym) for sym in SYMBOLS}
    while running:
        for sym, tk in tickers.items():
            try:
                px = tk.fast_info.get("last_price", None)
                if px is None:
                    # fallback to history 1m
                    hist = tk.history(period="1d", interval="1m")
                    if not hist.empty:
                        px = float(hist["Close"].iloc[-1])
                if px is None:  # still no price
                    continue
                msg = {"symbol": sym, "timestamp": now_iso(), "price": float(round(px, 4))}
                pr.send(TOPIC, msg)
                print(f"[price_producer] {sym} -> {msg['price']}")
            except Exception as e:
                print(f"[price_producer] ERROR {sym}: {e}")
        time.sleep(5)

def main():
    pr = producer()
    if DATA_SOURCE == "yfinance":
        stream_yfinance(pr)
    else:
        stream_simulated(pr)

if __name__ == "__main__":
    main()
