from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import websocket
import time
import os
from  itertools import cycle

# Symbols to subscribe
STOCK_SYMBOLS = [
    "SPY", "AAPL", "MSFT", "AMZN", "GOOGL",
    "NVDA", "META", "TSLA", "BRK.B", "JPM", "UNH"
]
BATCH_SIZE = 2  # number of symbols per rotation
ROTATION_INTERVAL = 30  # seconds to wait before rotating symbols

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_trades")
API_KEY = os.getenv("FINNHUB_API_KEY")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "stock_checker-kafka-1:9092")
def connect_kafka(retries=5, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("✅ Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Waiting for Kafka... retry {i+1}/{retries}")
            time.sleep(delay)
    raise Exception("❌ Could not connect to Kafka after retries")


def run_producer():
    """Start WebSocket producer and stream trades to Kafka."""
    print("STARTING PRODUCER")
    producer = connect_kafka()
    
    symbol_cycle = cycle([STOCK_SYMBOLS[i:i+BATCH_SIZE] for i in range(0, len(STOCK_SYMBOLS), BATCH_SIZE)])

    # WebSocket callbacks
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if data.get("type") == "trade":
                for trade in data["data"]:
                    wrapped = {"type": "trade", "data": [trade]}
                    producer.send(KAFKA_TOPIC, wrapped)
                    print(f"[Producer] Sent trade: {wrapped}")
        except Exception as e:
            print(f"[Producer Error] on_message: {e}")

    def on_error(ws, error):
        print(f"[Producer] WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("[Producer] WebSocket closed")
        time.sleep(30)
        start_websocket(next(symbol_cycle))

    def on_open(ws, symbols):
        print("[Producer] WebSocket opened")

        try:
            for symbol in symbols:
                ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            print(f"[Producer] Subscribed to symbols: {symbols}")
        except Exception as e:
            print(f"[Producer Error] subscribing: {e}")
   
    def start_websocket(symbols):
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={API_KEY}",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=lambda ws: on_open(ws, symbols),
        )
        ws.run_forever()

    while True:
        batch = next(symbol_cycle)
        start_websocket(batch)
        print(f"[Producer] Waiting {ROTATION_INTERVAL}s before next batch")
        time.sleep(ROTATION_INTERVAL)
if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("Missing FINNHUB_API_KEY environment variable")
    run_producer()
