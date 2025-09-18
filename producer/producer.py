from kafka import KafkaProducer
import json
import websocket, time
import os

STOCK_SYMBOLS = [
    "SPY",
    "AAPL", "MSFT", "AMZN", "GOOGL", "NVDA",
    "META", "TSLA", "BRK.B", "JPM", "UNH"
]

KAFKA_TOPIC = "stock_trades"
API_KEY = os.getenv("FINNHUB_API_KEY")

def run_producer():
    """Start WebSocket producer and stream to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    def on_message(ws, message):
        try:
            data = json.loads(message)
            producer.send(KAFKA_TOPIC, data)
            print("[Producer] Sent:", data)
        except Exception as e:
            print(f"[Producer Error] on_message: {e}")

    def on_error(ws, error):
        print(f"[Producer] WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("[Producer] WebSocket closed")

    def on_open(ws):
        try:
            for symbol in STOCK_SYMBOLS:
                ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        except Exception as e:
            print(f"[Producer Error] subscribing: {e}")

    try:
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={API_KEY}",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        
        ws.run_forever()
        print("[Producer] Finished streaming")
    except KeyboardInterrupt:
        print("[Producer] Stopped manually")
    except Exception as e:
        print(f"[Producer Fatal Error]: {e}")
if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("Missing FINNHUB_API_KEY environment variable")
    run_producer()
