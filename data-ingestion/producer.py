from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import websocket

API_KEY = "d2dlm19r01qjrul3rdfgd2dlm19r01qjrul3rdg0"
STOCK_SYMBOLS = [
    "SPY",  # S&P 500 ETF
    "AAPL", "MSFT", "AMZN", "GOOGL", "NVDA",
    "META", "TSLA", "BRK.B", "JPM", "UNH"
]
KAFKA_TOPIC = "stock_prices"

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    try:
        data = json.loads(message)
        producer.send(KAFKA_TOPIC, data)
    except Exception as e:
        print(f"Error in on_message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    try:
        for symbol in STOCK_SYMBOLS:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
    except Exception as e:
        print(f"Error subscribing: {e}")

try:
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()
except KeyboardInterrupt:
    print("Producer stopped manually")
except Exception as e:
    print(f"Producer error: {e}")
