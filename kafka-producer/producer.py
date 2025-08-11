from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import requests

API_KEY = "3BPTTRNM07LMR8D1"
TICKERS = ["AAPL", "MSFT", "GOOG", "TSLA", "AMZN"]  # You can expand later
INTERVAL = "1min"

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Example: fetch and send stock prices
def fetch_stock_data(symbol):
    url = f'https://www.alphavantage.co/query'
    params = {
        'function':'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval': INTERVAL,
        'apikey': API_KEY
    }
    r = requests.get(url, params=params)
    if r.status_code == 200:
        data = r.json()
        return {
            'symbol': symbol,
            'data': data
        }
    else:
        print(f"Error fetching data for {symbol}: {r.status_code}")
        return None
while True:
    for ticker in TICKERS:
        stock_data = fetch_stock_data(ticker)
        if stock_data:
            producer.send(f'stock_data_raw', stock_data)
            print(f"Sent data for {ticker}")
        time.sleep(12)