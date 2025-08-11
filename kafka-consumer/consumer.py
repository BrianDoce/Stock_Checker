from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import psycopg2
from datetime import datetime
import json 

def save_to_postgres(data):
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='stocks_db',
            user='postgres',
            password='root'
        )
        cur = conn.cursor()

        insert_query = """
        INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
        """

        cur.execute(insert_query, (
            data["symbol"],
            data["timestamp"],
            data["open"],
            data["high"],
            data["low"],
            data["close"],
            data["volume"]
        ))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print("Database insert error:", e)


consumer = KafkaConsumer(
        'stock_data_raw',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda v: loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='stock_group'
)
print("[Consumer] Listening for stock data...")

for message in consumer:
    try:
        raw_data = message.value
        
        # Extract symbol & time series
        symbol = raw_data.get("meta", {}).get("symbol", "UNKNOWN")
        time_series = raw_data.get("time_series", {})

        for ts, values in time_series.items():
            cleaned = {
                "symbol": symbol,
                "timestamp": datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"),
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            }
            save_to_postgres(cleaned)

    except Exception as e:
        print("Error processing message:", e)