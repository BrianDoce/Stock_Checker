from kafka import KafkaConsumer
from datetime import datetime
import time
from json import loads
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import statistics
import os 

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock_trades")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "root")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "stocks_db")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", 5432)
bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

POSTGRES_URI = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

for i in range(10):
    try:
        engine = create_engine(POSTGRES_URI)
        with engine.connect() as conn:
            print("✅ Connected to Postgres")
        break
    except OperationalError:
        print("⏳ Waiting for Postgres...")
        time.sleep(5)
else:
    raise Exception("❌ Could not connect to Postgres after retries")

with engine.begin() as conn:
    conn.execute(
        text("""
        CREATE TABLE IF NOT EXISTS stock_trades (
            symbol TEXT,
            price DOUBLE PRECISION,
            timestamp TIMESTAMP,
            volume INTEGER,
            price_change_pct DOUBLE PRECISION,
            rolling_avg_price DOUBLE PRECISION,
            volume_category TEXT,
            market_session TEXT
        );
        """)
    )

# Store previous prices for feature engineering
last_seen_prices = {}
price_history = {} # For rolling average

consumer = None
for i in range(10):
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='stock_group'
        )
        print("✅ Connected to Kafka")
        break
    except Exception:
        print("⏳ Waiting for Kafka...")
        time.sleep(5)
else:
    raise Exception("❌ Could not connect to Kafka after retries")
print("[Consumer] Listening for stock data...")

for message in consumer:
    try:
        raw_data = message.value
        
        if raw_data.get("type") != "trade":
            continue
        # Extract symbol & time series
        trades = raw_data.get("data", [])
        if not trades:
            continue

        trade = trades[0]
        symbol = trade.get("s", "UNKOWN")
        price = trade.get("p", None)
        timestamp = trade.get("t", None)
        volume = trade.get("v", None)

        if not all([symbol, price, timestamp, volume]):
            print(f"[Warning] Skipping incomplete record: {trade}")
            continue

        try:
            price = float(price)
            volume = int(volume)
        except ValueError:
            print(f"[Warning] Invalid data types: {trade}")
            continue
        if price <= 0 or volume <= 0:
            continue
        timestamp_dt = datetime.fromtimestamp(timestamp / 1000)

        # 1. Price change %
        prev_price = last_seen_prices.get(symbol)
        if prev_price:
            price_change_pct = ((price - prev_price) / prev_price) * 100
        else:
            price_change_pct = 0.0
        last_seen_prices[symbol] = price

        # 2. Rolling average (last 5 trades)
        history = price_history.get(symbol, [])
        history.append(price)
        if len(history) > 5:
            history.pop(0)
        price_history[symbol] = history
        rolling_avg = statistics.mean(history)

        # 3. Volume category
        if volume < 100:
            volume_category = "small"
        elif volume < 1000:
            volume_category = "medium"
        else:
            volume_category = "large"

        # 4. Market session flag
        def get_market_session(ts):
            if ts.time() < datetime.time(9, 30):
                return "pre-market"
            elif ts.time() <= datetime.time(16, 0):
                return "regular"
            else:
                return "after-hours"
        market_session = get_market_session(timestamp_dt)
        with engine.begin() as conn:
            conn.execute(
                text("""
                INSERT INTO stock_trades (
                    symbol, price, timestamp, volume,
                    price_change_pct, rolling_avg_price,
                    volume_category, market_session
                )
                VALUES (:symbol, :price, :timestamp, :volume,
                        :price_change_pct, :rolling_avg_price,
                        :volume_category, :market_session)
                """),
                {
                    "symbol": symbol,
                    "price": price,
                    "timestamp": timestamp_dt,
                    "volume": volume,
                    "price_change_pct": price_change_pct,
                    "rolling_avg_price": rolling_avg,
                    "volume_category": volume_category,
                    "market_session": market_session
                }
            )

        print(f"[Stored] {symbol} {price} ({market_session})")

    except Exception as e:
        print("Error processing message:", e)