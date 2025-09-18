from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import your producer/consumer functions
from data_ingestion.producer import run_producer
from data_ingestion.consumer import run_consumer

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "stock_pipeline",
    default_args=default_args,
    description="Daily stock pipeline with Kafka and Postgres",
    schedule_interval="@daily",   # change if needed
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stocks", "kafka"],
) as dag:

    producer_task = PythonOperator(
        task_id="run_producer",
        python_callable=run_producer,
    )

    consumer_task = PythonOperator(
        task_id="run_consumer",
        python_callable=run_consumer,
    )

    producer_task >> consumer_task