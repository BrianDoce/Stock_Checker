from airflow import DAG
from airflow.operators.python import PythonOperator
from data_ingestion.producer import run_producer
from data_ingestion.consumer import run_consumer
import os
from datetime import datetime
import subprocess

default_args = {
    'owner': 'brian',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 10),
    'retries': 1
}

dag = DAG(
    'daily_kafka_ingestion',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

DATA_INGESTION_PATH = "/opt/airflow/data_ingestion"

def run_producer():
    """Run producer.py from data-ingestion folder"""
    script_path = os.path.join(DATA_INGESTION_PATH, "producer.py")
    subprocess.run(["python", script_path], check=True)

def run_consumer():
    """Run consumer.py from data-ingestion folder"""
    script_path = os.path.join(DATA_INGESTION_PATH, "consumer.py")
    subprocess.run(["python", script_path], check=True)

producer_task = PythonOperator(
    task_id='run_producer',
    python_callable=run_producer,
    dag=dag
)

consumer_task = PythonOperator(
    task_id='run_consumer',
    python_callable=run_consumer,
    dag=dag
)

# Producer runs first, then consumer
producer_task >> consumer_task