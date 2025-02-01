from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/dags/scripts')
from stock_fetcher import main as fetch_stocks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # Add these parameters for continuous running
    'max_active_runs': 1,
    'execution_timeout': timedelta(minutes=4),  # Ensure task completes before next run
}

dag = DAG(
    'stock_data_fetcher',
    default_args=default_args,
    description='Fetch stock data every 5 minutes continuously',
    # Use cron expression for every 5 minutes
    schedule_interval='*/5 * * * *',
    # Start date in the past to ensure it starts immediately
    start_date=datetime(2025, 2, 1),
    catchup=False,
    # Add these parameters for continuous running
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=4)
)

def run_stock_fetcher():
    import asyncio
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(fetch_stocks())

fetch_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=run_stock_fetcher,
    dag=dag,
    # Add execution timeout to ensure task completes before next run
    execution_timeout=timedelta(minutes=4)
)