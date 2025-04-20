from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
# from reddit_etl import run_reddit_etl

from extract_reddit import extract_reddit_posts
from save_bronze import save_bronze
from transform_silver import transform_silver
from transform_gold import transform_gold
from load_to_postgres import load_to_postgres


default_args = {
    'owner': 'airflow',  # Change to your name/team
    'depends_on_past': False,   # Don't rerun if previous run failed
    'start_date': datetime(2025, 4, 1),  # Start date (YYYY, MM, DD)
    'email': ['admin@example.com'],  # Your alert email
    'email_on_failure': False,    # Enable alerts on failure
    'email_on_retry': False,      # Enable alerts on retry
    'retries': 1,                # Increase retries for API calls
    'retry_delay': timedelta(minutes=2)  # Longer delay for rate limits
}

with DAG(
    'reddit_dag',
    default_args=default_args,
    schedule_interval=None,  # Add this if you don't want automatic scheduling
    catchup=False,          # Add this to prevent backfilling
    description='Reddit ETL Pipeline'
) as dag:
    
    t1 = PythonOperator(
        task_id='extract_reddit',
        python_callable=extract_reddit_posts
    )
    
    t2 = PythonOperator(
        task_id='save_bronze_layer',
        python_callable=save_bronze
    )
    
    t3 = PythonOperator(
        task_id='transform_silver_layer',
        python_callable=transform_silver
    )
    
    t4 = PythonOperator(
        task_id='transform_gold_layer',
        python_callable=transform_gold
    )
    
    t5 = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )
    
    t1 >> t2 >> t3 >> t4 >> t5