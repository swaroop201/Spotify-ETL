from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from spotify_etl import spotify_etl  # Update with your actual script name

default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': datetime(2023, 1, 1),
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}

dag = DAG(
   'spotify_etl_dag',
   default_args=default_args,
   description='Spotify ETL process',
   schedule_interval=timedelta(days=1),  # Adjust as needed
)

def run_spotify_etl(**kwargs):
    
    artist_name_input = kwargs['artist_name']
    spotify_etl(artist_name_input)

run_etl_task = PythonOperator(
   task_id='run_spotify_etl',
   python_callable=run_spotify_etl,
   op_kwargs={'artist_name': 'Arman Malik'},  # Replace with desired default or dynamic input
   dag=dag,
)

run_etl_task