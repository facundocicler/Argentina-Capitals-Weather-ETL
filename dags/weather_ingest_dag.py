from datetime import datetime, timedelta
from airflow import DAG
from plugins.operators.extract_from_api_operator import ExtractWeatherDataOperator
from plugins.operators.load_to_mongo_operator import LoadRawDataOperator

default_args = {
    'owner': 'facundo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='weather_ingest',
    description='Extrae datos de la API y los guarda en MongoDB',
    start_date=datetime(2025, 7, 1),
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args
) as dag:
    
    extract_task = ExtractWeatherDataOperator(
        task_id='extract_weather_data'
    )

    load_mongodb_task = LoadRawDataOperator(
        task_id='load_data_to_mongodb'
    )

    extract_task >> load_mongodb_task