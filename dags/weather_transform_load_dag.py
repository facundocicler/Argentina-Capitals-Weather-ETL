from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.operators.wait_mysql_operator import WaitForMySQLOperator
from plugins.operators.mysql_dll_operator import MySqlDDLOperator
from docker.types import Mount
from dotenv import dotenv_values

default_args = {
    'owner': 'facundo',
    'retries': 1
}

env_vars = dotenv_values("/opt/airflow/.env")

with DAG(
    dag_id='weather_transform_load',
    description='Transforma datos desde MongoDB y los carga en MySQL manualmente',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    transform_task = DockerOperator(
        task_id='transform_with_spark',
        image='weather-app',
        command='python spark_jobs/transform_weather_job.py',
        network_mode='app-net',
        docker_url='unix://var/run/docker.sock',
        api_version='auto',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source='/opt/airflow/data', target='/tmp', type='bind')
        ],
        environment=env_vars,
        working_dir='/app',
        retries=1,
        retry_delay=timedelta(minutes=2),
        tty=True
    )

    wait_for_mysql_task = WaitForMySQLOperator(
        task_id="wait_for_mysql_ready",
    )

    create_table_task = MySqlDDLOperator(
        task_id="create_weather_table",
        sql="/opt/airflow/sql/create_weather_table.sql",
    )

    load_task = DockerOperator(
        task_id='load_data_to_mysql',
        image='weather-app',
        command='python spark_jobs/load_to_mysql_job.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='app-net',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source='/opt/airflow/data', target='/tmp', type='bind')
        ],
        environment=env_vars,  
        working_dir='/app'
    )

    transform_task >> wait_for_mysql_task >> create_table_task >> load_task