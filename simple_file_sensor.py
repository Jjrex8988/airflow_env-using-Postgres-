# nano ~/airflow/dags/simple_file_sensor.py

# mkdir -p ~/airflow/dags/tmp


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'jjrex8988',
}

with DAG(
        dag_id='simple_file_sensor',
        description='Running a simple file sensor',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'sensor', 'file sensor'],
) as dag:
    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = '/home/jjrex8988/airflow/dags/tmp/laptops.csv',
        poke_interval = 10, # How often the sensor will check to see whether the file is available? in seconds
        timeout = 60 * 10 # Just time out and stop looking for that file (10 minutes)
    )

    checking_for_file




