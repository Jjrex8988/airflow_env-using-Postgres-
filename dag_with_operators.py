# Every workflow that you execute in Airflow is defined as a DAG
# Taskflow API, PostgresSQL, Sensors, Hooks

# TaskFlow API
# The TaskFlow API (new style, since Airflow 2.0)
# The TaskFlow API lets you write DAGs in a clean, Pythonic way using the @task decorator.

# from airflow.decorators import dag, task
# from datetime import datetime
#
#
# @dag(schedule="@daily", start_date=datetime(2025, 1, 1), catchup=False) #DECORATOR-BASED SYNTAX
# def my_etl():
#     @task
#     def extract():
#         return "data"
#
#     @task
#     def transform(data):
#         return data.upper()
#
#     @task
#     def load(data):
#         print(f"Loading {data}")
#
#     raw = extract()
#     processed = transform(raw)
#     load(processed)
#
# my_etl()


# PostgreSQL
# Most popular relational databases used in production

# SENSORS
# Type of operator that waits for a certain condition or external event to occur

# HOOKs
# Provide an interface to interact with external systme such as databases, cloud platforms, API etc


# nano ~/airflow/dags/dag_with_operators.py
import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.decorators import dag, task

default_args = {
    'owner': 'jjrex8988',
}


# 1
# def task_a():
#     print("TASK A executed!")
#
#
# def task_b():
#     time.sleep(5)
#     print("TASK B executed!")
#
#
# def task_c():
#     time.sleep(5)
#     print("TASK C executed!")
#
#
# def task_d():
#     time.sleep(5)
#     print("TASK D executed!")
#
#
# def task_e():
#     print("TASK E executed!")
#
#
# with DAG(
#         dag_id='dag_with_operators',
#         description='Python operators in DAGs',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval='@once',
#         tags=['dependencies', 'python', 'operators']
# ) as dag:
#     taskA = PythonOperator(
#         task_id='taskA',
#         python_callable=task_a
#     )
#
#     taskB = PythonOperator(
#         task_id='taskB',
#         python_callable=task_b
#     )
#
#     taskC = PythonOperator(
#         task_id='taskC',
#         python_callable=task_c
#     )
#
#     taskD = PythonOperator(
#         task_id='taskD',
#         python_callable=task_d
#     )
#
#     taskE = PythonOperator(
#         task_id='taskE',
#         python_callable=task_e
#     )
#
# taskA >> [taskB, taskC, taskD] >> taskE


# 2
# This @dag decorator identifies the function dag_with_taskflow_api as containing the definition of a workflow
@dag(dag_id='dag_with_taskflow',
     description='DAG using the TaskFlow API',
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@once',
     tags=['dependencies', 'python', 'taskflow_api'])
def dag_with_taskflow_api():
    @task
    def task_a():
        print("TASK A executed!")

    @task
    def task_b():
        time.sleep(5)
        print("TASK B executed!")

    @task
    def task_c():
        time.sleep(5)
        print("TASK C executed!")

    @task
    def task_d():
        time.sleep(5)
        print("TASK D executed!")

    @task
    def task_e():
        print("TASK E executed!")

    task_a() >> [task_b(), task_c(), task_d()] >> task_e()

dag_with_taskflow_api()