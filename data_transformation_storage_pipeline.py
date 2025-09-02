# nano ~/airflow/dags/data_transformation_storage_pipeline.py

# sqlite_operator.execute(context=None)
# You are manually instantiating a SqliteOperator inside a @task function, then calling its .execute() yourself.
# Since you’re not inside Airflow’s normal operator execution loop, you don’t have a context dict to pass in.
#
# So you pass context=None just to make the method run.
#
# That means the operator runs “outside” of Airflow’s usual context (it won’t push XComs, won’t log to task instance metadata, etc). It’s basically just using the operator class like a normal Python function.


from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd

import json
import csv

# OLD
# from airflow.operators.sqlite_operator import SqliteOperator

# NEW
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner': 'jjrex8988',
}


# 1
# @dag(dag_id='data_transformation_storage_pipeline',
#      description='Data transformation and storage pipeline',
#      default_args=default_args,
#      start_date=days_ago(1),
#      schedule_interval='@once',
#      tags=['sqlite', 'python', 'taskflow_api', 'xcoms'])
# def data_transformation_storage_pipeline():
#     @task
#     def read_dataset():
#         df = pd.read_csv('/home/jjrex8988/airflow/dags/datasets/car_data.csv')
#
#         return df.to_json()
#
#     @task
#     def create_table():
#         sqlite_operator = SqliteOperator(
#             task_id="create_table",
#             sqlite_conn_id="my_sqlite_conn",
#             sql="""CREATE TABLE IF NOT EXISTS car_data (
#                         id INTEGER PRIMARY KEY,
#                         brand TEXT NOT NULL,
#                         model TEXT NOT NULL,
#                         body_style TEXT NOT NULL,
#                         seat INTEGER NOT NULL,
#                         price INTEGER NOT NULL);""",
#         )
#
#         sqlite_operator.execute(context=None)
#
#     @task
#     def insert_selected_data(**kwargs):
#         ti = kwargs['ti']
#
#         json_data = ti.xcom_pull(task_ids='read_dataset')
#
#         df = pd.read_json(json_data)
#
#         df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]
#
#         df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
#
#         insert_query = """
#             INSERT INTO car_data (brand, model, body_style, seat, price)
#             VALUES (?, ?, ?, ?, ?)
#         """
#
#         parameters = df.to_dict(orient='records')
#
#         for record in parameters:
#             sqlite_operator = SqliteOperator(
#                 task_id="insert_data",
#                 sqlite_conn_id="my_sqlite_conn",
#                 sql=insert_query,
#                 parameters=tuple(record.values()),
#             )
#
#             sqlite_operator.execute(context=None)
#
#     read_dataset() >> create_table() >> insert_selected_data()
#
#
# data_transformation_storage_pipeline()


# 2
@dag(dag_id='data_transformation_storage_pipeline',
     description='Data transformation and storage pipeline',
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@once',
     tags=['sqlite', 'python', 'taskflow_api', 'xcoms'])
def data_transformation_storage_pipeline():
    @task
    def read_car_data():
        df = pd.read_csv('/home/jjrex8988/airflow/dags/datasets/car_data.csv')

        return df.to_json()

    @task
    def read_car_categories():
        df = pd.read_csv('/home/jjrex8988/airflow/dags/datasets/car_categories.csv')

        return df.to_json()

    @task
    def create_table_car_data():
        sqlite_operator = SqliteOperator(
            task_id="create_table_car_data",
            sqlite_conn_id="my_sqlite_conn",
            sql="""CREATE TABLE IF NOT EXISTS car_data (
                        id INTEGER PRIMARY KEY,
                        brand TEXT NOT NULL,
                        model TEXT NOT NULL,
                        body_style TEXT NOT NULL,
                        seat INTEGER NOT NULL,
                        price INTEGER NOT NULL);""",
        )

        sqlite_operator.execute(context=None)

    @task
    def create_table_car_categories():
        sqlite_operator = SqliteOperator(
            task_id="create_table_car_categories",
            sqlite_conn_id="my_sqlite_conn",
            sql="""CREATE TABLE IF NOT EXISTS car_categories (
                        id INTEGER PRIMARY KEY,
                        brand TEXT NOT NULL,
                        category TEXT NOT NULL);""",
        )
        sqlite_operator.execute(context=None)

    @task
    def insert_car_data(**kwargs):
        ti = kwargs['ti']

        json_data = ti.xcom_pull(task_ids='read_car_data')

        df = pd.read_json(json_data)

        df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]

        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        insert_query = """
            INSERT INTO car_data (brand, model, body_style, seat, price)
            VALUES (?, ?, ?, ?, ?)
        """

        parameters = df.to_dict(orient='records')

        for record in parameters:
            sqlite_operator = SqliteOperator(
                task_id="insert_car_data",
                sqlite_conn_id="my_sqlite_conn",
                sql=insert_query,
                parameters=tuple(record.values()),
            )

            sqlite_operator.execute(context=None)

    @task
    def insert_car_categories(**kwargs):
        ti = kwargs['ti']

        json_data = ti.xcom_pull(task_ids='read_car_categories')

        df = pd.read_json(json_data)

        df = df[['Brand', 'Category']]

        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        insert_query = """
            INSERT INTO car_categories (brand, category)
            VALUES (?, ?)
        """

        parameters = df.to_dict(orient='records')

        for record in parameters:
            sqlite_operator = SqliteOperator(
                task_id="insert_car_categories",
                sqlite_conn_id="my_sqlite_conn",
                sql=insert_query,
                parameters=tuple(record.values()),
            )

            sqlite_operator.execute(context=None)

    @task
    def join():
        sqlite_operator = SqliteOperator(
            task_id="join_table",
            sqlite_conn_id="my_sqlite_conn",
            sql="""CREATE TABLE IF NOT EXISTS car_details AS
                   SELECT car_data.brand, 
                          car_data.model, 
                          car_data.price, 
                          car_categories.category
                    FROM car_data JOIN car_categories 
                    ON car_data.brand = car_categories.brand;
                """,
        )

        sqlite_operator.execute(context=None)

    join_task = join()

    read_car_data() >> create_table_car_data() >> \
    insert_car_data() >> join_task
    read_car_categories() >> create_table_car_categories() >> \
    insert_car_categories() >> join_task


data_transformation_storage_pipeline()
