# nano ~/airflow/dags/data_transformation_storage_operators_pipeline.py

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

import pandas as pd
import json

DEFAULT_ARGS = {"owner": "jjrex8988"}
SQLITE_CONN_ID = "my_sqlite_conn"
CAR_DATA_CSV = "/home/jjrex8988/airflow/dags/datasets/car_data.csv"
CAR_CATEGORIES_CSV = "/home/jjrex8988/airflow/dags/datasets/car_categories.csv"

# ---------- Python callables ----------
def read_car_data_fn(**_):
    df = pd.read_csv(CAR_DATA_CSV)
    df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df.to_json(orient="records")

def read_car_categories_fn(**_):
    df = pd.read_csv(CAR_CATEGORIES_CSV)
    df = df[['Brand', 'Category']]
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df.to_json(orient="records")

def insert_car_data_fn(**kwargs):
    ti = kwargs["ti"]
    payload = ti.xcom_pull(task_ids="read_car_data")
    rows = []
    if payload:
        for r in json.loads(payload):
            rows.append((r["Brand"], r["Model"], r["BodyStyle"], int(r["Seats"]), int(r["PriceEuro"])))
    if rows:
        hook = SqliteHook(sqlite_conn_id=SQLITE_CONN_ID)
        hook.insert_rows(
            table="car_data_operators",
            rows=rows,
            target_fields=["brand", "model", "body_style", "seat", "price"],
            commit_every=1000,
            replace=False,
        )

def insert_car_categories_fn(**kwargs):
    ti = kwargs["ti"]
    payload = ti.xcom_pull(task_ids="read_car_categories")
    rows = []
    if payload:
        for r in json.loads(payload):
            rows.append((r["Brand"], r["Category"]))
    if rows:
        hook = SqliteHook(sqlite_conn_id=SQLITE_CONN_ID)
        hook.insert_rows(
            table="car_categories_operators",
            rows=rows,
            target_fields=["brand", "category"],
            commit_every=1000,
            replace=False,
        )

with DAG(
    dag_id="data_transformation_storage_operators_pipeline",
    description="Data transformation and storage pipeline (operators only)",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval='@once',
    catchup=False,
    tags=["sqlite","python","operators"],
) as dag:

    # Read CSVs
    read_car_data = PythonOperator(
        task_id="read_car_data",
        python_callable=read_car_data_fn,
    )
    read_car_categories = PythonOperator(
        task_id="read_car_categories",
        python_callable=read_car_categories_fn,
    )

    # Create tables (note *_operators names)
    create_table_car_data = SqliteOperator(
        task_id="create_table_car_data",
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS car_data_operators (
            id INTEGER PRIMARY KEY,
            brand TEXT NOT NULL,
            model TEXT NOT NULL,
            body_style TEXT NOT NULL,
            seat INTEGER NOT NULL,
            price INTEGER NOT NULL
        );
        """,
    )
    create_table_car_categories = SqliteOperator(
        task_id="create_table_car_categories",
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS car_categories_operators (
            id INTEGER PRIMARY KEY,
            brand TEXT NOT NULL,
            category TEXT NOT NULL
        );
        """,
    )

    # Optional: clear tables to avoid duplicate rows on rerun
    clear_car_data = SqliteOperator(
        task_id="clear_car_data",
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="DELETE FROM car_data_operators;",
    )
    clear_car_categories = SqliteOperator(
        task_id="clear_car_categories",
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="DELETE FROM car_categories_operators;",
    )

    # Insert rows
    insert_car_data = PythonOperator(
        task_id="insert_car_data",
        python_callable=insert_car_data_fn,
    )
    insert_car_categories = PythonOperator(
        task_id="insert_car_categories",
        python_callable=insert_car_categories_fn,
    )

    # Build joined table with *_operators name
    create_joined = SqliteOperator(
        task_id="create_joined",
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS car_details_operators AS
        SELECT d.brand,
               d.model,
               d.price,
               c.category
        FROM car_data_operators d
        JOIN car_categories_operators c
          ON d.brand = c.brand;
        """,
    )
    # If you want to re-create the joined table on reruns, do this instead:
    # sql="""
    # DROP TABLE IF EXISTS car_details_operators;
    # CREATE TABLE car_details_operators AS
    # SELECT DISTINCT d.brand, d.model, d.price, c.category
    # FROM car_data_operators d
    # JOIN car_categories_operators c
    #   ON d.brand = c.brand;
    # """

    # Wiring
read_car_data >> create_table_car_data >> clear_car_data >> insert_car_data
read_car_categories >> create_table_car_categories >> clear_car_categories >> insert_car_categories
[insert_car_data, insert_car_categories] >> create_joined
