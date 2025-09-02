# nano ~/airflow/dags/pipeline_with_file_sensor.py

# nano ~/airflow/dags/sql_statements/create_table_laptops.sql
# CREATE TABLE IF NOT EXISTS laptops (
#     id SERIAL PRIMARY KEY,
#     company VARCHAR(255),
#     product VARCHAR(255),
#     type_name VARCHAR(255),
#     price_euros NUMERIC(10, 2)
# );


import pandas as pd
import psycopg2
import glob

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'jjrex8988',
}

FILE_PATH = '/home/jjrex8988/airflow/dags/tmp/laptops_*.csv'
FILE_COLS = ['Id', 'Company', 'Product', 'TypeName', 'Price_euros']

OUTPUT_FILE = '//home/jjrex8988/airflow/dags/output/{}.csv'


def insert_laptop_data():
    conn = psycopg2.connect(
        host='localhost',
        database='laptop_db',
        port='5433',
        user='jjrex8988',
        password='PASSWORD', ## CHANGE TO OWN PASSWORD
    )

    cur = conn.cursor()

    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS)

        records = df.to_dict('records')

        for record in records:
            query = f"""INSERT INTO laptops 
                        (id, company, product, type_name, price_euros) 
                        VALUES (
                            {record['Id']}, 
                            '{record['Company']}', 
                            '{record['Product']}', 
                            '{record['TypeName']}', 
                            {record['Price_euros']})
                    """

            cur.execute(query)
    conn.commit()

    cur.close()
    conn.close()


def filter_gaming_laptops():
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS)

        gaming_laptops_df = df[df['TypeName'] == 'Gaming']

        gaming_laptops_df.to_csv(OUTPUT_FILE.format('gaming_laptops'),
                                 mode='a', header=False, index=False)


def filter_notebook_laptops():
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS)

        notebook_laptops_df = df[df['TypeName'] == 'Notebook']

        notebook_laptops_df.to_csv(OUTPUT_FILE.format('notebook_laptops'),
                                   mode='a', header=False, index=False)


def filter_ultrabook_laptops():
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS)

        ultrabook_laptops_df = df[df['TypeName'] == 'Ultrabook']

        ultrabook_laptops_df.to_csv(OUTPUT_FILE.format('ultrabook_laptops'),
                                    mode='a', header=False, index=False)


with DAG(
        dag_id='pipeline_with_file_sensor',
        description='Running a pipeline using a file sensor',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['pipeline', 'sensor', 'file sensor'],
        template_searchpath='/home/jjrex8988/airflow/dags/sql_statements'
) as dag:
    create_table_laptop = PostgresOperator(
        task_id='create_table_laptop',
        postgres_conn_id='postgres_connection_laptop_db',
        sql='create_table_laptops.sql'
    )

    checking_for_file = FileSensor(
        task_id='checking_for_file',
        filepath=FILE_PATH,
        poke_interval=10,
        timeout=60 * 10
    )

    insert_laptop_data = PythonOperator(
        task_id='insert_laptop_data',
        python_callable=insert_laptop_data
    )

    filter_gaming_laptops = PythonOperator(
        task_id='filter_gaming_laptops',
        python_callable=filter_gaming_laptops
    )

    filter_notebook_laptops = PythonOperator(
        task_id='filter_notebook_laptops',
        python_callable=filter_notebook_laptops
    )

    filter_ultrabook_laptops = PythonOperator(
        task_id='filter_ultrabook_laptops',
        python_callable=filter_ultrabook_laptops
    )

    delete_file = BashOperator(
        task_id='delete_file',
        bash_command='rm {0}'.format(FILE_PATH)
    )

    create_table_laptop >> checking_for_file >> insert_laptop_data >> \
    [filter_gaming_laptops, filter_notebook_laptops, filter_ultrabook_laptops] >> \
    delete_file
