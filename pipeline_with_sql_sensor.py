# nano ~/airflow/dags/pipeline_with_sql_sensor.py


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'jjrex8988',
}

with DAG(
        dag_id='pipeline_wit_sql_sensor',
        description='Executing a pipeline with a SQL sensor',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        catchup=False,
        tags=['postgres', 'sensor', 'sql senor'],
) as dag:
    create_laptops_table = PostgresOperator(
        task_id='create_laptops_table',
        postgres_conn_id='postgres_connection_laptop_db',
        sql="""
            CREATE TABLE IF NOT EXISTS laptops (
                id SERIAL PRIMARY KEY,
                company VARCHAR(255),
                product VARCHAR(255),
                type_name VARCHAR(255),
                price_euros NUMERIC(10, 2)
            );
        """
    )

    create_premium_laptops_table = PostgresOperator(
        task_id='create_premium_laptops_table',
        postgres_conn_id='postgres_connection_laptop_db',
        sql="""
            CREATE TABLE IF NOT EXISTS premium_laptops (
                id SERIAL PRIMARY KEY,
                company VARCHAR(255),
                product VARCHAR(255),
                type_name VARCHAR(255),
                price_euros NUMERIC(10, 2)
            );
        """
    )

    wait_for_premium_laptops = SqlSensor(
        task_id='wait_for_premium_laptops',
        conn_id='postgres_connection_laptop_db',
        sql="SELECT EXISTS(SELECT 1 FROM laptops WHERE price_euros > 500)",
        poke_interval=10,
        timeout=10 * 60
    )

    insert_data_into_premium_laptops_table = PostgresOperator(
        task_id='insert_data_into_premium_laptops_table',
        postgres_conn_id='postgres_connection_laptop_db',
        sql="""INSERT INTO premium_laptops 
               SELECT * FROM laptops WHERE price_euros > 500"""
    )

    delete_laptop_data = PostgresOperator(
        task_id='delete_laptop_data',
        postgres_conn_id='postgres_connection_laptop_db',
        sql="DELETE FROM laptops"
    )

    [create_laptops_table, create_premium_laptops_table] >> \
    wait_for_premium_laptops >> \
    insert_data_into_premium_laptops_table >> delete_laptop_data


# INSERT INTO laptops (id, company, product, type_name, price_euros)
# VALUES (1, 'DELL', 'Inspiron 3567', 'Notebook', 485);

# INSERT INTO laptops (id, company, product, type_name, price_euros) VALUES (2, 'Apple', 'Macbook', 'Notebook', 1103);
#
# INSERT INTO laptops (id, company, product, type_name, price_euros) VALUES (3, 'Acer', 'Aspire 5', 'Notebook', 389);
#
# INSERT INTO laptops (id, company, product, type_name, price_euros) VALUES (4, 'Dell', 'Vostro 5471', 'Ultrabook', 499);
#
# INSERT INTO laptops (id, company, product, type_name, price_euros) VALUES (5, 'Asus', 'Zenbook Pro', 'Ultrabook', 1983);