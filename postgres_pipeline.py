# nano ~/airflow/dags/postgres_pipeline.py


# mkdir -p ~/airflow/dags/sql_statements
# nano ~/airflow/dags/sql_statements/create_customers.sql
# nano ~/airflow/dags/sql_statements/create_table_customer_purchases.sql

# ls ~/airflow/dags/sql_statements
# cat ~/airflow/dags/sql_statements/create_customers.sql
# cat ~/airflow/dags/sql_statements/create_table_customer_purchases.sql


# # create_table_customers.sql
#
# CREATE TABLE IF NOT EXISTS customers(
#             id INTEGER PRIMARY KEY,
#             name VARCHAR(50) NOT NULL
#         );


# # create_table_customer_purchases.sql
#
# CREATE TABLE IF NOT EXISTS customer_purchases(
#     id INTEGER PRIMARY KEY,
#     product VARCHAR(100) NOT NULL,
#     price INTEGER NOT NULL,
#     customer_id INTEGER NOT NULL REFERENCES customers (id)
# );


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

import csv

default_args = {
    'owner': 'jjrex8988',
}


# 1
# with DAG(
#         dag_id='postgres_pipeline',
#         description='Running a pipeline using the Postgres operator',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval='@once',
#         tags=['operator', 'postgres'],
#         template_searchpath='/home/jjrex8988/airflow/dags/sql_statements'
# ) as dag:
#     create_table_customers = PostgresOperator(
#         task_id='create_table_customers',
#         postgres_conn_id='postgres_connection',
#         sql='create_table_customers.sql'
#     )
#
#     create_table_customer_purchases = PostgresOperator(
#         task_id='create_table_customer_purchases',
#         postgres_conn_id='postgres_connection',
#         sql='create_table_customer_purchases.sql'
#     )
#
#
#     create_table_customers >> create_table_customer_purchases


# nano ~/airflow/dags/sql_statements/insert_customers.sql
# nano ~/airflow/dags/sql_statements/insert_customer_purchases.sql

# # insert_customers.sql
#
# INSERT INTO customers (id, name) VALUES (1, 'Janice Smith');
# INSERT INTO customers (id, name) VALUES (2, 'Ronald Jones');
# INSERT INTO customers (id, name) VALUES (3, 'Kiele West');
# INSERT INTO customers (id, name) VALUES (4, 'Leonard Cruise');
# INSERT INTO customers (id, name) VALUES (5, 'Rihanna Luis');
# INSERT INTO customers (id, name) VALUES (6, 'Koma Day-Lewis');


# # insert_customer_purchases.sql

# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (1, 'Bread - Hamburger Buns', 6.14, 1);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (2, 'Muffin Batt - Ban Dream Zero', 5.95, 2);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (3, 'Pastry - Choclate Baked', 2.2, 1);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (4, 'Placemat - Scallop, White', 8.07, 6);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (5, 'Sauerkraut', 7.34, 3);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (6, 'Wine - Mondavi Coastal Private', 2.69, 2);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (7, 'Pepper - Chilli Seeds Mild', 4.44, 1);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (8, 'Straw - Regular', 2.69, 3);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (9, 'Cheese - Colby', 6.12, 4);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (10, 'Croissant, Raw - Mini', 7.98, 2);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (11, 'Wine - Chardonnay South', 3.47, 5);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (12, 'Bar Nature Valley', 6.11, 6);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (13, 'Clementine', 1.78, 4);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (14, 'Sole - Fillet', 2.18, 2);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (15, 'Lemon Tarts', 3.97, 1);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (16, 'Creamers - 10%', 3.14, 1);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (17, 'Mushroom - Trumpet, Dry', 5.17, 3);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (18, 'Duck - Breast', 9.66, 5);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (19, 'Thyme - Fresh', 4.14, 1);
# INSERT INTO customer_purchases (id, product, price, customer_id)
# VALUES (20, 'Nescafe - Frothy French Vanilla', 7.87, 2);

# 2

# with DAG(
#     dag_id = 'postgres_pipeline',
#     description = 'Running a pipeline using the Postgres operator',
#     default_args = default_args,
#     start_date = days_ago(1),
#     schedule_interval = '@once',
#     tags = ['operator', 'postgres'],
#     template_searchpath = '/home/jjrex8988/airflow/dags/sql_statements'
# ) as dag:
#
#     create_table_customers = PostgresOperator(
#         task_id = 'create_table_customers',
#         postgres_conn_id = 'postgres_connection',
#         sql = 'create_table_customers.sql'
#     )
#
#     create_table_customer_purchases = PostgresOperator(
#         task_id = 'create_table_customer_purchases',
#         postgres_conn_id = 'postgres_connection',
#         sql = 'create_table_customer_purchases.sql'
#     )
#
#     insert_customers = PostgresOperator(
#         task_id = 'insert_customers',
#         postgres_conn_id = 'postgres_connection',
#         sql = 'insert_customers.sql'
#     )
#
#     insert_customer_purchases = PostgresOperator(
#         task_id = 'insert_customer_purchases',
#         postgres_conn_id = 'postgres_connection',
#         sql = 'insert_customer_purchases.sql'
#     )
#
#     create_table_customers >> create_table_customer_purchases >> \
#         insert_customers >> insert_customer_purchases


# nano ~/airflow/dags/sql_statements/joining_table.sql
# CREATE TABLE complete_customer_details
# AS
# SELECT customers.id,
#        customers.name,
#        customer_purchases.product,
#        customer_purchases.price
# FROM customers RIGHT JOIN customer_purchases
# ON customers.id = customer_purchases.customer_id;

# 3
# with DAG(
#         dag_id='postgres_pipeline',
#         description='Running a pipeline using the Postgres operator',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval='@once',
#         tags=['operator', 'postgres'],
#         template_searchpath='/home/jjrex8988/airflow/dags/sql_statements'
# ) as dag:
#     create_table_customers = PostgresOperator(
#         task_id='create_table_customers',
#         postgres_conn_id='postgres_connection',
#         sql='create_table_customers.sql'
#     )
#
#     create_table_customer_purchases = PostgresOperator(
#         task_id='create_table_customer_purchases',
#         postgres_conn_id='postgres_connection',
#         sql='create_table_customer_purchases.sql'
#     )
#
#     insert_customers = PostgresOperator(
#         task_id='insert_customers',
#         postgres_conn_id='postgres_connection',
#         sql='insert_customers.sql'
#     )
#
#     insert_customer_purchases = PostgresOperator(
#         task_id='insert_customer_purchases',
#         postgres_conn_id='postgres_connection',
#         sql='insert_customer_purchases.sql'
#     )
#
#     joining_table = PostgresOperator(
#         task_id='joining_table',
#         postgres_conn_id='postgres_connection',
#         sql='joining_table.sql'
#     )
#
#     filtering_customers = PostgresOperator(
#         task_id='filtering_customers',
#         postgres_conn_id='postgres_connection',
#         sql='''
#             SELECT name, product, price
#             FROM complete_customer_details
#             WHERE price BETWEEN %(lower_bound)s AND %(upper_bound)s
#         ''',
#         parameters={'lower_bound': 5, 'upper_bound': 9}
#     )
#
#     create_table_customers >> create_table_customer_purchases >> \
#     insert_customers >> insert_customer_purchases >> \
#     joining_table >> filtering_customers


def saving_to_csv(ti):
    filter_data = ti.xcom_pull(task_ids='filtering_customers')

    with open('/home/jjrex8988/airflow/dags/output/filtered_customer_data.csv',
              'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Name', 'Product', 'Price'])

        for row in filter_data:
            writer.writerow(row)


with DAG(
        dag_id='postgres_pipeline',
        description='Running a pipeline using the Postgres operator',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['operator', 'postgres'],
        template_searchpath='/home/jjrex8988/airflow/dags/sql_statements'
) as dag:
    create_table_customers = PostgresOperator(
        task_id='create_table_customers',
        postgres_conn_id='postgres_connection',
        sql='create_table_customers.sql'
    )

    create_table_customer_purchases = PostgresOperator(
        task_id='create_table_customer_purchases',
        postgres_conn_id='postgres_connection',
        sql='create_table_customer_purchases.sql'
    )

    insert_customers = PostgresOperator(
        task_id='insert_customers',
        postgres_conn_id='postgres_connection',
        sql='insert_customers.sql'
    )

    insert_customer_purchases = PostgresOperator(
        task_id='insert_customer_purchases',
        postgres_conn_id='postgres_connection',
        sql='insert_customer_purchases.sql'
    )

    joining_table = PostgresOperator(
        task_id='joining_table',
        postgres_conn_id='postgres_connection',
        sql='joining_table.sql'
    )

    # filtering_customers = PostgresOperator(
    #     task_id='filtering_customers',
    #     postgres_conn_id='postgres_connection',
    #     sql='''
    #         SELECT name, product, price
    #         FROM complete_customer_details
    #         WHERE price BETWEEN %(lower_bound)s AND %(upper_bound)s
    #     ''',
    #     parameters={'lower_bound': 5, 'upper_bound': 9}
    # )

    filtering_customers = PostgresOperator(
        task_id='filtering_customers',
        postgres_conn_id='postgres_connection',
        sql='''
            SELECT name, product, price
            FROM complete_customer_details
            WHERE name = ANY(%(names)s)
        ''',
        parameters={'names': ['Kiele West', 'Koma Day-Lewis']}
    )

    saving_to_csv = PythonOperator(
        task_id='saving_to_csv',
        python_callable=saving_to_csv,
    )

    create_table_customers >> create_table_customer_purchases >> \
    insert_customers >> insert_customer_purchases >> \
    joining_table >> filtering_customers >> saving_to_csv
