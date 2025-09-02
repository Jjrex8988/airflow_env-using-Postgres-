# nano ~/airflow/dags/passing_data_with_taskflow.py

import time

import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.decorators import dag, task

default_args = {
    'owner': 'jjrex8988',
}


@dag(dag_id='cross_task_communication_taskflow',
     description='Xcom using the TaskFlow API',
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@once',
     tags=['xcom', 'python', 'taskflow_api'])
# 1
# def passing_data_with_taskflow_api():
#     @task
#     def get_order_prices():
#         order_price_data = {
#             'o1': 234.45,
#             'o2': 10.00,
#             'o3': 34.77,
#             'o4': 45.66,
#             'o5': 399
#         }
#
#         return order_price_data
#
#     @task
#     def compute_sum(order_price_data: dict):
#
#         total = 0
#         for order in order_price_data:
#             total += order_price_data[order]
#
#         return total
#
#     @task
#     def compute_average(order_price_data: dict):
#
#         total = 0
#         count = 0
#         for order in order_price_data:
#             total += order_price_data[order]
#             count += 1
#
#         average = total / count
#
#         return average
#
#     @task
#     def display_result(total, average):
#
#         print("Total price of goods {total}".format(total=total))
#         print("Average price of goods {average}".format(average=average))
#
#     order_price_data = get_order_prices()
#
#     total = compute_sum(order_price_data)
#     average = compute_average(order_price_data)
#
#     display_result(total, average)
#
#
# passing_data_with_taskflow_api()


# 2
def passing_data_with_taskflow_api():
    @task
    def get_order_prices():
        order_price_data = {
            'o1': 234.45,
            'o2': 10.00,
            'o3': 34.77,
            'o4': 45.66,
            'o5': 399
        }

        return order_price_data

    # @task
    @task(multiple_outputs=True)  # Related to #4
    def compute_total_and_average(order_price_data: dict):
        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order]
            count += 1

        average = total / count

        return {'total_price': total, 'average_price': average}

    # 3
    # @task
    # def display_result(price_summary_data: dict):
    #     total = price_summary_data['total_price']
    #     average = price_summary_data['average_price']
    #
    #     print("Total price of goods {total}".format(total=total))
    #     print("Average price of goods {average}".format(average=average))
    #
    # order_price_data = get_order_prices()
    #
    # price_summary_data = compute_total_and_average(order_price_data)
    #
    # display_result(price_summary_data)

    # 4
    @task
    def display_result(total, average):
        print("Total price of goods {total}".format(total=total))
        print("Average price of goods {average}".format(average=average))

    order_price_data = get_order_prices()

    price_summary_data = compute_total_and_average(order_price_data)

    # airflow.exceptions.XComNotFound: XComArg result from compute_total_and_average at cross_task_communication_taskflow with key="total_price" is not found!
    display_result(
        price_summary_data['total_price'],
        price_summary_data['average_price']
    )


passing_data_with_taskflow_api()
