# nano ~/airflow/dags/branching_using_operators.py

import pandas as pd

from airflow.utils.dates import days_ago

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.decorators import dag, task

default_args = {
    'owner': 'jjrex8988',
}


# 1
# def read_csv_file():
#     df = pd.read_csv('/home/jjrex8988/airflow/dags/datasets/car_data.csv')
#
#     print(df)
#
#     return df.to_json()
#
#
# def determine_branch():
#     final_output = Variable.get("transform", default_var=None)
#
#     if final_output == 'filter_two_seaters':
#         return 'filter_two_seaters_task'
#     elif final_output == 'filter_fwds':
#         return 'filter_fwds_task'
#
#
# def filter_two_seaters(ti):
#     json_data = ti.xcom_pull(task_ids='read_csv_file_task')
#
#     df = pd.read_json(json_data)
#
#     two_seater_df = df[df['Seats'] == 2]
#
#     ti.xcom_push(key='transform_result', value=two_seater_df.to_json())
#     ti.xcom_push(key='transform_filename', value='two_seaters')
#
#
# def filter_fwds(ti):
#     json_data = ti.xcom_pull(task_ids='read_csv_file_task')
#
#     df = pd.read_json(json_data)
#
#     fwd_df = df[df['PowerTrain'] == 'FWD']
#
#     ti.xcom_push(key='transform_result', value=fwd_df.to_json())
#     ti.xcom_push(key='transform_filename', value='fwds')
#
#
# def write_csv_result(ti):
#
#     json_data = ti.xcom_pull(key='transform_result')
#     file_name = ti.xcom_pull(key='transform_filename')
#
#     df = pd.read_json(json_data)
#     df.to_csv('/home/jjrex8988/airflow/dags/output/{0}.csv'.format(file_name), index=False)
#
#
# with DAG(
#     dag_id = 'branching_using_operators',
#     description = 'Branching using operators',
#     default_args = default_args,
#     start_date = days_ago(1),
#     schedule_interval = '@once',
#     tags = ['branching', 'python', 'operators']
# ) as dag:
#     read_csv_file_task = PythonOperator(
#         task_id = 'read_csv_file_task',
#         python_callable = read_csv_file
#     )
#
#     determine_branch_task = BranchPythonOperator(
#         task_id='determine_branch_task',
#         python_callable=determine_branch
#     )
#
#     filter_two_seaters_task = PythonOperator(
#         task_id='filter_two_seaters_task',
#         python_callable=filter_two_seaters
#     )
#
#     filter_fwds_task = PythonOperator(
#         task_id='filter_fwds_task',
#         python_callable=filter_fwds
#     )
#
#     write_csv_result_task = PythonOperator(
#         task_id = 'write_csv_result_task',
#         python_callable = write_csv_result,
#         trigger_rule='none_failed'
#     )
#
# read_csv_file_task >> determine_branch_task >> \
#     [filter_two_seaters_task, filter_fwds_task] >> write_csv_result_task
#


# 2
@dag(
    dag_id='branching_using_taskflow',
    description='Branching using taskflow',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['branching', 'python', 'taskflow']
)
def branching_using_taskflow():
    def read_csv_file():
        df = pd.read_csv('/home/jjrex8988/airflow/dags/datasets/car_data.csv')

        print(df)

        return df.to_json()

    # @task.branch
    # def determine_branch():
    #     final_output = Variable.get("transform", default_var=None)
    #
    #     if final_output == 'filter_two_seaters':
    #         return 'filter_two_seaters_task'
    #     elif final_output == 'filter_five_seaters':
    #         return 'filter_five_seaters_task'
    #     elif final_output == 'filter_fwds':
    #         return 'filter_fwds_task'

    # EXTRA LEARNING FOR TWO VARIABLES VALUE # KEY = transform, VALUE filter_two_seaters, filter_five_seaters
    @task.branch
    def determine_branch():
        choice = Variable.get("transform", default_var='filter_two_seaters')
        selected = [c.strip() for c in choice.split(',')]

        mapping = {
            "filter_two_seaters": "filter_two_seaters_task",
            "filter_five_seaters": "filter_five_seaters_task",
            "filter_fwds": "filter_fwds_task",
        }

        return [mapping[c] for c in selected if c in mapping]

    def filter_two_seaters(ti):
        json_data = ti.xcom_pull(task_ids='read_csv_file_task')

        df = pd.read_json(json_data)

        two_seater_df = df[df['Seats'] == 2]

        ti.xcom_push(key='transform_result', value=two_seater_df.to_json())
        ti.xcom_push(key='transform_filename', value='two_seaters')

    def filter_five_seaters(ti):
        json_data = ti.xcom_pull(task_ids='read_csv_file_task')

        df = pd.read_json(json_data)

        five_seater_df = df[df['Seats'] == 5]

        ti.xcom_push(key='transform_result', value=five_seater_df.to_json())
        ti.xcom_push(key='transform_filename', value='five_seaters')

    def filter_fwds(ti):
        json_data = ti.xcom_pull(task_ids='read_csv_file_task')

        df = pd.read_json(json_data)

        fwd_df = df[df['PowerTrain'] == 'FWD']

        ti.xcom_push(key='transform_result', value=fwd_df.to_json())
        ti.xcom_push(key='transform_filename', value='fwds')

    # def write_csv_result(ti):
    #
    #     json_data = ti.xcom_pull(key='transform_result')
    #     file_name = ti.xcom_pull(key='transform_filename')
    #
    #     df = pd.read_json(json_data)
    #
    #     df.to_csv(
    #         '/home/jjrex8988/airflow/dags/output/{0}.csv'.format(file_name), index=False)

    # EXTRA LEARNING
    from airflow.utils.trigger_rule import TriggerRule

    def write_csv_result(ti):
        outputs = []
        for tid, name in [
            ("filter_two_seaters_task", "two_seaters"),
            ("filter_five_seaters_task", "five_seaters"),
            ("filter_fwds_task", "fwds"),
        ]:
            # Pull only from that branch
            json_data = ti.xcom_pull(task_ids=tid, key='transform_result')
            if json_data:
                df = pd.read_json(json_data)
                df.to_csv(f'/home/jjrex8988/airflow/dags/output/{name}.csv', index=False)
                outputs.append(name)

        print(f"Wrote: {outputs}")



    read_csv_file_task = PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file
    )

    filter_two_seaters_task = PythonOperator(
        task_id='filter_two_seaters_task',
        python_callable=filter_two_seaters
    )

    filter_five_seaters_task = PythonOperator(
        task_id='filter_five_seaters_task',
        python_callable=filter_five_seaters
    )

    filter_fwds_task = PythonOperator(
        task_id='filter_fwds_task',
        python_callable=filter_fwds
    )

    # write_csv_result_task = PythonOperator(
    #     task_id='write_csv_result_task',
    #     python_callable=write_csv_result,
    #     trigger_rule='none_failed'
    # )

    # EXTRA LEARNING FOR TWO VARIABLES VALUE
    write_csv_result_task = PythonOperator(
        task_id='write_csv_result_task',
        python_callable=write_csv_result,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # tolerate skipped branches
    )

    read_csv_file_task >> determine_branch() >> [
        filter_two_seaters_task,
        filter_five_seaters_task,
        filter_fwds_task
    ] >> write_csv_result_task


branching_using_taskflow()



