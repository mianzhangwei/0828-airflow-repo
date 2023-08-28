from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, Airflow!")

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 8, 15),
}

dag = DAG(
    'hello_airflow_dag',
    default_args=default_args,
    schedule_interval=None,  # 禁止自动触发
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

