from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# 定义默认参数
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 24),
    'retries': 1,
}

# 创建DAG实例
dag = DAG(
    'print_task_dag',
    default_args=default_args,
    schedule_interval=None,  # 设置为None，手动触发DAG运行
)

# 创建任务：开始任务
start_task = DummyOperator(task_id='start_task', dag=dag)

# 创建任务：打印消息的Python函数
def print_message():
    print("Hello, Airflow!")

# 创建任务：PythonOperator任务
print_message_task = PythonOperator(
    task_id='print_message_task',
    python_callable=print_message,
    dag=dag,
)

# 创建任务：结束任务
end_task = DummyOperator(task_id='end_task', dag=dag)

# 设置任务的依赖关系
start_task >> print_message_task >> end_task
