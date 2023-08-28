from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'baidu_curl_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
)

def check_curl_output(**kwargs):
    ti = kwargs['ti']
    curl_output = ti.xcom_pull(task_ids='curl_baidu')
    
    # Check if the curl output contains a specific keyword to determine success
    if '百度一下' in curl_output:
        print("Success: Content found in curl output")
    else:
        print("Failure: Content not found in curl output")

curl_baidu_task = BashOperator(
    task_id='curl_baidu',
    bash_command='curl www.baidu.com',
    dag=dag,
)

check_curl_task = PythonOperator(
    task_id='check_curl',
    python_callable=check_curl_output,
    provide_context=True,
    dag=dag,
)

curl_baidu_task >> check_curl_task
