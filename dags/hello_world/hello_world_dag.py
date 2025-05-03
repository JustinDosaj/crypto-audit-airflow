from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore

from common.say_hello import say_hello

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='hello_world',
    default_args=default_args,
    description="Simple hello world DAG",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hello-worlds']
) as dag:
    start = EmptyOperator(task_id='start'),
    end = EmptyOperator(task_id='end'),

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello
    )

    (
        start >> hello_task >> end
    )