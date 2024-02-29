from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 17)
}

# Instantiate the DAG object
with DAG('sample_dag1',
         default_args=default_args,
         description='A simple sample DAG',
         schedule_interval='@daily',
         catchup=False) as dag:

    # Define the tasks
    start_task = DummyOperator(task_id='start_task')

    # Task 1: Python operator
    def task1_function():
        print("Executing Task 1")

    task1 = PythonOperator(
        task_id='task1',
        python_callable=task1_function
    )

    # Task 2: Dummy operator
    task2 = DummyOperator(task_id='task2')

    # Task 3: Python operator
    def task3_function():
        print("Executing Task 3")

    task3 = PythonOperator(
        task_id='task3',
        python_callable=task3_function
    )

    end_task = DummyOperator(task_id='end_task')

    # Define the task dependencies
    start_task >> task1 >> task2 >> task3 >> end_task