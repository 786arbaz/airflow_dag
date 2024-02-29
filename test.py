from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args dictionary to set the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG with the defined default_args
dag = DAG(
    'hello_airflow_dag',
    default_args=default_args,
    description='A simple DAG that prints Hello, Airflow!',
    schedule_interval=timedelta(days=1),  # Set your desired schedule
)

# Define a Python function to be executed by the PythonOperator
def print_hello():
    print("Hello, Airflow!")

# Instantiate a PythonOperator, specifying the task_id, python_callable, and DAG
hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Set the task dependency: hello_task will run after the DAG starts
hello_task

if __name__ == "__main__":
    dag.cli()
