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
    'python_operator_dag',
    default_args=default_args,
    description='A simple DAG using PythonOperator',
    schedule_interval=timedelta(days=1),  # Set your desired schedule
)

# Define a Python function to be executed by the PythonOperator
def transform_data():
    data = [1, 2, 3, 4, 5]
    transformed_data = [x * 2 for x in data]
    print("Transformed Data:", transformed_data)

# Instantiate a PythonOperator, specifying the task_id, python_callable, and DAG
python_task = PythonOperator(
    task_id='python_transform_task',
    python_callable=transform_data,
    dag=dag,
)

# Set the task dependency: python_task will run after the DAG starts
python_task

if __name__ == "__main__":
    dag.cli()
