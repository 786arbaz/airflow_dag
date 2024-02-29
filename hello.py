from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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
    'bash_operator_dag',
    default_args=default_args,
    description='A simple DAG using BashOperator',
    schedule_interval=timedelta(days=1),  # Set your desired schedule
)

# Define a Bash command to be executed by the BashOperator
bash_command = 'echo "Hello, Airflow!"'

# Instantiate a BashOperator, specifying the task_id, bash_command, and DAG
bash_task = BashOperator(
    task_id='bash_hello_task',
    bash_command=bash_command,
    dag=dag,
)

# Set the task dependency: bash_task will run after the DAG starts
bash_task

if __name__ == "__main__":
    dag.cli()
