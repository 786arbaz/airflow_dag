from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from mysql_script import execute_mysql_query

# Define your MySQL connection string ID
mysql_conn_id = 'mysql_conn'

# Define your DAG
dag = DAG(
    'mysql_insert_dag',
    description='A DAG to insert values into MySQL table',
    schedule_interval=None,  # Set your desired schedule or None for manual triggering
    start_date=datetime(2024, 2, 28),
    catchup=False
)

# Define a task using PythonOperator and link it to your Python function
mysql_insert_task = PythonOperator(
    task_id='mysql_insert_task',
    python_callable=execute_mysql_query,
    provide_context=True,  # Set to True if your function requires the context
    op_args=[mysql_conn_id],  # Pass any additional arguments your function needs
    dag=dag,
    retries=3
)
