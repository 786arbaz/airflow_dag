from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the function to be executed by the DAG
def fetch_and_save_data():
    # Replace this with your actual data fetching and saving logic
    print("Fetching data...")
    # Fetch data
    data = [1, 2, 3, 4, 5]
    print("Saving data...")
    # Save data
    with open("/path/to/destination/data.txt", "w") as file:
        file.write("\n".join(map(str, data)))

# Define the DAG
dag = DAG(
    'simple_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

# Define the tasks in the DAG
fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_task',
    python_callable=fetch_and_save_data,
    dag=dag,
)

# Set task dependencies
fetch_and_save_task
