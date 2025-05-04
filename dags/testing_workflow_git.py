# Importing necessary modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Function that will be executed by PythonOperator
def print_current_date():
    print(f"Current date and time: {datetime.now()}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Defining the DAG
dag = DAG(
    'testing_workflow_feature',  # The name of the DAG
    default_args=default_args,
    description='A simple DAG that prints current date',
    schedule_interval='@daily',  # Run once a day
    start_date=datetime(2025, 1, 1),
    catchup=False,  # Disable catchup for missed schedules
)

# Defining the task that will execute the print_current_date function
print_task = PythonOperator(
    task_id='print_current_date_task',  # Task name
    python_callable=print_current_date,  # Function to be executed
    dag=dag,  # DAG to which the task belongs
)

# Setting the task to run
print_task
