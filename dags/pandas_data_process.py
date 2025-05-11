from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_dummy_data(**kwargs):
    """Generate a DataFrame with dummy data"""
    np.random.seed(42)
    data = {
        'id': range(1, 1001),
        'value': np.random.randint(1, 100, size=1000),
        'category': np.random.choice(['A', 'B', 'C'], size=1000),
        'timestamp': pd.date_range(start='2023-01-01', periods=1000, freq='H')
    }
    df = pd.DataFrame(data)
    time.sleep(5)  # Simulate processing time
    return df

def transform_data(**kwargs):
    """Perform pandas transformations"""
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='generate_data')
    
    # Transformation 1: Calculate mean by category
    time.sleep(3)
    mean_by_category = df.groupby('category')['value'].mean().reset_index()
    mean_by_category.columns = ['category', 'mean_value']
    
    # Transformation 2: Add a new column
    time.sleep(2)
    df['value_squared'] = df['value'] ** 2
    
    # Transformation 3: Filter data
    time.sleep(1)
    filtered_df = df[df['value'] > 50]
    
    return {
        'mean_by_category': mean_by_category.to_dict(),
        'filtered_count': len(filtered_df),
        'max_value_squared': df['value_squared'].max()
    }

def log_results(**kwargs):
    """Log final results"""
    ti = kwargs['ti']
    results = ti.xcom_pull(task_ids='transform_data')
    
    print("\n=== Transformation Results ===")
    print(f"Mean by category:\n{results['mean_by_category']}")
    print(f"Filtered rows count: {results['filtered_count']}")
    print(f"Max squared value: {results['max_value_squared']}")

with DAG(
    'dummy_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['example'],
) as dag:

    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_dummy_data,
        provide_context=True,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    log_results = PythonOperator(
        task_id='log_results',
        python_callable=log_results,
        provide_context=True,
    )

    generate_data >> transform_data >> log_results
