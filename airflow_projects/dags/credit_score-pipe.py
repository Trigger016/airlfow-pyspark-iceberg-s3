from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def simple_task():
    print("✅ Task executed successfully!")

with DAG(
    dag_id="credit-score-pipeline",
    schedule=None,
    catchup=False,
    start_date=datetime(2020, 5, 16),
    ):
    start = EmptyOperator(task_id="start")
    
    middle = PythonOperator(
        task_id="calculate_score",
        python_callable=simple_task
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> middle >> end