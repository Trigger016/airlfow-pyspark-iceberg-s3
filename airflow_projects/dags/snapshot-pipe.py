# dev only (pylint editing purpose, comment out this before run)
# import sys
# sys.path.append('/home/trigger/projects/interview-projects/insignia-flows/airflow_projects')
from scripts.main_pipeline import helper

from pendulum import datetime

from airflow.sdk import DAG
from airflow.utils.email import send_email
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="main-pipeline",
    schedule='0 0 1 * *',
    catchup=True,
    start_date=datetime(2020, 5, 16),
):

    start = EmptyOperator(task_id='start')
    end = TriggerDagRunOperator(
                task_id='trigger_credit-score-pipeline',
                trigger_dag_id='credit-score-pipeline',
                conf={"run_date": "{{ ds }}"}, 
                trigger_rule='all_success'
            )

    task_submit = SparkSubmitOperator(
        task_id=f'account-snapshots',
        application='/opt/airflow/projects/scripts/main_pipeline/pyscripts/snapshot-job.py',
        application_args=['--run_date', '{{ ds }}'],
        conn_id='spark_default',
        name=f'account-snapshot_ignition',
        verbose=True,
        trigger_rule='all_success',
        executor_cores=2,
        executor_memory='2g',
        retries=3,
        retry_delay=5,
        on_failure_callback=[helper.notify_email_on_failure]
    )

    start >> task_submit >> end