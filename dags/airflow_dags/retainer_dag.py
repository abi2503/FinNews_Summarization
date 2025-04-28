# airflow_dags/retrainer_dag.py
from airflow.decorators import dag, task

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id="retrainer_dag",
    default_args=default_args,
    description="Retrain models on new financial news data",
    schedule_interval="@weekly",  # or "@monthly"
    start_date=days_ago(1),
    catchup=False,
    tags=["retraining", "models"]
)
def retrain_models():

    @task
    def update_dataset():
        os.system("python retrainer/update_dataset.py")

    @task
    def auto_finetune():
        os.system("python retrainer/auto_finetune.py")

    update_dataset() >> auto_finetune()

dag = retrain_models()
