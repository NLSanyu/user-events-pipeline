from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from decouple import config
from scripts.amplitude_to_s3_upload import upload_to_s3
from scripts.s3_to_mongo_download import download_to_mongo


def upload_amplitude_data():
    upload_to_s3()

def download_s3_data():
    download_to_mongo()

default_args = {
    'owner': 'Lydia',
    'start_date': datetime(2021, 5, 7),
    'email': 'some@mail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=20)
}

data_transfer_dag = DAG(
    dag_id='prepare_data',
    default_args=default_args,
    schedule_interval='@daily'
)

upload_amplitude_data_task = PythonOperator(
    task_id='upload_amplitude_data',
    python_callable=upload_amplitude_data,
    dag=data_transfer_dag
)

download_s3_data_task = PythonOperator(
    task_id='download_s3_data',
    python_callable=download_s3_data,
    dag=data_transfer_dag
)

upload_amplitude_data_task >> download_s3_data_task
