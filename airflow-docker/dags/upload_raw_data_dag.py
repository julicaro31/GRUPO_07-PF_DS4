import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('s3_conn')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name


def upload_to_s3(folder: str, bucket_name: str) -> None: 
    """Uploads new files to S3 bucket""" 
    hook = S3Hook('s3_conn')
    for file in os.listdir(path=folder):
        if ('.csv' in file) or ('.tsv' in file):
            try:
                hook.load_file(filename=os.path.join(folder,file), key=file, bucket_name=bucket_name)
            except ValueError:
                pass

# This dag upload all our raw data to an S3 bucket

with DAG(
    dag_id='s3_dag_raw_data',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_upload_to_s3_raw_data = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','raw_data'),
            'bucket_name': 'rawdatagrupo07'
        }
    )