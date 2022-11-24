import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from functions.transform import transform_rental_price


def upload_to_s3(folder: str, bucket_name: str) -> None: 
    """Uploads new files to S3 bucket""" 
    hook = S3Hook('s3_conn')
    for file in os.listdir(path=folder):
        if '.csv' in file:
            try:
                hook.load_file(filename=os.path.join(folder,file), key=file, bucket_name=bucket_name)
            except ValueError:
                pass

with DAG(
    dag_id='s3_dag_testing',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform_rental_price,
    ) 

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )

task_transform>>task_upload_to_s3