import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from functions.transform import transform_list_rental_price,transform_crime_rate,transform_weather_events,transform_redfin_data,transform_income


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
    dag_id='etl_list_rental_price',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_transform_list_rental_price = PythonOperator(
        task_id='transform_list_rental_price',
        python_callable=transform_list_rental_price,
    )

    task_upload_to_s3_list_rental_price = PythonOperator(
        task_id='upload_to_s3_list_rental_price',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )


with DAG(
    dag_id='etl_crime_rate',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_transform_crime_rate = PythonOperator(
        task_id='transform_crime_rate',
        python_callable=transform_crime_rate,
    )

    task_upload_to_s3_crime_rate = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )


with DAG(
    dag_id='etl_weather_events',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_transform_weather_events = PythonOperator(
        task_id='transform_weather_events',
        python_callable=transform_weather_events,
    )

    task_upload_to_s3_weather_events = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )


with DAG(
    dag_id='etl_redfin_data',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_transform_redfin_data = PythonOperator(
        task_id='transform_redfin_data',
        python_callable=transform_redfin_data,
    )

    task_upload_to_s3_redfin_data = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )

with DAG(
    dag_id='etl_income',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_transform_income = PythonOperator(
        task_id='transform_income',
        python_callable=transform_income,
    )

    task_upload_to_s3_income = PythonOperator(
        task_id='upload_to_s3_income',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )



task_transform_list_rental_price>>task_upload_to_s3_list_rental_price
task_transform_crime_rate>>task_upload_to_s3_crime_rate
task_transform_weather_events>>task_upload_to_s3_weather_events
task_transform_redfin_data>>task_upload_to_s3_redfin_data
task_transform_income>>task_upload_to_s3_income