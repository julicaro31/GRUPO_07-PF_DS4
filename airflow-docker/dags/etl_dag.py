import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from functions.transform import transform_list_rental_price,transform_crime_rate,transform_weather_events,transform_redfin_data,transform_income,transform_population
from functions.load_mysql import load_to_mysql

def upload_to_s3(folder: str, bucket_name: str,**kwargs) -> None: 
    """Uploads new files to S3 bucket""" 
    hook = S3Hook('s3_conn')
    new_file=False
    for file in os.listdir(path=folder):
        if '.csv' in file:
            try:
                hook.load_file(filename=os.path.join(folder,file), key=file, bucket_name=bucket_name)
                new_file=True
            except ValueError:
                pass
    if new_file:
        kwargs['ti'].xcom_push(key='new_file', value=True)
    else:
        kwargs['ti'].xcom_push(key='new_file', value=False)

# These dags extract the files (each dag for each type of table) from an S3 bucket, 
# transform it and the uploads it to a different S3 bucket if the file doesn't exits already.
# If it is a new file, it is uploaded to the database.
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
        },
    )
    task_upload_to_db_listing_prices = PythonOperator(
        task_id='upload_to_db_listing_prices',
        provide_context = True,
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'listing_prices(1).csv',
            'previous_task_id':'upload_to_s3_list_rental_price',
            'table_name': 'listing_price'
        }
    )
    task_upload_to_db_rental_prices = PythonOperator(
        task_id='upload_to_db_rental_prices',
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'rental_prices(1).csv',
            'previous_task_id':'upload_to_s3_list_rental_price',
            'table_name': 'rental_price'
        },
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
        task_id='upload_to_s3_crime_rate',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )

    task_upload_to_db_crime_rate = PythonOperator(
        task_id='upload_to_db_crime_rate',
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'crime_rate.csv',
            'previous_task_id':'upload_to_s3_crime_rate',
            'table_name': 'crime_rate'
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

    task_upload_to_db_weather_events = PythonOperator(
        task_id='upload_to_db_weather_events',
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'weather_events.csv',
            'previous_task_id':'upload_to_s3_weather_events',
            'table_name': 'weather_event'
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

    task_upload_to_db_redfin_data_1 = PythonOperator(
        task_id='upload_to_db_redfin_data_1',
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'homes_sold_&_total_2022.csv',
            'previous_task_id':'upload_to_s3',
            'table_name': 'sales_inventory'
        }
    )
    task_upload_to_db_redfin_data_2 = PythonOperator(
        task_id='upload_to_db_redfin_data_2',
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'price_drops_2022.csv',
            'previous_task_id':'upload_to_s3',
            'table_name': 'price_drop'
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

    task_upload_to_db_income = PythonOperator(
        task_id='upload_to_db_income',
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'incomebycounty1.csv',
            'previous_task_id':'upload_to_s3_income',
            'table_name': 'personal income'
        }
    )


with DAG(
    dag_id='etl_population',
    schedule_interval='@once',
    start_date=datetime(2022, 11, 23),
    catchup=False
) as dag:

    task_transform_population = PythonOperator(
        task_id='transform_population',
        python_callable=transform_population,
    )

    task_upload_to_s3_population = PythonOperator(
        task_id='upload_to_s3_population',
        python_callable=upload_to_s3,
        op_kwargs={
            'folder': os.path.join('datasets','clean_data'),
            'bucket_name': 'cleandatagrupo07'
        }
    )
    task_upload_to_db_population = PythonOperator(
        task_id='upload_to_db_population',
        python_callable=load_to_mysql,
        op_kwargs={
            'file_name': 'population.csv',
            'previous_task_id':'upload_to_s3_population',
            'table_name': 'population'
        }
    )



task_transform_list_rental_price>>task_upload_to_s3_list_rental_price>>task_upload_to_db_listing_prices
task_upload_to_s3_list_rental_price>>task_upload_to_db_rental_prices
task_transform_crime_rate>>task_upload_to_s3_crime_rate>>task_upload_to_db_crime_rate
task_transform_weather_events>>task_upload_to_s3_weather_events>>task_upload_to_db_weather_events
task_transform_redfin_data>>task_upload_to_s3_redfin_data>>task_upload_to_db_redfin_data_1
task_upload_to_s3_redfin_data>>task_upload_to_db_redfin_data_2
task_transform_income>>task_upload_to_s3_income>>task_upload_to_db_income
task_transform_population>>task_upload_to_s3_population>>task_upload_to_db_population