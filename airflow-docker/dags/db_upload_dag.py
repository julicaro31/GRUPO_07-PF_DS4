from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from functions.load_mysql_aws import load_initial,load


with DAG(
        dag_id="upload_initial",
        schedule_interval="@once",
        start_date=datetime(2022, 11, 22),
        catchup=False
) as dag:
    UploadInitial = PythonOperator(
        task_id='upload_initial',
        python_callable=load_initial)

with DAG(
        dag_id="upload",
        schedule_interval="@daily",
        start_date=datetime(2022, 11, 22),
        catchup=False
) as dag:
    UploadInitial = PythonOperator(
        task_id='upload',
        python_callable=load)

