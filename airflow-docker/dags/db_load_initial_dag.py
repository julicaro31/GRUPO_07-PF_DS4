from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from functions.load_mysql import load_initial

# This is the initial dag to create the database, the tables and their primary and foreign keys.

with DAG(
        dag_id="upload_initial",
        schedule_interval="@once",
        start_date=datetime(2022, 11, 22),
        catchup=False
) as dag:
    UploadInitial = PythonOperator(
        task_id='upload_initial',
        python_callable=load_initial)


