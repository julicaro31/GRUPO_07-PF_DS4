from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from load_mysql_local import load_initial,load



with DAG(
        dag_id="upload",
        schedule_interval="@once",
        start_date=datetime(2022, 11, 22),
        catchup=False
) as dag:
    UploadFileInitial = PythonOperator(
        task_id='upload_initial',
        python_callable=load_initial)

    UploadFile = PythonOperator(
        task_id='upload_file',
        python_callable=load,
        dag=dag)

    UploadFileInitial >> UploadFile