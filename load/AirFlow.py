from airflow import DAG
from datetime import datetime
import pandas as pd

from airflow.utils.email import send_email
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


def transformation():
    trainDetailsDF = pd.read_csv('gs://input_csv/Event_File_03_16_2022.csv')
    print(trainDetailsDF.head())


with DAG(
        dag_id="pipeline_demo",
        schedule_interval="@hourly",
        start_date=datetime(2022, 1, 23),
        catchup=False
) as dag:
    buisness_logic_task = PythonOperator(
        task_id='ApplyBusinessLogic',
        python_callable=transformation,
        dag=dag)

    upload_task = GCSToGCSOperator(
        task_id='upload_task',
        source_bucket='input_csv',
        destination_bucket='transformed_csv',
        source_object='Event_File_03_16_2022.csv',
        move_object=True,
        dag=dag
    )

    email_task = EmailOperator(
        task_id="SendStatusEmail",
        depends_on_past=True,
        to='youremail',
        subject='Pipeline Status!',
        html_content='<p>Hi Everyone, Process completed Successfully! <p>',
        dag=dag)

    buisness_logic_task >> upload_task >> email_task


    #pagina https://morioh.com/p/0d90c657284c