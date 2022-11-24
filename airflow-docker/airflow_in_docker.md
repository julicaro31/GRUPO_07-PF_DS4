## Running Airflow in Docker

Sources: 
- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- https://www.youtube.com/watch?v=aTaytcxy2Ck&t=154s

Prerequisites: Docker and Docker Compose installed and running.

First we fecth the docker-compose.yaml file running the command:

- curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.4.3/docker-compose.yaml'

We add in the file (in volumes):
- ./datasets:/opt/airflow/datasets

To install dependencies: 
- Create the files ***requirements.txt*** and  ***Dockerfile***
- Replace (in .yalm file) **image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.1.0}** with **build: .**

AWS configuration:
- We add the AWS credentials as ENV variables in docker-compose.yaml file
- We also add the AWS CLI image which can be used to access AWS S3 objects to verify if required data has been stored on S3 or not:

awscli:
      image: amazon/aws-cli
      entrypoint: tail -f /dev/null
      environment:      
        <<: *airflow-common-env


We create the following folders (they will be sincronized with the containers):
- mkdir ./dags ./plugins ./logs ./datasets

To make sure that user permissions are the same between those folders and the containers (in Linux or MacOS):

- echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

We inicialize Airflow intance:

- docker-compose up airflow-init

To run all the services:

- docker-compose up

Web server:

- localhost:8080
- user: airflow
- password: airflow
