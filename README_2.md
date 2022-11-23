# **HOUSING MARKET**

## acá va lo que escribimos en la semana 1


## ETL Process

### Extract
We extract the data from different sources:
- Housing Market: https://www.zillow.com/, https://www.redfin.com/news/data-center/
- Crime: https://corgis-edu.github.io/corgis/csv/state_crime/
- Weather: https://www.kaggle.com/datasets/sobhanmoosavi/us-weather-events?resource=download

### Transform

Using Python, mostly Pandas library, we transform the data to save the information we need in a structured and uniform format.

### Load

We upload the data to a MySQL data base in the cloud using RDS from Amazon Web Services.
The data is uploaded using Python, which connects to MySQL with ***mysql.connector***.
The uploads are scheduled using Airflow DAGs.


