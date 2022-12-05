# **HOUSING MARKET**

Given the fact that the real estate market has many ups and downs regarding the properties values, it is important to know the economic impact it could have. This can vary over time and there can be economic situations that affect the market as in 2008, when a recessive period occurred.

This project is focused on the study of the real estate market in the United States. 
Different areas of this country will be analyzed to find the best places to make an investment. Several variables will be taken into account such as the property type, time, weather, personal income and crime rate.


# Technology Stack

All the raw data obtained from different web sources is stored in a S3 AWS bucket.
This data is then extracted from the bucket and transformed using Python, mostly Pandas library in order to have the information needed in a structured and uniform format.

Once the data is clean, it is uploaded to a different S3 bucket

The data is also uploaded to a MySQL database in the cloud using RDS from Amazon Web Services. One of the advantages of working in the cloud is that all members of the team can access the database from their computers and use the cloud storage resources. Some security rules are set so only certain IPs can have access. 

The processes of extraction, transformation and load are orchestrated using Apache Airflow DAGs. Working with Airflow helps monitoring and automatizing the pipelines. Also, the workflow can be visualized using the Airflow UI.


## DER

The following image shows the Entity Relationship Diagram of our database:

<img src="_src/DER_housing_market.png"  height="700">


# Machine Learning

We train different models to predict the house prices in the United States.

## Time Series

One approach is using a time series the Seasonal ARIMA model to predict the house prices in 2023.

### Why seasonal?

In the following image we can see there's a significant partial autocorrelation at 12. This means that the price at a certain month depends on the price at the same month a year ago.

<img src="_src/autocorrelation.png"  height="500">

### Results

The following image shows the house price predictions for 2023 using this model.

<img src="_src/results_sarima.png"  height="500">