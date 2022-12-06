import os
import pandas as pd
from functions.add_foreign_key import add_fk
from functions.create_database import create_database
from functions.create_table import create_table
from functions.load_data import load_data
from functions.private.my_aws_endpoint import my_aws_endpoint
from airflow.models import DAG
from airflow.operators.python import PythonOperator

# If your docker is running on Windows or Mac replace host with host.docker.internal to connect to your local MySQL
host=my_aws_endpoint
user='admin'

def load_initial():
    """Creates database and tables with their primary and foreign keys"""
    create_database('housing_market',host=host,user=user)
    
    variables_city = variables_city = 'IdCity VARCHAR(255) NOT NULL,City VARCHAR(255) NOT NULL,County VARCHAR(255) NOT NULL, State VARCHAR(255) NOT NULL,Latitude DECIMAL(8,5),Longitude DECIMAL(8,5),PRIMARY KEY(IdCity)'
    create_table('housing_market','city_lat_lon',variables_city,host=host,user=user)
    add_fk('State','housing_market','city','state_code','foreign_state_code_city',host=host,user=user)


    variables = 'State VARCHAR(25) NOT NULL,StateName VARCHAR(255) NOT NULL,PRIMARY KEY(State)'
    create_table('housing_market','state_code',variables,host=host,user=user)

    df = pd.DataFrame({"Date": pd.date_range('2000-01-01', '2050-12-31')})
    df["Day"] = df.Date.dt.day
    df["Month"] = df.Date.dt.month
    df["Quarter"] = df.Date.dt.quarter
    df["Year"] = df.Date.dt.year
    df.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','calendar.csv'),index=False)
    variables = 'Date DATE NOT NULL,Day INTEGER NOT NULL,Month INTEGER NOT NULL,Quarter INTEGER NOT NULL,Year INTEGER NOT NULL,PRIMARY KEY(Date)'
    create_table('housing_market','calendar',variables,host=host,user=user)
    load_data(os.path.join(os.getcwd(),'datasets','clean_data','calendar.csv'),'housing_market','calendar',host=host,user=user)
    
    variables_listing_prices = 'IdCity VARCHAR(255) NOT NULL,Date DATE NOT NULL,MedianListingPrice_1Bedroom DECIMAL(15,5) ,MedianListingPrice_2Bedroom DECIMAL(15,5),MedianListingPrice_3Bedroom DECIMAL(15,5),MedianListingPrice_4Bedroom DECIMAL(15,5),MedianListingPrice_5BedroomOrMore DECIMAL(15,5),MedianListingPrice_AllHomes DECIMAL(15,5),MedianListingPrice_CondoCoop DECIMAL(15,5),MedianListingPrice_DuplexTriplex DECIMAL(15,5),MedianListingPrice_SingleFamilyResidence DECIMAL(15,5)'
    create_table('housing_market','listing_price',variables_listing_prices,host=host,user=user)
    add_fk('IdCity','housing_market','listing_price','city','foreign_IdCity_listing_price',host=host,user=user)
    add_fk('Date','housing_market','listing_price','calendar','foreign_Date_listing_price',host=host,user=user)


    variables_rental_prices = 'IdCity VARCHAR(255) NOT NULL,Date DATE NOT NULL,MedianRentalPrice_1Bedroom DECIMAL(10,5),MedianRentalPrice_2Bedroom DECIMAL(10,5),MedianRentalPrice_3Bedroom DECIMAL(10,5),MedianRentalPrice_4Bedroom DECIMAL(10,5),MedianRentalPrice_5BedroomOrMore DECIMAL(10,5),MedianRentalPrice_AllHomes DECIMAL(10,5),MedianRentalPrice_CondoCoop DECIMAL(10,5),MedianRentalPrice_DuplexTriplex DECIMAL(10,5),MedianRentalPrice_MultiFamilyResidence5PlusUnits DECIMAL(10,5),MedianRentalPrice_SingleFamilyResidence DECIMAL(10,5),MedianRentalPrice_Studio DECIMAL(10,5)'
    create_table('housing_market','rental_price',variables_rental_prices,host=host,user=user)
    add_fk('IdCity','housing_market','rental_price','city','foreign_IdCity_rental_price',host=host,user=user)
    add_fk('Date','housing_market','rental_price','calendar','foreign_Date_rental_price',host=host,user=user)

    variables_crime_rate = 'State VARCHAR(25) NOT NULL,Year INTEGER,Population INTEGER,CrimePropertyRate DECIMAL(10,5),CrimeViolentRate DECIMAL(10,5)'
    create_table('housing_market','crime_rate',variables_crime_rate,host=host,user=user)
    add_fk('State','housing_market','crime_rate','state_code','foreign_state_code_crime_rate',host=host,user=user)

    variables = 'IdCity VARCHAR(255) NOT NULL,PeriodBegin DATE,PeriodEnd DATE,HomesSold DECIMAL(12,5),HomesSold_mom DECIMAL(10,5),HomesSold_yoy DECIMAL(10,5),Inventory DECIMAL(12,5),Inventory_mom DECIMAL(10,5),Inventory_yoy DECIMAL(10,5)'
    create_table('housing_market','sales_inventory',variables,host=host,user=user)
    add_fk('IdCity','housing_market','sales_inventory','city','foreign_IdCity_sells_inventory',host=host,user=user)
    add_fk('PeriodBegin','housing_market','sales_inventory','calendar','foreign_date_period_begin',column_name_parent='Date',host=host,user=user)

    variables = 'IdCity VARCHAR(255) NOT NULL,PeriodBegin DATE,PeriodEnd DATE,PriceDrops DECIMAL(10,7),PriceDrops_mom DECIMAL(10,7),PriceDrops_yoy DECIMAL(10,7),MedianListPrice DECIMAL(15,5)'
    create_table('housing_market','price_drop',variables,host=host,user=user)
    add_fk('IdCity','housing_market','price_drop','city','foreign_IdCity_price_drops',host=host,user=user)
    add_fk('PeriodBegin','housing_market','price_drop','calendar','price_drops_date_period_begin',column_name_parent='Date',host=host,user=user)

    variables = 'IdCity VARCHAR(255) NOT NULL,Year INTEGER,Month INTEGER,Type VARCHAR(255),Precipitation_inch DECIMAL(16,10),Hours DECIMAL(10,6)'
    create_table('housing_market','weather_event',variables,host=host,user=user)
    add_fk('IdCity','housing_market','weather_event','city','foreign_IdCity_weather_event',host=host,user=user)

    variables = 'IdCity VARCHAR(255) NOT NULL, Year INTEGER, PopEstimate INTEGER'
    create_table('housing_market','population',variables,host=host,user=user)
    add_fk('IdCity','housing_market','population','city','foreign_IdCity_population',host=host,user=user)

    variables = 'County VARCHAR(255),State VARCHAR(25),Fips_Code INTEGER,Year INTEGER,Personal_Income INTEGER'
    create_table('housing_market','personal_income',variables,host=my_aws_endpoint,user='admin')
    add_fk('State','housing_market','personal_income','state_code','foreign_state_code_personal_income',host=my_aws_endpoint,user='admin')

    load_data(os.path.join(os.getcwd(),'datasets','clean_data','cities.csv'),'housing_market','city',host=host,user=user)
    load_data(os.path.join(os.getcwd(),'datasets','clean_data','states_id.csv'),'housing_market','state_code',host=host,user=user)

def load_to_mysql(file_name,previous_task_id,table_name,**kwargs):
    """Loads file to database depending on the return value from the previous task"""
    ti = kwargs['ti']
    new = ti.xcom_pull(key='new_file',task_ids=previous_task_id)
    if new:
        load_data(os.path.join(os.getcwd(),'datasets','clean_data',file_name),'housing_market',table_name,host=host,user=user)
    
