import os
import pandas as pd
from functions.add_foreign_key import add_fk
from functions.create_database import create_database
from functions.create_table import create_table
from functions.load_data import load_data
from private.my_aws_endpoint import my_aws_endpoint

# If your docker is running on Windows or Mac replace localhost with host.docker.internal to connect to your local MySQL

def load_initial():
    create_database('housing_market',host=my_aws_endpoint,user='admin')
    
    variables_city = 'IdCity VARCHAR(255) NOT NULL,City VARCHAR(255) NOT NULL,County VARCHAR(255) NOT NULL, State VARCHAR(255) NOT NULL,PRIMARY KEY(IdCity)'
    create_table('housing_market','city',variables_city,host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'datasets','cities.csv'),'housing_market','city',host=my_aws_endpoint,user='admin')
    add_fk('State','housing_market','city','state_code','foreign_state_code_city',host=my_aws_endpoint,user='admin')


    variables = 'State VARCHAR(25) NOT NULL,StateName VARCHAR(255) NOT NULL,PRIMARY KEY(State)'
    create_table('housing_market','state_code',variables,host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'datasets','states_id.csv'),'housing_market','state_code',host=my_aws_endpoint,user='admin')

    
    variables_listing_prices = 'IdCity VARCHAR(255) NOT NULL,Date DATE NOT NULL,MedianListingPrice_1Bedroom DECIMAL(15,5) ,MedianListingPrice_2Bedroom DECIMAL(15,5),MedianListingPrice_3Bedroom DECIMAL(15,5),MedianListingPrice_4Bedroom DECIMAL(15,5),MedianListingPrice_5BedroomOrMore DECIMAL(15,5),MedianListingPrice_AllHomes DECIMAL(15,5),MedianListingPrice_CondoCoop DECIMAL(15,5),MedianListingPrice_DuplexTriplex DECIMAL(15,5),MedianListingPrice_SingleFamilyResidence DECIMAL(15,5)'
    create_table('housing_market','listing_price',variables_listing_prices,host=my_aws_endpoint,user='admin')
    add_fk('IdCity','housing_market','listing_price','city','foreign_IdCity_listing_price',host=my_aws_endpoint,user='admin')


    variables_rental_prices = 'IdCity VARCHAR(255) NOT NULL,Date DATE NOT NULL,MedianRentalPrice_1Bedroom DECIMAL(10,5),MedianRentalPrice_2Bedroom DECIMAL(10,5),MedianRentalPrice_3Bedroom DECIMAL(10,5),MedianRentalPrice_4Bedroom DECIMAL(10,5),MedianRentalPrice_5BedroomOrMore DECIMAL(10,5),MedianRentalPrice_AllHomes DECIMAL(10,5),MedianRentalPrice_CondoCoop DECIMAL(10,5),MedianRentalPrice_DuplexTriplex DECIMAL(10,5),MedianRentalPrice_MultiFamilyResidence5PlusUnits DECIMAL(10,5),MedianRentalPrice_SingleFamilyResidence DECIMAL(10,5),MedianRentalPrice_Studio DECIMAL(10,5)'
    create_table('housing_market','rental_price',variables_rental_prices,host=my_aws_endpoint,user='admin')
    add_fk('IdCity','housing_market','rental_price','city','foreign_IdCity_rental_price',host=my_aws_endpoint,user='admin')
    
    variables_crime_rate = 'State VARCHAR(25) NOT NULL,Year INTEGER,Population INTEGER,CrimePropertyRate DECIMAL(10,5),CrimeViolentRate DECIMAL(10,5)'
    create_table('housing_market','crime_rate',variables_crime_rate,host=my_aws_endpoint,user='admin')
    add_fk('State','housing_market','crime_rate','state_code','foreign_state_code_crime_rate',host=my_aws_endpoint,user='admin')

    variables = 'IdCity VARCHAR(255) NOT NULL,PeriodBegin DATE,PeriodEnd DATE,HomesSold DECIMAL(12,5),HomesSold_mom DECIMAL(10,5),HomesSold_yoy DECIMAL(10,5),Inventory DECIMAL(12,5),Inventory_mom DECIMAL(10,5),Inventory_yoy DECIMAL(10,5)'
    create_table('housing_market','sells_inventory',variables,host=my_aws_endpoint,user='admin')
    add_fk('IdCity','housing_market','sells_inventory','city','foreign_IdCity_sells_inventory',host=my_aws_endpoint,user='admin')

    variables = 'IdCity VARCHAR(255) NOT NULL,PeriodBegin DATE,PeriodEnd DATE,PriceDrops DECIMAL(10,7),PriceDrops_mom DECIMAL(10,7),PriceDrops_yoy DECIMAL(10,7)'
    create_table('housing_market','price_drops',variables,host=my_aws_endpoint,user='admin')
    add_fk('IdCity','housing_market','price_drops','city','foreign_IdCity_price_drops',host=my_aws_endpoint,user='admin')
    
    variables = 'IdCity VARCHAR(255) NOT NULL,Year INTEGER,Month INTEGER,Type VARCHAR(255),Precipitation_inch DECIMAL(16,10),Hours DECIMAL(10,6)'
    create_table('housing_market','weather_event',variables,host=my_aws_endpoint,user='admin')
    add_fk('IdCity','housing_market','weather_event','city','foreign_IdCity_weather_event',host=my_aws_endpoint,user='admin')

    variables = 'IdCity VARCHAR(255) NOT NULL, Year INTEGER, PopEstimate INTEGER'
    create_table('housing_market','population',variables,host=my_aws_endpoint,user='admin')
    add_fk('IdCity','housing_market','population','city','foreign_IdCity_population',host=my_aws_endpoint,user='admin')


def load():
    
    load_data(os.path.join(os.getcwd(),'datasets','listing_prices.csv'),'housing_market','listing_price',host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'datasets','rental_prices.csv'),'housing_market','rental_price',host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'datasets','crime_rate.csv'),'housing_market','crime_rate',host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'datasets','homes_sold_&_total_2022.csv'),'housing_market','sells_inventory',host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'datasets','price_drops_2022.csv'),'housing_market','price_drops',host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'datasets','weather_events.csv'),'housing_market','weather_event',host=my_aws_endpoint,user='admin')
    load_data(os.path.join(os.getcwd(),'..','_clean_data','population.csv'),'housing_market','population',host=my_aws_endpoint,user='admin')
