import os
from functions.add_foreign_key import add_fk
from functions.create_database import create_database
from functions.create_table import create_table
from functions.load_data import load_data

# If your docker is running on Windows or Mac replace localhost with host.docker.internal to connect to your local MySQL

def load_initial():
    create_database('housing_market',host='host.docker.internal')
    
    variables_city = 'IdCity VARCHAR(255) NOT NULL,City VARCHAR(255) NOT NULL,County VARCHAR(255) NOT NULL, State VARCHAR(255) NOT NULL,PRIMARY KEY(IdCity)'
    create_table('housing_market','city',variables_city,host='host.docker.internal')
    load_data(os.path.join(os.getcwd(),'datasets','cities.csv'),'housing_market','city',host='host.docker.internal')
    add_fk('State','housing_market','city','state_code','foreign_state_code_city',host='host.docker.internal')

    variables = 'State VARCHAR(25) NOT NULL,StateName VARCHAR(255) NOT NULL,PRIMARY KEY(State)'
    create_table('housing_market','state_code',variables,host='host.docker.internal')
    load_data(os.path.join(os.getcwd(),'datasets','states_id.csv'),'housing_market','state_code',host='host.docker.internal')

    variables = 'Date DATE NOT NULL,Day INTEGER NOT NULL,Month INTEGER NOT NULL,Quarter INTEGER NOT NULL,Year INTEGER NOT NULL,PRIMARY KEY(Date)'
    create_table('housing_market','calendar',variables)
    load_data(os.path.join(os.getcwd(),'..','_clean_data','calendar.csv'),'housing_market','calendar')
    
    variables_listing_prices = 'IdCity VARCHAR(255) NOT NULL,Date DATE NOT NULL,MedianListingPrice_1Bedroom DECIMAL(15,5) ,MedianListingPrice_2Bedroom DECIMAL(15,5),MedianListingPrice_3Bedroom DECIMAL(15,5),MedianListingPrice_4Bedroom DECIMAL(15,5),MedianListingPrice_5BedroomOrMore DECIMAL(15,5),MedianListingPrice_AllHomes DECIMAL(15,5),MedianListingPrice_CondoCoop DECIMAL(15,5),MedianListingPrice_DuplexTriplex DECIMAL(15,5),MedianListingPrice_SingleFamilyResidence DECIMAL(15,5)'
    create_table('housing_market','listing_price',variables_listing_prices,host='host.docker.internal')
    add_fk('IdCity','housing_market','listing_price','city','foreign_IdCity_listing_price',host='host.docker.internal')
    add_fk('Date','housing_market','listing_price','calendar','foreign_Date_listing_price')

    variables_rental_prices = 'IdCity VARCHAR(255) NOT NULL,Date DATE NOT NULL,MedianRentalPrice_1Bedroom DECIMAL(10,5),MedianRentalPrice_2Bedroom DECIMAL(10,5),MedianRentalPrice_3Bedroom DECIMAL(10,5),MedianRentalPrice_4Bedroom DECIMAL(10,5),MedianRentalPrice_5BedroomOrMore DECIMAL(10,5),MedianRentalPrice_AllHomes DECIMAL(10,5),MedianRentalPrice_CondoCoop DECIMAL(10,5),MedianRentalPrice_DuplexTriplex DECIMAL(10,5),MedianRentalPrice_MultiFamilyResidence5PlusUnits DECIMAL(10,5),MedianRentalPrice_SingleFamilyResidence DECIMAL(10,5),MedianRentalPrice_Studio DECIMAL(10,5)'
    create_table('housing_market','rental_price',variables_rental_prices,host='host.docker.internal')
    add_fk('IdCity','housing_market','rental_price','city','foreign_IdCity_rental_price',host='host.docker.internal')
    add_fk('Date','housing_market','rental_price','calendar','foreign_Date_rental_price')
    
    variables_crime_rate = 'State VARCHAR(25) NOT NULL,Year INTEGER,Population INTEGER,CrimePropertyRate DECIMAL(10,5),CrimeViolentRate DECIMAL(10,5)'
    create_table('housing_market','crime_rate',variables_crime_rate,host='host.docker.internal')
    add_fk('State','housing_market','crime_rate','state_code','foreign_state_code_crime_rate',host='host.docker.internal')

    variables = 'IdCity VARCHAR(255) NOT NULL,PeriodBegin DATE,PeriodEnd DATE,HomesSold DECIMAL(12,5),HomesSold_mom DECIMAL(10,5),HomesSold_yoy DECIMAL(10,5),Inventory DECIMAL(12,5),Inventory_mom DECIMAL(10,5),Inventory_yoy DECIMAL(10,5)'
    create_table('housing_market','sells_inventory',variables,host='host.docker.internal')
    add_fk('IdCity','housing_market','sells_inventory','city','foreign_IdCity_sells_inventory',host='host.docker.internal')
    add_fk('PeriodBegin','housing_market','sells_inventory','calendar','foreign_date_period_begin',column_name_parent='Date')

    variables = 'IdCity VARCHAR(255) NOT NULL,PeriodBegin DATE,PeriodEnd DATE,PriceDrops DECIMAL(10,7),PriceDrops_mom DECIMAL(10,7),PriceDrops_yoy DECIMAL(10,7)'
    create_table('housing_market','price_drops',variables,host='host.docker.internal')
    add_fk('IdCity','housing_market','price_drops','city','foreign_IdCity_price_drops',host='host.docker.internal')
    add_fk('PeriodBegin','housing_market','price_drops','calendar','price_drops_date_period_begin',column_name_parent='Date')
    
    variables = 'IdCity VARCHAR(255) NOT NULL,Year INTEGER,Month INTEGER,Type VARCHAR(255),Precipitation_inch DECIMAL(16,10),Hours DECIMAL(10,6)'
    create_table('housing_market','weather_event',variables,host='host.docker.internal')
    add_fk('IdCity','housing_market','weather_event','city','foreign_IdCity_weather_event',host='host.docker.internal')

    variables = 'IdCity VARCHAR(255) NOT NULL, Year INTEGER, PopEstimate INTEGER'
    create_table('housing_market','population',variables,host='host.docker.internal')
    add_fk('IdCity','housing_market','population','city','foreign_IdCity_population',host='host.docker.internal')

def load():
    
    load_data(os.path.join(os.getcwd(),'datasets','listing_prices.csv'),'housing_market','listing_price',host='host.docker.internal')
    load_data(os.path.join(os.getcwd(),'datasets','rental_prices.csv'),'housing_market','rental_price',host='host.docker.internal')
    load_data(os.path.join(os.getcwd(),'datasets','crime_rate.csv'),'housing_market','crime_rate',host='host.docker.internal')
    load_data(os.path.join(os.getcwd(),'datasets','homes_sold_&_total_2022.csv'),'housing_market','sells_inventory',host='host.docker.internal')
    load_data(os.path.join(os.getcwd(),'datasets','price_drops_2022.csv'),'housing_market','price_drops')
    load_data(os.path.join(os.getcwd(),'datasets','weather_events.csv'),'housing_market','weather_event',host='host.docker.internal')
    load_data(os.path.join(os.getcwd(),'..','_clean_data','population.csv'),'housing_market','population',host='host.docker.internal')
