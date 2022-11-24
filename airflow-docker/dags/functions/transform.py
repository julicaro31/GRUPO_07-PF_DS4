import os
import pandas as pd
from functions.private.s3_aws import access_key, secret_access_key

def transform_rental_price():

    df = pd.read_csv(f"s3://rawdatagrupo07/City_time_series.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
        },nrows=10000
    )

    df.Date = pd.to_datetime(df.Date)

    df = df[df['Date']>='2011-01-01']

    df.rename(columns = {'RegionName':'Unique_City_ID'}, inplace = True)

    rental_prices = df[['Unique_City_ID','Date','MedianRentalPrice_1Bedroom',
       'MedianRentalPrice_2Bedroom', 'MedianRentalPrice_3Bedroom',
       'MedianRentalPrice_4Bedroom', 'MedianRentalPrice_5BedroomOrMore',
       'MedianRentalPrice_AllHomes', 'MedianRentalPrice_CondoCoop',
       'MedianRentalPrice_DuplexTriplex','MedianRentalPrice_MultiFamilyResidence5PlusUnits',
       'MedianRentalPrice_SingleFamilyResidence', 'MedianRentalPrice_Studio']]

    rental_prices = rental_prices.drop_duplicates(subset=['MedianRentalPrice_1Bedroom','MedianRentalPrice_2Bedroom', 'MedianRentalPrice_3Bedroom','MedianRentalPrice_4Bedroom', 'MedianRentalPrice_5BedroomOrMore','MedianRentalPrice_AllHomes', 'MedianRentalPrice_CondoCoop','MedianRentalPrice_DuplexTriplex','MedianRentalPrice_MultiFamilyResidence5PlusUnits','MedianRentalPrice_SingleFamilyResidence', 'MedianRentalPrice_Studio'])
    
    rental_prices.fillna(0,inplace=True)

    rental_prices.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','testing_rental_prices.csv'),index=False)

