import os
import pandas as pd
from functions.private.s3_aws import access_key, secret_access_key
from functions.us_state_abbrev import us_state_to_abbrev

def transform_list_rental_price():

    header = pd.read_csv(f"s3://rawdatagrupo07/City_time_series.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
        },nrows=1
    )
    
    df = pd.read_csv(f"s3://rawdatagrupo07/City_time_series.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
        },nrows=10000,skiprows=2381420,names=list(header.columns)
    )

    df['Date'] = pd.to_datetime(df['Date'])

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

    rental_prices.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','rental_prices.csv'),index=False)

    listing_prices = df[['Unique_City_ID', 'Date','MedianListingPrice_1Bedroom', 'MedianListingPrice_2Bedroom',
       'MedianListingPrice_3Bedroom', 'MedianListingPrice_4Bedroom',
       'MedianListingPrice_5BedroomOrMore', 'MedianListingPrice_AllHomes',
       'MedianListingPrice_CondoCoop', 'MedianListingPrice_DuplexTriplex',
       'MedianListingPrice_SingleFamilyResidence']]

    listing_prices = listing_prices.drop_duplicates(subset=['MedianListingPrice_1Bedroom', 'MedianListingPrice_2Bedroom',
       'MedianListingPrice_3Bedroom', 'MedianListingPrice_4Bedroom',
       'MedianListingPrice_5BedroomOrMore', 'MedianListingPrice_AllHomes',
       'MedianListingPrice_CondoCoop', 'MedianListingPrice_DuplexTriplex',
       'MedianListingPrice_SingleFamilyResidence'])

    listing_prices.fillna(0,inplace=True)

    listing_prices.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','listing_prices.csv'),index=False)


def transform_crime_rate():
    
    df = pd.read_csv(f"s3://rawdatagrupo07/state_crime.csv",
    storage_options={
        "key": access_key,
        "secret": secret_access_key
    },
    )

    df = df[df["Year"]>=2011]

    states_id = pd.DataFrame()
    states_id['StateId'] = us_state_to_abbrev.values()
    states_id['StateName'] = us_state_to_abbrev.keys()

    states_id.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','states_id_prubea.csv'),index=False)

    df2 = pd.DataFrame()

    df2['State'] = df.State.astype(str).apply(lambda x: us_state_to_abbrev[x])
    df2['Year'] = df['Year']
    df2['Population'] = df['Data.Population']
    df2['CrimePropertyRate'] = df['Data.Rates.Property.All']
    df2['CrimeViolentRate'] = df['Data.Rates.Violent.All']
    df2.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','crime_rate_prueba.csv'),index=False)

def transform_weather_events():
    df = pd.read_csv(f"s3://rawdatagrupo07/WeatherEvents_Jan2016-Dec2021.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
        },nrows=10000
    )

    df['EndTime(UTC)'] = pd.to_datetime(df['EndTime(UTC)'])
    df['StartTime(UTC)'] = pd.to_datetime(df['StartTime(UTC)'])

    df['Year'] = df['StartTime(UTC)'].dt.year
    df['Month'] = df['StartTime(UTC)'].dt.month
    df['dTime'] = df['EndTime(UTC)'] - df['StartTime(UTC)']
    df['Hours'] = df.dTime.dt.seconds/3600.0
    df2 = df.groupby(['Year','Month','State','County','City','Type'],as_index=False)[['Precipitation(in)','Hours']].sum()

    cities = pd.read_csv(f"s3://cleandatagrupo07/cities.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
        }
    )

    df3 = pd.merge(df2, cities, how = 'inner', on=['State','County','City'])
    df3.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','weather_events_prueba.csv'),columns=['Unique_City_ID','Year', 'Month', 'Type', 'Precipitation(in)', 'Hours'],index=False)


def transform_redfin_data():
    
    data = pd.read_csv(f"s3://rawdatagrupo07/city_market_tracker.tsv",
    storage_options={
        "key": access_key,
        "secret": secret_access_key
    },sep='\t',nrows=10000
    )

    data.period_begin = pd.to_datetime(data.period_begin)
    data.period_end = pd.to_datetime(data.period_end)

    data = data.rename(columns = {'period_begin':'PeriodBegin','period_end':'PeriodEnd','city':'City','state_code':'State','homes_sold':'HomesSold','homes_sold_mom':'HomesSold_mom', 'homes_sold_yoy':'HomesSold_yoy','inventory':'Inventory','inventory_mom':'Inventory_mom','inventory_yoy':'Inventory_yoy'})

    homes_sold_total = data[['PeriodBegin','PeriodEnd','City', 'State','HomesSold','HomesSold_mom', 'HomesSold_yoy','Inventory', 'Inventory_mom','Inventory_yoy']]

    cities = pd.read_csv(f"s3://cleandatagrupo07/cities.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
        }
    )

    homes_sold_total_id = pd.merge(homes_sold_total, cities, how = 'inner', on=['State','City'])

    homes_sold_total_2022 = homes_sold_total_id[homes_sold_total_id['PeriodBegin'].dt.year==2022]
    homes_sold_total_2022.dropna(inplace=True)
    homes_sold_total_2022.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','homes_sold_&_total_2022_prueba.csv'),columns=['Unique_City_ID','PeriodBegin', 'PeriodEnd','HomesSold',
       'HomesSold_mom', 'HomesSold_yoy', 'Inventory', 'Inventory_mom',
       'Inventory_yoy'],index=False)

    data = data.rename(columns = {'price_drops':'PriceDrops','price_drops_mom':'PriceDrops_mom','price_drops_yoy':'PriceDrops_yoy'})
    price_drops = data[['PeriodBegin','PeriodEnd','City', 'State','PriceDrops', 'PriceDrops_mom','PriceDrops_yoy']]
    price_drops_2022 = price_drops[price_drops.PeriodBegin.dt.year==2022]
    price_drops_id_2022 = pd.merge(price_drops_2022, cities, how = 'inner', on=['State','City'])
    price_drops_id_2022.dropna(inplace=True)
    price_drops_id_2022.PriceDrops = price_drops_id_2022.PriceDrops*100
    price_drops_id_2022.PriceDrops_mom = price_drops_id_2022.PriceDrops_mom*100
    price_drops_id_2022.PriceDrops_yoy = price_drops_id_2022.PriceDrops_yoy*100

    price_drops_id_2022.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','price_drops_2022_prueba.csv'),columns=['Unique_City_ID','PeriodBegin', 'PeriodEnd','PriceDrops','PriceDrops_mom', 'PriceDrops_yoy'],index=False)

