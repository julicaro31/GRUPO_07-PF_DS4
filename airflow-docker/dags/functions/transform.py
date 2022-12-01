import os
import pandas as pd
from functions.private.s3_aws import access_key, secret_access_key
from functions.us_state_abbrev import us_state_to_abbrev
import re

# Functions with the transform steps for each file that corresponds to a certain table in the database.
# For more information about the exploratory data analysis check the folder called extract_transform

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

    states_id.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','states_id.csv'),index=False)

    df2 = pd.DataFrame()

    df2['State'] = df.State.astype(str).apply(lambda x: us_state_to_abbrev[x])
    df2['Year'] = df['Year']
    df2['Population'] = df['Data.Population']
    df2['CrimePropertyRate'] = df['Data.Rates.Property.All']
    df2['CrimeViolentRate'] = df['Data.Rates.Violent.All']
    df2.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','crime_rate.csv'),index=False)

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
    df3.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','weather_events.csv'),columns=['Unique_City_ID','Year', 'Month', 'Type', 'Precipitation(in)', 'Hours'],index=False)


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
    homes_sold_total_2022.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','homes_sold_&_total_2022.csv'),columns=['Unique_City_ID','PeriodBegin', 'PeriodEnd','HomesSold',
       'HomesSold_mom', 'HomesSold_yoy', 'Inventory', 'Inventory_mom',
       'Inventory_yoy'],index=False)

    data = data.rename(columns = {'price_drops':'PriceDrops','price_drops_mom':'PriceDrops_mom','price_drops_yoy':'PriceDrops_yoy','median_list_price':'MedianListPrice'})
    price_drops = data[['PeriodBegin','PeriodEnd','City', 'State','PriceDrops', 'PriceDrops_mom','PriceDrops_yoy','MedianListPrice']]
    price_drops_2022 = price_drops[price_drops.PeriodBegin.dt.year==2022]
    price_drops_id_2022 = pd.merge(price_drops_2022, cities, how = 'inner', on=['State','City'])
    price_drops_id_2022.dropna(inplace=True)
    price_drops_id_2022.PriceDrops = price_drops_id_2022.PriceDrops*100
    price_drops_id_2022.PriceDrops_mom = price_drops_id_2022.PriceDrops_mom*100
    price_drops_id_2022.PriceDrops_yoy = price_drops_id_2022.PriceDrops_yoy*100

    price_drops_id_2022.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','price_drops_2022.csv'),columns=['Unique_City_ID','PeriodBegin', 'PeriodEnd','PriceDrops','PriceDrops_mom', 'PriceDrops_yoy','MedianListPrice'],index=False)


def transform_income():
    # Personal income per capita by Metropolitan Statistical Area (MSA)
    df = pd.read_csv(f"s3://rawdatagrupo07/personal_income_2011-2021bymetro.csv",
    storage_options={
        "key": access_key,
        "secret": secret_access_key
    },
    )
    df.drop(index=0, inplace=True)
    df.reset_index(inplace=True, drop=True)
    geo = df['GeoName'].str.split(pat=',', expand=True)
    geo
    msa = geo[0]
    state = geo[1].str.split(expand=True)
    state = state[0]
    df = pd.concat([msa, state, df], axis=1)
    df.drop(['GeoName'], axis=1, inplace=True )
    df.columns = ['MSA', 'State', 'Fips_Code', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']
    metro = pd.melt(df, id_vars=['MSA','State','Fips_Code'],value_vars=['2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021'],var_name='Year',value_name='Personal_Income')
    metro.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','incomebymsa.csv'), index=False)

    # Personal income per capita by County
    
    df = pd.read_csv(f"s3://rawdatagrupo07/personal_income_2017-2021bycounty.csv",
    storage_options={
        "key": access_key,
        "secret": secret_access_key
    },
    )

    df['State'] = df['GeoName'].str.split(pat=',')
    df['State'] = df['State'].apply(lambda x: x[-1])

    df['County'] = df['GeoName'].str.split(pat=',')
    df['County'] = df['County'].apply(lambda x: x[0])
    df['County'] = df['County'].str.split(pat='+')
    df['County'] = df['County'].apply(lambda x: x[0])
    df = df[['County', 'State', 'GeoFips', '2017', '2018', '2019', '2020', '2021']]

    county = pd.melt(df, id_vars=['County','State','GeoFips'],value_vars=['2017', '2018', '2019', '2020', '2021'],var_name='Year',value_name='Personal_Income')
    county.rename(columns={'GeoFips': 'Fips_Code'}, inplace=True)

    county['County'] = county['County'].astype(str).apply(lambda x: re.sub('\(.+\)','',x))
    county['County'] = county['County'].str.rstrip()
    county['State'] = county['State'].str.replace('*', '', regex=False)
    county['State'] = county['State'].str.lstrip()
    county['Personal_Income'] = county['Personal_Income'].replace('(NA)', None)

    county.isnull().sum()
    county.dropna(inplace=True)
    county['Personal_Income'] = county['Personal_Income'].astype(int)
    county.reset_index(inplace=True, drop=True)
    county.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','incomebycounty2.csv'), index=False)

    # Personal income per capita by State

    df = pd.read_csv(f"s3://rawdatagrupo07/personal_income_2011-2021bystate.csv",
    storage_options={
        "key": access_key,
        "secret": secret_access_key
    },
    )

    df.drop(index=0, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df['GeoName'] = df['GeoName'].str.replace('*', '', regex=False)
    df['GeoName'] = df['GeoName'].str.lstrip()

    state = pd.melt(df, id_vars=['GeoName','GeoFips'],value_vars=['2011', '2012', '2013', '2014', '2015', '2016','2017', '2018', '2019', '2020', '2021'],var_name='Year',value_name='Personal_Income')
    state.rename(columns={'GeoFips': 'Fips_Code', 'GeoName': 'State'}, inplace=True)

    state.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','incomebystate.csv'), index=False)


def transform_population():
    population2010 = pd.read_csv(f"s3://rawdatagrupo07/SUB-EST2020_ALL.csv",
    storage_options={
        "key": access_key,
        "secret": secret_access_key
    },encoding = "ISO-8859-1"
    )

    population2021 = pd.read_csv(f"s3://rawdatagrupo07/sub-est2021_all.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
    },engine='python',encoding='latin1'
    )

    # Dataframe population2010
    population2010['SUMLEV']= population2010['SUMLEV'].astype(str)
    population2010['STATE']= population2010['STATE'].astype(str)
    population2010['COUNTY']= population2010['COUNTY'].astype(str)
    population2010[ 'PLACE']= population2010[ 'PLACE'].astype(str)
    population2010['COUSUB']= population2010['COUSUB'].astype(str)
    population2010['CONCIT']= population2010['CONCIT'].astype(str)
    population2010['PRIMGEO_FLAG']= population2010['PRIMGEO_FLAG'].astype(str)

    #Dataframe population2021
    population2021['SUMLEV']= population2021['SUMLEV'].astype(str)
    population2021['STATE']= population2021['STATE'].astype(str)
    population2021['COUNTY']= population2021['COUNTY'].astype(str)
    population2021[ 'PLACE']= population2021[ 'PLACE'].astype(str)
    population2021['COUSUB']= population2021['COUSUB'].astype(str)
    population2021['CONCIT']= population2021['CONCIT'].astype(str)
    population2021['PRIMGEO_FLAG']= population2021['PRIMGEO_FLAG'].astype(str)

    population2010['Indicator']= population2010[['SUMLEV','STATE','COUNTY','PLACE','COUSUB','CONCIT','PRIMGEO_FLAG','FUNCSTAT']].apply(''.join, axis = 1)
    population2021['Indicator']= population2021[['SUMLEV','STATE','COUNTY','PLACE','COUSUB','CONCIT','PRIMGEO_FLAG','FUNCSTAT']].apply(''.join, axis = 1)

    population= pd.merge(population2010,population2021, how = 'inner', left_on='Indicator', right_on='Indicator')
    population = population[['NAME_x', 'STNAME_x', 'POPESTIMATE2010', 'POPESTIMATE2011',
       'POPESTIMATE2012', 'POPESTIMATE2013', 'POPESTIMATE2014',
       'POPESTIMATE2015', 'POPESTIMATE2016', 'POPESTIMATE2017',
       'POPESTIMATE2018', 'POPESTIMATE2019',
       'POPESTIMATE2020_x', 'POPESTIMATE2021']]

    population['NAME_x']= population['NAME_x'].str.strip('city')
    population['NAME_x']= population['NAME_x'].str.strip()

    population['NAME_x']= population['NAME_x'].str.strip('town')
    population['NAME_x']= population['NAME_x'].str.strip()

    population = population.rename(columns= {'NAME_x':'City'})
    population = population.rename(columns= {'STNAME_x':'State'})
    population = population.rename(columns= {'POPESTIMATE2020_x':'POPESTIMATE2020'})

    population['State'] = population.State.astype(str).apply(lambda x: us_state_to_abbrev[x])
    cities = pd.read_csv(f"s3://cleandatagrupo07/cities.csv",
        storage_options={
            "key": access_key,
            "secret": secret_access_key
        }
    )

    df3 = pd.merge(cities, population, how = 'inner', on =['City', 'State'])

    df3.drop_duplicates(subset = ['Unique_City_ID'], keep= 'first')
    df3.drop(['City','County','State'],axis=1,inplace=True)
    df3.drop_duplicates(subset=['Unique_City_ID'],inplace=True)
    values = [i for i in range(2010,2022)]
    keys = [f"POPESTIMATE{i}" for i in range(2010,2022)]
    df3.rename(columns=dict(zip(keys, values)),inplace=True)
    df_melt = pd.melt(df3, id_vars =['Unique_City_ID'], value_vars = values)
    df_melt.rename(columns={'variable':'Year','value':'PopEstimate'},inplace=True)

    df_melt.to_csv(os.path.join(os.getcwd(),'datasets','clean_data','population.csv'), index=False)
