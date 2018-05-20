import config
import requests
import pandas as pd
import numpy as np
import datetime as dt

from do_geocode import geocode_addresses, update_address_csv

#Set of functions used to get and clean josn data from the city of Providence crime log API. 
#Could be modified to work with other data;

violent_crime = ['Assault, Aggravated', 'Murder\\Manslaughter', 'Statutory Rape', 'Assault, Threats']

property_crime = ['Larceny from Motor Vehicle', 'Vandalism', 'Larceny from Building', 'Burglary',
                  'Robbery', 'Larceny, Other', 'Larceny, Purse-snatching', 'Motor Vehicle Theft',
                  'Larceny, Shoplifting', 'Tresspassing', 'Arson']

def create_df(link=config.api_link, key=config.api_key, master_file = 'pvd_crime_master.csv'):
    """
    Retrives json data from an api and return it as a pandas DataFrame
    
    link: link for json api data
    key: user key for api
    limit: number of rows to return
    
    returns: DataFrame
    """
    #only want reports we don't already have, so what is the most recent date in the master
    master = pd.read_csv(master_file, nrows=1)
    most_recent = pd.to_datetime(master['reported_date'])[0]
    most_recent_format = most_recent.strftime('%Y-%m-%dT%H:%M:%S.000')

    headers = {'Authentication': key} #api_key
    
    query = "SELECT * WHERE reported_date > '"+most_recent_format+"' ORDER BY reported_date LIMIT 13000"

    params = {'$query': query}

    response = requests.get(link, headers=headers, params=params) #json data
    response_json = response.json() #json data as list of dictionaries
    
    #create and return pandas DataFrame of json response

    return pd.DataFrame(response_json)

def split_no_offense(df):
    df['counts'] = pd.to_numeric(df['counts'])
    no_offense = df[df['counts']==0]
    df = df[df['counts']>0]
    return no_offense

def classify_crime_helper(crime, violent_crime=violent_crime, property_crime=property_crime):
    if crime in violent_crime:
        return 'violent_crime'
    elif crime in property_crime:
        return 'property_crime' 
    else:
        return 'other_crime'

def classify_crime(df):
    classified = df.offense_desc.apply(classify_crime_helper)
    return df.assign(offense_cat=pd.Series(classified).values)

def parse_dates(df, args=("%Y-%m-%dT%H:%M:%S.%f",)):
    """
    Takes a pandas DataFrame column of date string and returns column as
    a column of datetime objects. 

    df: pandas DataFrame
    column: name of column in df containing date strings
    args = format of date string to parse

    returns series object of column with datetime objects
    use: df['column'] = parse_dates(df, 'column')

    Default column and args correspond to the Providence crime log api formating.
    """
    try:
        datetime_col = df['reported_date'].apply(dt.datetime.strptime, args=args)
    except  ValueError:
        datetime_col = pd.to_datetime(df.reported_date)

    return df.assign(reported_date=pd.Series(datetime_col).values)

def clean_location(df):
    """
    Takes a pandas DataFrame column of location strings and return them
    in a format that matches the OpenAddresses dataset and is more usable by the geolocators
    
    df: pandas DataFrame
    column: name of column in df containing address strings

    returns series object of column with formatted addresses
    use: df['column'] = clean_location(df, 'column') 

    Default column corresponds to the Providence crime log api formating.
    """
    
    local = df['location'].astype(str)
    
    #geocoders read X St at Y St better than X & Y or X/Y
    local = local.str.replace("&", "at")
    local = local.str.replace("/", "at")
    
   #OpenAddress dataset has addresses in title case
    local = local.str.title()

    return df.assign(location=local.values)

def get_lat_lon(df, google_key, addresses_provided=False):

    parsable = df[df.location.notnull()]
    addresses = df.location.values
    address_df = geocode_addresses(addresses, key=config.google_key)
    
    df = df.merge(address_df, how='left', on='location', left_index=True)

    return df

def add_to_master(df, today, master_file = 'pvd_crime_master.csv'):

    #read in masters
    master = pd.read_csv(master_file)


    archive_master = 'master_archive/'+today+'_'+master_file

    master.to_csv(archive_master, index=False)

    #merge new data and old
    master = pd.concat([df,master])
    master = parse_dates(master)

    master.sort_values('reported_date', ascending=False, inplace=True)
    master.reset_index(inplace=True, drop=True)

    master.to_csv(master_file, index=False)
    return master



def create_crime_log(link=config.api_link, key=config.api_key, google_key=config.google_key, master_file = 'pvd_crime_master.csv',return_recent_only=False, only_create_csv=False):
    #request json from api and return as pandas dataframe
    pvd_crime_log = create_df(link=link, key=key, master_file=master_file)
    
    #add column classifying the type of offense
    pvd_crime_log = classify_crime(pvd_crime_log)

    #convert report_date column from strings to pandas datetime objects
    pvd_crime_log = parse_dates(pvd_crime_log)
    
    #parse addresses in location column to google api format
    pvd_crime_log = clean_location(pvd_crime_log)
    
    #use address csv and google api to query lat/lon of reported locations
    pvd_crime_log = get_lat_lon(pvd_crime_log, google_key=google_key)

    #get todays date for crime log csv save
    today = dt.datetime.now().strftime("%m_%d_%Y")
    
    #save current run
    filename = 'crime_log_runs/'+today+'pvd_crime_log.csv'
    pvd_crime_log.to_csv(filename, index=False)
    
    #add current crime_log pull to master file of all runs
    master = add_to_master(pvd_crime_log, today)

    #if called as script do not return dataframes 
    if only_create_csv:
        print('Complete')
        return None
    #return either all crime log data available or just new data since last run
    if return_recent_only:
        return pvd_crime_log #new data
    else:
        return master #all data
    


if __name__ == "__main__":
    create_crime_log(only_create_csv=True)
    

