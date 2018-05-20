import config

import requests
import pandas as pd
import numpy as np
import datetime as dt

from do_geocode import do_geocode

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
    
    query = "SELECT * WHERE reported_date > '"+most_recent_format+"' ORDER BY reported_date"

    params = {'$query': query}

    response = requests.get(link, headers=headers, params=params) #json data
    response_json = response.json() #json data as list of dictionaries
    
    #create and return pandas DataFrame of json response

    return pd.DataFrame(response_json)


def split_no_offense(df):
    df['counts'] = pd.to_numeric(df['counts'])
    no_offense = df[df['counts']==0]
    df = df[df['counts']>0]
    return df, no_offense

def classify_crime(crime, violent_crime=violent_crime, property_crime=property_crime):
    if crime in violent_crime:
        return 'violent_crime'
    elif crime in property_crime:
        return 'property_crime' 
    else:
        return 'other_crime'

def parse_dates(df, column='reported_date', args=("%Y-%m-%dT%H:%M:%S.%f",)):
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
        datetime_col = df[column].apply(dt.datetime.strptime, args=args)
    except  ValueError:
        pd.to_datetime(df[column])

    return datetime_col

def create_hour_minute_day(df, column='reported_date'):
    """
    Takes a pandas DataFrame column of datetime objects and appends 3 columns;
    (hour, minute, day)

    Used to make plotting these values easier.

    df: pandas DataFrame
    column: name of column in df containing datetime objects

    returns nothing, modifies given df

    Default column corresponds to the Providence crime log api formating.
    """

    df["hour"] = [i.hour for i in df[column]]
    df["minute"] = [i.minute for i in df[column]]
    df["day"] = [i.day for i in df[column]]

    #ensure each column is fully numeric
    for col in ['counts', 'month', 'year']:
        df[col] = pd.to_numeric(df[col])


def clean_location(df, column='location'):
    """
    Takes a pandas DataFrame column of location strings and return them
    in a format that matches the OpenAddresses dataset and is more usable by the geolocators
    
    df: pandas DataFrame
    column: name of column in df containing address strings

    returns series object of column with formatted addresses
    use: df['column'] = clean_location(df, 'column') 

    Default column corresponds to the Providence crime log api formating.
    """

    df[column] = df[column].astype(str)

    #geocoders read X St at Y St better than X & Y or X/Y
    df[column] = df[column].str.replace("&", "at")
    df[column] = df[column].str.replace("/", "at")
    
    #OpenAddress dataset has addresses in title case
    df[column] = df[column].str.title()

    return df[column]


def create_address_df(address_file):
    """
    Creates dataframe of addresses for use with Providence crime log api location column
    from a given csv file of addresses

    address_file: path to csv containing addresses
    return dataframe with column location, lat, and lon.
    """

    addresses = pd.read_csv(address_file)

    #want to join the crime dataframe and address on location column
    #location column should be street number street address format
    addresses['location'] = addresses['NUMBER'] + ' ' + addresses['STREET']
    addresses = addresses[['location', 'LAT', 'LON']]
    
    #lowercase column names are more consitent with crime dataframe
    addresses.rename(columns = {'LON':'lon', 'LAT':'lat'}, inplace=True)

    return addresses


def get_lat_lon(df, google_key, address_file='pvd_coords.csv', create_addresses=False, city_state = 'Providence, RI', no_offense=False):

    """
    Takes a pandas dataframe and adds lat and lon columns of the coordinates for addresses
    in the location column of the dataframe. Value will be np.nan for values that could not be parse.

    First attempts to fill in all lat and lon value using the a csv of known address-lat/lon matches

    For any addresses not found it will use Nominatim as a geocoder first and when the addresses 
    cannot be found it tries to find the addresses using Google's map api. 
    Google's api is much stricter than Nominatim, which is why it is used second.

    df: dataframe
    google_key: your api key to the google maps api
    address_file: the csv file that contains addresses in a location column mapped to lat/lon columns
    create_addresses: if your address csv file does not have a location column with addresses in the format:
        street number street, this will create a df with that column and the lat/lon columns only
    
    return the dataframe with the new columns
    saves a copy of the dataframe  with the lat/lon columns as csv file with name pvd_crime_today.csv
    adds newly parse addresses to the address file and saves a copy
    """

    #create address file in correct format if not provided as such
    #on the used to decide this, would like to perform a check of format in the future
    if create_addresses:
        addresses = create_address_df(address_file)
    else:
        addresses = pd.read_csv(address_file)

    #join dataframe on location column, use left join to ensure we keep all crime data
    #while only bringing in lat/lon data related to the addresses in the crime dataframe    
    df = df.merge(addresses, how='left', on='location', left_index=True, right_index=True)

    #create a dataframe of only the null coordiante addresses
    null_locals = pd.DataFrame(df[df['lat'].isnull()]['location'].unique(), columns=['location'])
    null_locals['lat'] = np.nan
    null_locals['lon'] = np.nan
    #add city and state to addresses to increase performance with geocoders
    null_locals['location'] = null_locals['location'] +', '+ city_state

    #work through addresses
    for i, address in zip(null_locals.index, null_locals['location']):
        #try with default geocoder
        lat, lon = do_geocode(address)
        if lat != None:
            null_locals.set_value(i, 'lat', lat)
            null_locals.set_value(i, 'lon', lon)
        else:
            #if default geocode does not work try the googlemaps api
            lat, lon = do_geocode(address, 'google', google_key)
            null_locals.set_value(i, 'lat', lat)
            null_locals.set_value(i, 'lon', lon)
    
    #grab the locations we could parse and their coordinates
    no_longer_null = null_locals[null_locals['lat'].notnull()]


    #remove the city state; currently only works for ', Providence, RI'; need to fix
    no_longer_null['location'] = no_longer_null['location'].str[:-16]

    #bring those coordinate into the crime dataset using a join
    #might be a better way to do this, this way create extra columns to deal with
    df = df.merge(no_longer_null, how='left', on='location', left_index=True, right_index=True) 

    #find null coordinates from our first run using the OpenAddresses dataset
    lonx_null = df[df['lon_x'].isnull()].index
    latx_null = df[df['lat_x'].isnull()].index

    #fill them with the values from our geocode run
    df.loc[lonx_null, 'lon_x'] = df.loc[lonx_null, 'lon_y']
    df.loc[latx_null, 'lat_x'] = df.loc[latx_null, 'lat_y']

    #remove the geocode columns; their info is not in the original columns
    df.drop(['lon_y', 'lat_y'], axis=1, inplace=True)

    #rename original columns to match original names
    df.rename(columns = {'lon_x':'lon', 'lat_x':'lat'}, inplace=True)
        
    #sort by date; this could be done in different function
    #but getting the lat/lon is generally the end of my processing
    #need in reported_date order for merging with master crime log file
    df.sort_values('reported_date', ascending=False, inplace=True)
    df.reset_index(inplace=True, drop=True)
    #save csv file
    today = dt.datetime.now().strftime("%m_%d_%Y")
    if no_offense:
        filename = 'crime_log_runs/pvd_non_offense_'+today+'.csv'
    else:
        filename = 'crime_log_runs/pvd_crime_'+today+'.csv'
    df.to_csv(filename, index=False)

    #appends newly parse address and coordinates from the geolocator run
    #now we can find them without the geocoder in the next pull
    addresses = addresses.append(no_longer_null, ignore_index=True)
    
    #save csv, currently overwrites my current file
    addresses.to_csv('pvd_coords.csv', index=False)
    
    #returns dataframe with lat/lon columns
    return df

def add_to_master(df, no_offense, master_file = 'pvd_crime_master.csv', no_offense_master_file = 'non_offenses_master.csv', return_masters=False):
    master = pd.read_csv(master_file)
    
    no_offense_master = pd.read_csv(no_offense_master_file)
    master['reported_date'] = pd.to_datetime(master.reported_date)
    no_offense_master['reported_date'] = pd.to_datetime(no_offense_master.reported_date)

    today = dt.datetime.now().strftime("%m_%d_%Y")
    archive_master = 'master_archive/'+today+'_'+master_file
    archive_no_offense_master = 'master_archive/'+today+'_'+no_offense_master_file

    master.to_csv(archive_master, index=False)
    no_offense_master.to_csv(archive_no_offense_master, index=False)

    master = pd.concat([df,master])
    no_offense_master = pd.concat([no_offense,no_offense_master])
    

    master.drop_duplicates(subset=['casenumber', 'reported_date'], inplace=True)
    master.sort_values('reported_date', ascending=False, inplace=True)
    master.reset_index(inplace=True, drop=True)

    no_offense_master.drop_duplicates(subset=['casenumber', 'reported_date'], inplace=True)
    no_offense_master.sort_values('reported_date', ascending=False, inplace=True)
    no_offense_master.reset_index(inplace=True, drop=True)

    master.to_csv(master_file, index=False)
    no_offense_master.to_csv(no_offense_master_file, index=False)
    
    if return_masters:
        master['reported_date'] = pd.to_datetime(master.reported_date)
        no_offense_master['reported_date'] = pd.to_datetime(no_offense_master.reported_date)
        return master, no_offense_master



def clean_data(df):
    """
    Performs all the cleaning function on a given dataframe
    Returns dataframe with;
    reported_date column as datetime
    hour, minute, & day columns
    location column is standard format
    lat and lon columns

    df: pandas DataFrame

    saves copy of dataframe as pvd_crime_today.csv
    appends new addresses to address file 
    """
    df, no_offense = split_no_offense(df)
    df['offense_cat'] = df['offense_desc'].apply(classify_crime)
    no_offense['offense_cat'] = no_offense['offense_desc'].apply(classify_crime)
    df['reported_date'] = parse_dates(df)
    no_offense['reported_date'] = parse_dates(no_offense)
    create_hour_minute_day(df)
    create_hour_minute_day(no_offense)
    df['location'] = clean_location(df)
    no_offense['location'] = clean_location(no_offense)
    df = get_lat_lon(df, google_key=config.google_key)
    no_offense = get_lat_lon(no_offense, google_key=config.google_key, no_offense=True)
    return df, no_offense


def get_data_clean_data(link=None, key=None, master_file = None, only_return_recent=False):
    """
    Gets data from Providence crime log api and cleans data
    as outlined below.

    Returns dataframe with;
    reported_date column as datetime
    hour, minute, & day columns
    location column is standard format
    lat and lon columns

    link: link for json api data
    key: user key for api

    only_return_recent: default to return the masters, if set to true, still saves
    master copies, but only returns brand new data

    saves copy of dataframe as pvd_crime_today.csv
    appends new addresses to address file 
    """
    if link == None:
        link=config.api_link

    if key == None:
        key=config.api_key

    if only_return_recent:
        return_master = False
    
    df = create_df(link=link, key=key)
    df, no_offense = clean_data(df)
    #master, no_offense_master = add_to_master(df, no_offense, return_masters=return_master)

    if only_return_recent:
        return df, no_offense  
    else:
        return master, no_offense_master

if __name__ == "__main__":
    get_data_clean_data(link = config.api_link, key=config.api_key)

