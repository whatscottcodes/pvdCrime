import pandas as pd
import numpy as np
import requests

def geocode(address, key, parse_address=False, city_state=', Providence, RI', bounds='41.70,-71.65|42.0,-71.25'):
    link ='https://maps.googleapis.com/maps/api/geocode/json'
    
    if parse_address:
        addresses = address+ city_state
        address = address.replace(' ', '+')
        
    params={'address': address, 'bounds': bounds, 'key': key}
    response = requests.get(link, params=params)

    response_json=response.json()
    status = response_json['status']
    
    if status == 'ZERO_RESULTS':
        print(address)
        return np.nan, np.nan, np.nan, np.nan
    if status == 'OVER_QUERY_LIMIT':
        time.sleep(30)
    if status == 'REQUEST_DENIED':
        return 1, 1, 1, 1
    if status == 'INVALID_REQUEST':
        return 2, 2, 2, 2

    results = response_json['results'][0]
    address_components = results.get('address_components')

    for component in address_components:
        if component['types'][0] == 'neighborhood':
            neighborhood = component['long_name']
        if component['types'][0] == 'locality':
            city = component['long_name']
    
    lat = results.get('geometry').get('location').get('lat')
    lon = results.get('geometry').get('location').get('lng')

    try:
        neighborhood
    except UnboundLocalError or NameError:
        neighborhood = np.nan

    try:
        city
    except UnboundLocalError or NameError:
        city = np.nan

    return lat, lon, neighborhood, city


def geocode_addresses(addresses, key, city_state=', Providence, RI'):

    lats, lons, neighborhoods, cities = [], [], [], []
    addresses_google = [address+ city_state for address in addresses]
    addresses_google = [address.replace(' ', '+') for address in addresses_google]
    
    for address in addresses_google:
        lat, lon, hood, city = geocode(address, key=key)

        lats.append(lat)
        lons.append(lon)
        neighborhoods.append(hood)
        cities.append(city)
    
    address_dict = {'location': addresses, 'lat': lats, 'lon':lons, 'neighborhood':neighborhoods, 'city':cities}

    df = pd.DataFrame.from_dict(address_dict)
    df = df[df.lat.notnull()]
    df.reset_index(drop=True, inplace=True)

    #update_address_csv(df)

    return df

def update_address_csv(address_df, address_file='pvd_location_info.csv'):
    addresses_master = pd.read_csv(address_file)
    addresses_master = addresses_master.append(address_df, ignore_index=True)
    
    #save csv, currently overwrites my current file
    addresses.to_csv(address_file, index=False)
    
"""    
def do_geocode(address, geocoder=None, key=None):
    if geocoder == 'Google':
        geolocator = GoogleV3(api_key = key, timeout=3)
    else:
        geolocator = Nominatim(timeout=3)

    try:
        location = geolocator.geocode(address)
        if location != None:
            return location.latitude, location.longitude
        else:
            return np.nan, np.nan
            
    except GeocoderTimedOut or GeocoderQuotaExceeded:
        sleep(1)
        return do_geocode(address)"""
        
  






