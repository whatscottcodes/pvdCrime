import config

import requests
import pandas as pd
import numpy as np
import datetime as dt

from do_geocode import do_geocode


headers = {'Authentication': config.api_key}
params = {"$limit": 13000, "$offset": 0}

response = requests.get("https://data.providenceri.gov/resource/gfyp-tfg9.json", headers=headers, params=params)

response_json = response.json()

pvd_crimes =  pd.DataFrame(response_json)

today = dt.datetime.now().strftime("%m_%d_%Y")
filename = 'PVD_Crimes_'+today+'.csv'
pvd_crimes_loc.to_csv(filename)

pvd_crimes["reported_date"] = pvd_crimes["reported_date"].apply(dt.datetime.strptime, args=("%Y-%m-%dT%H:%M:%S.%f",))

pvd_crimes["hour"] = [i.hour for i in pvd_crimes['reported_date']]
pvd_crimes["minute"] = [i.minute for i in pvd_crimes['reported_date']]
pvd_crimes["day"] = [i.day for i in pvd_crimes['reported_date']]

for col in ['counts', 'month', 'year']:
    pvd_crimes[col] = pd.to_numeric(pvd_crimes[col])

pvd_crimes['location'] = pvd_crimes["location"].astype(str)
pvd_crimes['location'] = pvd_crimes['location'].str.replace("&", "at")
pvd_crimes['location'] = pvd_crimes['location'].str.replace("/", "at")
pvd_crimes['location'] = pvd_crimes['location'].str.title()

addresses = pd.read_csv('ri/providence.csv')
addresses['location'] = addresses['NUMBER'] + ' ' + addresses['STREET']
addresses = addresses[['location', 'LON', 'LAT']]

pvd_crimes_loc = pvd_crimes.merge(addresses, how='left', on='location')

pvd_crimes_loc.rename(columns = {'LON':'lon', 'LAT':'lat'}, inplace=True)

null_locals = pd.DataFrame(pvd_crimes_loc[pvd_crimes_loc['lat'].isnull()]['location'].unique(), columns=['location'])
null_locals['lon'] = np.nan
null_locals['lat'] = np.nan

null_locals['location'] = null_locals['location'] + ', Providence, RI'


for i, address in enumerate(null_locals['location']):
    lat, lon = do_geocode(address)
    if lat != None:
        null_locals.set_value(i, 'lat', lat)
        null_locals.set_value(i, 'lon', lon)
    else:
        lat, lon = do_geocode(address, 'google', key)
        null_locals.set_value(i, 'lat', lat)
        null_locals.set_value(i, 'lon', lon)
        
no_longer_null = null_locals[null_locals['lat'].notnull()]

no_longer_null['location'] = no_longer_null['location'].str[:-16]

pvd_crimes_loc = pvd_crimes_loc.merge(no_longer_null, how='left', on='location') 

lonx_null = pvd_crimes_loc[pvd_crimes_loc['lon_x'].isnull()].index
latx_null = pvd_crimes_loc[pvd_crimes_loc['lat_x'].isnull()].index

pvd_crimes_loc.loc[lonx_null, 'lon_x'] = pvd_crimes_loc.loc[lonx_null, 'lon_y']
pvd_crimes_loc.loc[latx_null, 'lat_x'] = pvd_crimes_loc.loc[latx_null, 'lat_y']

pvd_crimes_loc.drop(['lon_y', 'lat_y'], axis=1, inplace=True)

pvd_crimes_loc.rename(columns = {'lon_x':'lon', 'lat_x':'lat'}, inplace=True)

today = dt.datetime.now().strftime("%m_%d_%Y")
filename = 'PVD_Crimes_'+today+'.csv'
pvd_crimes_loc.to_csv(filename, index=False)

addresses = addresses.append(no_longer_null, ignore_index=True)

addresses.to_csv('pvd_addresses.csv', index=False)

