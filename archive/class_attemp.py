import config

import requests
import pandas as pd
import numpy as np
import datetime as dt

from do_geocode import do_geocode

class json_location_data(object):

    def __init__(self, link, key, limit):
        self.link = link
        self.key = key
        self.limit = limit

    def create_df(self):
        headers = {'Authentication': self.key}
        params = {"$limit": self.limit, "$offset": 0}

        response = requests.get(self.link, headers=headers, params=params)

        response_json = response.json()

        return pd.DataFrame(response_json)

    def parse_dates(self, df, series='reported_date', args=("%Y-%m-%dT%H:%M:%S.%f",)):
        return df[series].apply(dt.datetime.strptime, args=args)

    def create_hour_minute_day(self, data, series='reported_date'):
        data["hour"] = [i.hour for i in data[series]]
        data["minute"] = [i.minute for i in data[series]]
        data["day"] = [i.day for i in data[series]]

        for col in ['counts', 'month', 'year']:
            data[col] = pd.to_numeric(data[col])

    def clean_location(self, data, series='location'):
        data[series] = data[series].astype(str)
        data[series] = data[series].str.replace("&", "at")
        data[series] = data[series].str.replace("/", "at")
        data[series] = data[series].str.title()
        return data[series]

    def create_address_df(self, address_file):
        addresses = pd.read_csv(address_file)
        addresses['location'] = addresses['NUMBER'] + ' ' + addresses['STREET']
        addresses = addresses[['location', 'LAT', 'LON']]
        return addresses


    def get_lat_lon(self, data, address_file, create_addresses=False):
        if create_addresses:
            addresses = self.create_address_df(address_file)
        else:
            addresses = pd.read_csv(address_file)
       
        print(data.columns)
        print(addresses.columns)
        data = data.merge(addresses, how='left', on='location')
        data.rename(columns = {'LON':'lon', 'LAT':'lat'}, inplace=True)
        print(data.columns)
        null_locals = pd.DataFrame(data[data['lat'].isnull()]['location'].unique(), columns=['location'])
        null_locals['lat'] = np.nan
        null_locals['lon'] = np.nan
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

        data = data.merge(no_longer_null, how='left', on='location') 

        lonx_null = data[data['lon_x'].isnull()].index
        latx_null = data[data['lat_x'].isnull()].index

        data.loc[lonx_null, 'lon_x'] = data.loc[lonx_null, 'lon_y']
        data.loc[latx_null, 'lat_x'] = data.loc[latx_null, 'lat_y']

        data.drop(['lon_y', 'lat_y'], axis=1, inplace=True)

        data.rename(columns = {'lon_x':'lon', 'lat_x':'lat'}, inplace=True)
        
        data.sort_values('reported_date', inplace=True)

        today = dt.datetime.now().strftime("%m_%d_%Y")
        filename = 'PVD_Crimes_'+today+'.csv'
        data.to_csv(filename, index=False)

        addresses = addresses.append(no_longer_null, ignore_index=True)

        addresses.to_csv('pvd_addresses.csv', index=False)

        return data

    def do_all(self):
        df = self.create_df()
        df['reported_date'] = self.parse_dates(df)
        self.create_hour_minute_day(df)
        df['location'] = self.clean_location(df)
        df = self.get_lat_lon(df, 'ri/providence.csv', True)
        return df
