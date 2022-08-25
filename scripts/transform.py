from data_reader import DataReader
import pandas as pd
import numpy as np
import requests
import json
import sys, getopt

class DataTransform:
    def __init__(self):
        self.d = DataReader()

    def data_transform_crop(self):
        #data for year 2021
        df = self.d.read_file('crop.csv')
        df = df[df['year']==2021]
        df = df.drop(columns=['year'])
        return df

    def data_transform_spectral(self):
        df = self.d.read_file('spectral.csv')
        df['year'] = pd.DatetimeIndex(df['date']).year
        df = df[df['year']==2021]
        # check for null / blank values
        if df['nir'].isnull().sum()==0  and df['red'].isnull().sum()==0:
            df['NDVI'] = (df['nir'] - df['red']) / (df['nir'] + df['red'])
            # df = df.groupby(['tile_id', 'tile_geometry', 'NDVI', 'year'])
            # df2 = df.agg(pos_date=('NDVI', np.max))
            df2 = df[df.groupby(['tile_id', 'tile_geometry', 'year']).date.transform('max') == df['date']]
            df2 = df2.drop(columns=['nir', 'red', 'year'])
            df2 = df2.rename(columns={'date': 'pos_date', 'NDVI': 'pos'})
        else:
            df['nir'] = df['nir'].fillna(0)
            df['red'] = df['red'].fillna(0)

        return df2

    def data_transform_soil(self):
        df = self.d.read_file('soil.csv')
        df['h_weight'] = np.abs(df['hzdept'] - df['hzdepb']) / df['hzdepb']
        df['weighted_avg'] = (df['h_weight'] * df['comppct']) / 100
        df =df[['mukey', 'mukey_geometry', 'om', 'cec', 'ph', 'weighted_avg']]
        return df

    def data_transform_weather(self):
        dfw = self.d.read_file('weather.csv')
        dfw = dfw [dfw['year']==2021]
        df = self.d.read_file('crop.csv')

        df['State'] = ''
        df['County'] = ''
        df['lat_lon_pre'] = df['field_geometry'].str.split('(', 3).str[3]
        df['lat_lon'] = df['lat_lon_pre'].str.split(',', 3).str[0]
        
        for index, row in df.iterrows():
            #print(row['lat_lon'])
            state, county = self.get_api_data(row['lat_lon'])
            df.at[index,'State'] = state
            df.at[index,'County'] = county

        dfw1 = dfw.groupby(['fips_code'])['precip'].sum().reset_index(name ='sum_precip')
        dfw2 = dfw.groupby(['fips_code'])['temp'].min().reset_index(name ='min_temp')
        dfw2 = dfw2.drop(columns=['fips_code'])
        dfw3 = dfw.groupby(['fips_code'])['temp'].max().reset_index(name ='max_temp')
        dfw3 = dfw3.drop(columns=['fips_code'])
        dfw4 = dfw.groupby(['fips_code'])['temp'].mean().reset_index(name ='mean_temp')
        dfw4 = dfw4.drop(columns=['fips_code'])
        dfw = pd.concat([dfw1, dfw2, dfw3, dfw4], axis=1)
        dfw['state'] = dfw['fips_code'].apply(str)
        dfw['state'] = dfw['state'].str[:2]

        dfw['county'] = dfw['fips_code'].apply(str)
        dfw['county'] = dfw['county'].str[2:]

        dfw = pd.merge(dfw, df, left_on=['state', 'county'], right_on= ['State', 'County'])

        return dfw
        

    def get_api_data(self, lat_lon):
        lat, lon = lat_lon.split(' ')[0], lat_lon.split(' ')[1]
        url = 'https://geo.fcc.gov/api/census/block/find?latitude=%s&longitude=%s&format=json' % (lat, lon)
        try:
            response = requests.get(url)
            data = response.json()
            print(data)
            state = data['State']['FIPS']
            county = data['County']['FIPS'][2:]
        except:
            return '', ''
        return state, county



def main(argv):

    if argv[0] == 'crop':
        dt = DataTransform()
        df = dt.data_transform_crop()
        df.to_csv('outputs/crop_2021.csv', index = False)

    if argv[0] == 'spectral':
        dt = DataTransform()
        df = dt.data_transform_spectral()
        df.to_csv('outputs/spectral_2021.csv', index = False)

    if argv[0] == 'soil':
        dt = DataTransform()
        df = dt.data_transform_soil()
        df.to_csv('outputs/soil_output.csv', index = False)

    if argv[0] == 'weather':
        dt = DataTransform()
        df = dt.data_transform_weather()
        df.to_csv('outputs/weather_output.csv', index = False)

    print("Files are saved in output folder")

if __name__ == "__main__":
   main(sys.argv[1:])
