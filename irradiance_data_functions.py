'''
python script to call data from the weatherapi api and from the locally saved data
'''

# api key 
import secret

# import libraries
import pandas as pd
from datetime import datetime
import requests
import ast

'''
A function to get the irradiance data from local file if it exists, otherwise get it from the api and save it
'''
def api_irradiance_data(postcode, date_time, interval='15m'):
    # get the lat long from the postcode
    lat, lon = get_lat_long(postcode)
    # get the data from the API
    date_string = date_time.strftime('%Y-%m-%d')
    url = f"https://api.openweathermap.org/energy/1.0/solar/interval_data?lat={lat}&lon={lon}&date={date_string}&interval={interval}&appid={secret.openweathermap_api_key}"
    response = requests.get(url)
    irradiance_data = response.json()

    # put the data into a dataframe
    irradiance_df = pd.DataFrame(irradiance_data['irradiance']['intervals'])
    irradiance_df['start_time'] = pd.to_datetime(date_string + ' ' + irradiance_df['start'])

    # convert the dictionary items to columns
    # Convert string representations to dictionaries (this is not needed when using the api)
    # irradiance_df['clear_sky'] = irradiance_df['clear_sky'].apply(ast.literal_eval)
    # irradiance_df['cloudy_sky'] = irradiance_df['cloudy_sky'].apply(ast.literal_eval)

    # get the ghi, dni and dhi data from irradiance
    irradiance_df[['clear_sky_ghi', 'clear_sky_dni', 'clear_sky_dhi']] = irradiance_df['clear_sky'].apply(lambda x: pd.Series([x['ghi'], x['dni'], x['dhi']]))
    irradiance_df[['cloudy_sky_ghi', 'cloudy_sky_dni', 'cloudy_sky_dhi']] = irradiance_df['cloudy_sky'].apply(lambda x: pd.Series([x['ghi'], x['dni'], x['dhi']]))

    # drop the columns we don't need
    irradiance_df = irradiance_df.drop(columns=['clear_sky', 'cloudy_sky'])

    # save the data to a csv file for future use (so we don't have to keep calling the api)
    location = "data/irradiance/"
    name = f"irradiance_{postcode}_{date_string}.csv"
    irradiance_df.to_csv(location + name)

    return irradiance_df

'''
A function to see if the data is available locally, if not get it from the api
'''
def irradiance_data(postcode, date_time, interval='15m'):
    lat, lon = get_lat_long(postcode)
    date_string = date_time.strftime('%Y-%m-%d')
    location = "data/irradiance/"
    name = f"irradiance_{postcode}_{date_string}.csv"
    try:
        irradiance_df = pd.read_csv(location + name, index_col=0)
        irradiance_df['start_time'] = pd.to_datetime(irradiance_df['start_time'])
        print('Locally available data used')
        return irradiance_df
    except:
        print('No local data available, calling API')
        return api_irradiance_data(postcode, date_time, interval)
    
'''
function to get the lat long from the postcode
'''
def get_lat_long(postcode):
    url = f'https://api.postcodes.io/postcodes/{postcode}'
    response = requests.get(url)
    postcode_data = response.json()
    lat = postcode_data['result']['latitude']
    lon = postcode_data['result']['longitude']
    return lat, lon