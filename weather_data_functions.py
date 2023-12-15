'''
python script to call data from various weather api's
'''

from urllib.parse import quote
import requests
import secret
import pandas as pd
from datetime import datetime

def get_current_weather(postcode):
    encoded_postcode = quote(postcode)
    url = f'http://api.weatherapi.com/v1/current.json?key={secret.weatherapi_api_key}&q={encoded_postcode}&aqi=no'
    response = requests.get(url)
    weather_data = response.json()
    data = weather_data['current']

    # Extracting condition data
    condition_data = data.pop('condition')

    # Creating a DataFrame
    df = pd.DataFrame([data])
    # Extracting 'text' from the 'condition' dictionary and renaming the column
    df['condition'] = condition_data['text']
    # Setting 'last_updated' as the index
    df.set_index('last_updated', inplace=True)

    return df

# function to get a days worth of historical data
def get_historical_weather(postcode, date_time):
    encoded_postcode = quote(postcode)
    date_string = date_time.strftime('%Y-%m-%d')

    url = f'http://api.weatherapi.com/v1/history.json?key={secret.weatherapi_api_key}&q={encoded_postcode}&dt={date_string}'
    response = requests.get(url)
    weather_data = response.json()

    # create dataframe to store all the weather data
    weather_df = pd.DataFrame()
    # loop through each hour of the day
    for hour in weather_data['forecast']['forecastday'][0]['hour']:
        # take out condition data
        condition_data = hour.pop('condition')
        # put into dataframe
        df = pd.DataFrame([hour])
        # Extracting 'text' from the 'condition' dictionary and renaming the column
        df['condition'] = condition_data['text']
        # Setting 'last_updated' as the index
        df.set_index('time', inplace=True)
        # append to weather_df
        weather_df = pd.concat([weather_df, df])
    return weather_df