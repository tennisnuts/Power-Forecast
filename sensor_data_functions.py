'''
python script to define functions for data analysis
'''

# install libraries
import influxdb_client
import pandas as pd
import secret
import math
import pytz

def get_influx_data(date_time):

    # determine the datetimes to query between
    start_dt = date_time
    end_dt = start_dt + pd.Timedelta(days=1)

    # work out the start and end lookback times from datetime
    start_lookback = math.floor((pd.Timestamp.now() - start_dt).total_seconds() / 60 + 1)
    end_lookback = math.ceil((pd.Timestamp.now() - end_dt).total_seconds() / 60 - 1)

    bucket = "random"
    org = "Empati Limited"
    token = secret.influx_api_key
    # Store the URL of your InfluxDB instance
    url="https://eu-central-1-1.aws.cloud2.influxdata.com/"

    # instantiate the client
    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    # instantiate the query client
    query_api = client.query_api()

    # query written in InfluxQL
    query = f'from(bucket:"random")\
    |> range(start: -{start_lookback}m, stop: -{end_lookback}m)\
    |> filter(fn:(r) => r._measurement == "Power")\
    |> filter(fn:(r) => r._field == "value")'

    # submit the query
    result = query_api.query(org=org, query=query)

    # sort through the query and filter for OLD and NEW
    old_results = []
    new_results = []
    for table in result:
        for record in table.records:
            if record.values['appliance'] == 'OLD':
                old_results.append((record.values['_time'], record.values['_value']))
            else:
                new_results.append((record.values['_time'], record.values['_value']))

    # put the results into a dataframe and clean up
    old_df = pd.DataFrame(old_results, columns=['time', 'power'])
    new_df = pd.DataFrame(new_results, columns=['time', 'power'])

    old_df['datetime'] = pd.to_datetime(old_df['time'])
    old_df['power'] = pd.to_numeric(old_df['power'])
    old_df = old_df.set_index('datetime')
    old_df = old_df.drop(columns=['time'])

    new_df['datetime'] = pd.to_datetime(new_df['time'])
    new_df['power'] = pd.to_numeric(new_df['power'])
    new_df = new_df.set_index('datetime')
    new_df = new_df.drop(columns=['time'])

    # make the start and end datetimes timezone aware
    start_dt = pytz.utc.localize(start_dt)
    end_dt = pytz.utc.localize(end_dt)
    end_dt = end_dt - pd.Timedelta(seconds=1)

    # filter the dataframes to the start and end datetimes
    if len(old_df) > 0:
        old_df = old_df[start_dt:end_dt]
    if len(new_df) > 0:
        new_df = new_df[start_dt:end_dt]

    # determine the file locations for the dataframes
    consumption_location = "data/consumption/"
    generation_location = "data/generation/"

    # convert datetime to string for saving
    date_string = date_time.strftime('%Y-%m-%d')

    # save the dataframes to csv
    consumption_name = f"consumption_{date_string}.csv"
    generation_name = f"generation_{date_string}.csv"
    old_df.to_csv(generation_location + generation_name)
    new_df.to_csv(consumption_location + consumption_name)

    return old_df, new_df

def get_sensor_data(date_time):
    
    # determine the datetimes to query between
    start_dt = date_time
    end_dt = start_dt + pd.Timedelta(days=1)

    # work out the start and end lookback times from datetime
    start_lookback = math.floor((pd.Timestamp.now() - start_dt).total_seconds() / 60 + 1)
    end_lookback = math.ceil((pd.Timestamp.now() - end_dt).total_seconds() / 60 - 1)

    # determine the file locations for the dataframes
    consumption_location = "data/consumption/"
    generation_location = "data/generation/"

    # convert datetime to string for saving
    date_string = date_time.strftime('%Y-%m-%d')

    # determine the file names
    consumption_name = f"consumption_{date_string}.csv"
    generation_name = f"generation_{date_string}.csv"

    # try to load the dataframes from local storage
    try:
        consumption_df = pd.read_csv(consumption_location + consumption_name)
        generation_df = pd.read_csv(generation_location + generation_name)
        consumption_df['datetime'] = pd.to_datetime(consumption_df['datetime'])
        consumption_df = consumption_df.set_index('datetime')
        generation_df['datetime'] = pd.to_datetime(generation_df['datetime'])
        generation_df = generation_df.set_index('datetime')
        print('Data loaded from local storage')
    except:
        # if the dataframes don't exist then query influxdb
        print('Data not found in local storage, querying influxdb')
        generation_df, consumption_df = get_influx_data(date_time)

    # code to overwrite if the date is today so that we get the most up to date data
    if date_time.date() == pd.Timestamp.now().date():
        print('Date is today, querying influxdb to get up to date data')
        generation_df, consumption_df = get_influx_data(date_time)

    return generation_df, consumption_df

