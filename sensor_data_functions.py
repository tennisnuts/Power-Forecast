'''
python script to define functions for data analysis
'''

# install libraries
import influxdb_client
import pandas as pd
import secret

def get_sensor_data(lookback):
    # variables
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
    |> range(start: -{lookback})\
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

    return old_df, new_df

