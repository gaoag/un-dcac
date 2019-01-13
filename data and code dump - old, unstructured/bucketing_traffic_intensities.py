import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from dask.distributed import Client
from dask.distributed import LocalCluster
import dask.dataframe as dd

cluster = LocalCluster()
client = Client(cluster)


df = dd.read_csv('./processed_errors.csv', header=None, names=['endTime', 'startTime', 'level', 'x','y'])

max_lat = 19.595892
origin_lat = 19.220471
max_lon = -98.828109
origin_lon = -99.438230

#first, we cut out all the entries in our dataframe which don't fit the desired latitude/longitude.
df = df[(df['y'] < max_lat) & (df['y'] > origin_lat) & (df['x'] < max_lon) & (df['x'] > origin_lon)]

#next, we assign each row an x_bucket or y_bucket based upon its latitude and longitude.
lon_step_size = abs(max_lon - origin_lon)/500
lat_step_size = abs(max_lat - origin_lat)/500
df['x_bucket'] = df['x'].apply(lambda x: abs(x - origin_lon)//lon_step_size)
df['y_bucket'] = df['y'].apply(lambda y: abs(y - origin_lat)//lat_step_size)

# now, we groupby bucket and sum the level
grouped = df.groupby(['x_bucket', 'y_bucket']).agg({'level':'sum'}).compute()

grouped.reset_index(inplace=True)
# we normalize values on a 0-100 scale so we have a better notion of intensity
scaler = MinMaxScaler(feature_range=(0,100))
scaler.fit(grouped['level'].values.reshape(-1, 1))
grouped['level'] = scaler.transform(grouped['level'].values.reshape(-1, 1))

grouped.to_csv('./done8.csv')

# demonstrate how we can reverse lookup
# lookup_table = pd.read_csv('./bucketed_intensity.csv')

# let's say you want to lookup an intensity for lat=19.4 lon=-99
# def lookup(lookup_table, x, y, origin_lon, origin_lat, lon_step_size, lat_step_size):
    # x_bucket = abs(x - origin_lon)//lon_step_size
#     y_bucket = abs(y - origin_lat)//lat_step_size
#
#     lookup_table
#
#
# x = -99
# y = 19.4
# x_bucket = abs(x - origin_lon)//lon_step_size
# y_bucket = abs(y - origin_lat)//lat_step_size
#
# lookup_table
