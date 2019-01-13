import pandas as pd
import os
import json
import numpy as np
from tqdm import tqdm
from pandas.io.json import json_normalize
import multiprocessing as mp
from multiprocessing import Lock, Process
import time


def pull_out_jams(row):
    try:
        row_data_as_json = json.loads(row)
        return {'jams':row_data_as_json['jams'], 'startTime':row_data_as_json['startTime'], 'endTime':row_data_as_json['endTime']}
    except (ValueError, KeyError):
        return None
vectorized_pull_out_jams = np.vectorize(pull_out_jams)

dump_iterator = pd.read_csv('./errors.csv', chunksize=1, header=None, names=['id', 'datetime', 'data'])
for chunk in tqdm(dump_iterator):
    try:
        chunk['data'] = vectorized_pull_out_jams(chunk['data'])
    except (KeyError, ValueError, AttributeError):
        print('oops')
        chunk.to_csv('./still_error.csv', header=None, mode='a')
        continue

    try:
        chunk = json_normalize(chunk['data'].tolist())
    except (KeyError, AttributeError):
        print('oops2')
        chunk.to_csv('./still_error.csv', header=None, mode='a')
        continue

    chunk = pd.DataFrame({
          col:np.repeat(chunk[col].values, chunk['jams'].str.len())
          for col in chunk.columns.drop('jams')}
        ).assign(**{'jams':np.concatenate(chunk['jams'].values)})[chunk.columns]

    chunk = pd.concat([chunk.drop('jams', axis=1), pd.DataFrame(chunk['jams'].tolist())[['level', 'line']]], axis=1)

    chunk = pd.DataFrame({
          col:np.repeat(chunk[col].values, chunk['line'].str.len())
          for col in chunk.columns.drop('line')}
        ).assign(**{'line':np.concatenate(chunk['line'].values)})[chunk.columns]

    chunk = pd.concat([chunk.drop('line', axis=1), pd.DataFrame(chunk['line'].tolist())], axis=1)
    chunk.to_csv('./processed_errors.csv', header=None, mode='a')
