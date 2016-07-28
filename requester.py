import numpy as np
import pandas as pd
import os
import requests
import csv


def get_path():
    csv_path = os.path.join(os.path.dirname(__file__))
    return csv_path


def url_to_csv(url, fname='tmp.csv'):

    if fname[-4:] != '.csv':
        fname = "{}.csv".format(fname.split('.')[0])

    r = requests.get(url)
    if r.status_code >= 400 or r.text == '404 File Not Found':
        raise ValueError
        return

    # print "hi"
    try:
        # print "try"
        csv_df = pd.read_csv(url, header=None)
        # print "1"
        result = csv_df.to_csv(fname)
        # print "2"
        # csv.reader(result.splitlines(), delimiter=',')
        return result
    except Exception:
        # print "bye"
        raise TypeError
        return

# url='http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv'
# url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data'
# url = 'http://www.yahoo.com'
# url_to_csv(url, 'geo.csv')


def batch_url_to_csv(urls, fnames):

    http = urllib3.PoolManager()
    for i in range(len(urls)):
        response = http.request('GET', urls[i])
        with open(fnames[i], 'wb') as f:
            f.write(response.data)
        response.release_conn()

    lst_filenames = []
    for i in range(len(fnames)):
        lst_filenames += [os.path.join(get_path(), fnames[i])]
        # print os.path.exists(lst_filenames[i])
    return lst_filenames

urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
        'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
fnames = ['1week.csv', '2week.csv']
# print batch_url_to_csv(urls, fnames)


def url_to_df(url, header=None):

    #url to csv
    http = urllib3.PoolManager()
    fname = os.path.join(get_path(), 'tmp.csv')
    response = http.request('GET', url)
    with open(fname, 'wb') as f:
        f.write(response.data)
    response.release_conn()

    #read csv to DataFrame
    df = pd.read_csv(fname, sep=',', header=header)
    return df

# print url_to_df('http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv')

