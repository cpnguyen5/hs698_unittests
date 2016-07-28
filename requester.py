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

    for i in range(len(fnames)):
        if fnames[i][-4:] != '.csv':
            fnames[i] = "{}.csv".format(fnames[i].split('.')[0])

    for i in range(len(urls)):
        r = requests.get(urls[i])
        # if r.status_code >= 400 or r.text == '404 File Not Found':
        #     raise ValueError
        #     return

        csv_df = pd.read_csv(urls[i], header=None)
        csv_df.to_csv(fnames[i])

    #file names of existing CSV
    lst_filenames = []
    for i in range(len(fnames)):
        f_path = os.path.join(get_path(), fnames[i])
        if os.path.exists(f_path):
            lst_filenames += [f_path]
    return lst_filenames

# urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
#         'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
# fnames = ['1week.csv', '2week.csv']
# print batch_url_to_csv(urls, fnames)


def url_to_df(url, header=None):
    # r = requests.get(url)
    # if r.status_code >= 400 or r.text == '404 File Not Found':
    #     raise ValueError
    #     return

    #read csv to DataFrame
    df = pd.read_csv(url, sep=',', header=header)
    return df

# print url_to_df('http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv')

