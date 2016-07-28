import numpy as np
import pandas as pd
import os
import requests
import csv
import warnings
from collections import Counter

def get_path():
    csv_path = os.path.dirname(__file__)
    return csv_path


def invalid_url(url):
    """Function takes one parameter, url, and validates if url exists and is accessible."""
    r = requests.get(url)
    if r.status_code >= 400 or r.text == '404 File Not Found':
        raise ValueError
        return


def invalid_csv(url):
    """Function takes one parameter, url, and checks if its contents contains valid CSV files and
    not HTML."""
    r = requests.get(url)
    if r.headers['Content-type'].split('/')[1][:4] == 'html':
        raise TypeError
        return


def url_to_csv(url, fname='tmp.csv'):

    if fname[-4:] != '.csv':
        fname = "{}.csv".format(fname.split('.')[0])

    invalid_url(url)
    invalid_csv(url)

    try:
        csv_df = pd.read_csv(url, header=None)
        result = csv_df.to_csv(fname)
    except TypeError as type_e:
        raise type_e
    # except Exception:
    #     raise TypeError
    return

# url='http://earthquake.usgs.gov/'
# url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv'
# url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data'
# url = 'http://www.yahoo.com'
# url_to_csv(url, 'geo.csv')


def batch_url_to_csv(urls, fnames):

    url_count = Counter(urls)
    for url, count in url_count.items():
        if count > 1:
            raise AssertionError('Duplicate URLs cannot be present in the parameter "urls"')

    for i in range(len(fnames)):
        if fnames[i][-4:] != '.csv':
            fnames[i] = "{}.csv".format(fnames[i].split('.')[0])

    for i in range(len(urls)):
        r = requests.get(urls[i])
        url_warn = 'Inaccessible URL: %s' % urls[i]
        csv_warn = 'Cannot be parsed as CSV: %s' % urls[i]
        if r.status_code >= 400 or r.text == '404 File Not Found':
            warnings.warn(url_warn, RuntimeWarning)
            continue

        url_content = r.headers['Content-type'].split('/')[1][:4]
        if url_content == 'html':
            warnings.warn(csv_warn, RuntimeWarning)
            continue

        try:
            csv_df = pd.read_csv(urls[i], header=None)
            csv_df.to_csv(fnames[i])
        except warnings.warn(csv_warn, RuntimeWarning):
            continue

    #file names of existing CSV
    lst_filenames = []
    for i in range(len(fnames)):
        f_path = os.path.join(get_path(), fnames[i])
        if os.path.exists(f_path):
            lst_filenames += [f_path]
    return lst_filenames

# urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
#         'http://www.yahoo.com',
#         'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
# fnames = ['1week.csv', 'yahoo.cvs', '2week.csv']
# print batch_url_to_csv(urls, fnames)


def url_to_df(url, header=None):

    invalid_url(url)
    invalid_csv(url)
    #read csv to DataFrame
    df = pd.read_csv(url, sep=',', header=header)
    return df

# print url_to_df('http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv')
# print url_to_df('https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data')



# import warnings
#
# def fxn():
#     warnings.warn("deprecated", DeprecationWarning)
#
# with warnings.catch_warnings(record=True) as w:
#     # Cause all warnings to always be triggered.
#     warnings.simplefilter("always")
#     # Trigger a warning.
#     fxn()
#     # Verify some things
#     assert len(w) == 1
#     assert issubclass(w[-1].category, DeprecationWarning)
#     assert "deprecated" in str(w[-1].message)
#     print str(w)


# url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data'
# df_rows = url_to_df(url).shape[0]
# with requests.Session() as s:
#     download = s.get(url)
#     decoded_content = download.content.decode('utf-8')
#     cr = csv.reader(decoded_content.splitlines(), delimiter=',')
#     my_list = list(cr)
#     print my_list[0]
#     csv_rows = len(my_list)

