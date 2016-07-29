import pandas as pd
import os
import requests
import warnings
from collections import Counter


def get_path():
    """Function extracts and returns directory name of pathway."""
    csv_path = os.path.dirname(__file__)
    return csv_path


def invalid_url(url):
    """Function takes one parameter, the url, and validates if url exists and is accessible."""
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
    """Function takes two parameters, a URL and filename, and downloads the URL file as a CSV file."""

    #Ensures fname is CSV file type
    if fname[-4:] != '.csv':
        fname = "{}.csv".format(fname.split('.')[0])

    invalid_url(url) #validates URL
    invalid_csv(url) #validates if URL file can be parsed as CSV

    try:
        csv_df = pd.read_csv(url, header=None)
        result = csv_df.to_csv(fname)
    except TypeError as type_e:
        raise type_e
    # except Exception:
    #     raise TypeError
    return


def batch_url_to_csv(urls, fnames):
    """Function takes two parameters, a list of URLs and filenames, and downloads each URL file. Only
        valid URLs and files that can be parsed as a CSV will be saved. The function returns a list
        of filenames (file pathway)."""

    #Asserts Error if Duplicate URL
    url_count = Counter(urls)
    for url, count in url_count.items():
        if count > 1:
            raise AssertionError('Duplicate URLs cannot be present in the parameter "urls"')
    #Ensures output fnames are CSV
    for i in range(len(fnames)):
        if fnames[i][-4:] != '.csv':
            fnames[i] = "{}.csv".format(fnames[i].split('.')[0])

    #RuntimeWarning for Invalid URL or URL files that can't be parsed as CSV
    for i in range(len(urls)):
        r = requests.get(urls[i])
        url_warn = 'Invalid URL...URL skipped: %s' % urls[i]
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

    #filenames of existing/valid CSV
    lst_filenames = []
    for i in range(len(fnames)):
        f_path = os.path.join(get_path(), fnames[i])
        if os.path.exists(f_path): #ensures file exists
            lst_filenames += [f_path]
    return lst_filenames


def url_to_df(url, header=None):
    """Function takes two parameters, the URL and optional (Pandas DataFrame) header. The function
        downloads the URL and returns its contents as a Pandas DataFrame."""

    invalid_url(url) #Validates URL
    invalid_csv(url) #Validates if URL file can be parsed as CSV

    #read csv to DataFrame
    df = pd.read_csv(url, sep=',', header=header)
    return df
