import unittest
from requester import url_to_csv, batch_url_to_csv, url_to_df
import numpy as np
import pandas as pd
import requests
import csv
import os
from fun_things import add
from numpy.testing import assert_array_almost_equal
import urllib3


# class TestFunThings(unittest.TestCase):
#
#     def test_add(self):
#         res = add(3, 4)
#         self.assertEqual(res, 7)
# #
#
#     def test_add_fails_when_input_is_string(self):
#         with self.assertRaises(TypeError):
#             add(3, 'hello')
#
#
#     def test_numpy_array_almost_equal(self):
#         # arr1=np.array([0.0,0.1,0.15])
#         # arr2=np.array([0.0,0.09,0.15])
#         arr1 = np.array([0.0, 0.10000000, 0.15])
#         arr2 = np.array([0.0, 0.10000089, 0.15])
#         assert_array_almost_equal(arr1, arr2, decimal=6)

def valid_url_csv(urls):
    valid_urls = []
    for elem in urls:
        r = requests.get(elem)
        content_type = r.headers['Content-type'].split('/')[1][:4]
        if r.status_code >= 400 or r.text == '404 File Not Found' or content_type == 'html' :
            continue
        valid_urls += [elem]
    return valid_urls


class TestRequester(unittest.TestCase):

    def test_url_to_csv_fails_csv_parse(self):
        url='http://earthquake.usgs.gov/'
        # url='http://www.yahoo.com'
        with self.assertRaises(TypeError):
            url_to_csv(url, 'tmp.csv')


    def test_url_to_csv_invalid_url(self):
        url='http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv'
        with self.assertRaises(ValueError):
            url_to_csv(url, 'tmp.csv')


    def test_batch_generate(self):
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://www.yahoo.com',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12.csv', 'usgs10.cvs', 'yahoo.csv', 'usgs25.csv']
        generated_f = batch_url_to_csv(urls, fnames)
        truth_generated = []
        for f in fnames:
            f_path = os.path.join(os.path.dirname(__file__), f)
            if os.path.exists(f_path):
                truth_generated += [f_path]
        self.assertListEqual(generated_f, truth_generated)


    def test_batch_valid_filenames(self):
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
               'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
               'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12.csv', 'usgs10.csv','usgs25.csv']
        truth_fnames = ['usgs10.csv','usgs25.csv']
        valid_filenames = batch_url_to_csv(urls, fnames)
        truth_filenames = []
        for f in truth_fnames:
            truth_filenames += [os.path.join(os.path.dirname(__file__), f)]
        self.assertListEqual(valid_filenames, truth_filenames)


    def test_batch_num_valid_filenames(self):
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        # urls1 = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
        #         'http://www.yahoo.com',
        #         'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12.csv', 'usgs10.cvs', 'usgs25.csv']
        n_valid_f = batch_url_to_csv(urls, fnames)
        # n_valid_urls = valid_url_csv(urls) #len(n_valid_urls)
        self.assertEqual(len(n_valid_f), 2)


    def test_batch_duplicate_urls(self):
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12.csv', 'usgs10.csv', 'usgs25.csv']
        with self.assertRaises(AssertionError):
            batch_url_to_csv(urls, fnames)


    def test_url_to_df_DataFrame_obj(self):
        url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv'
        url_type = type(url_to_df(url))
        self.assertEqual(url_type, type(pd.DataFrame([1, 1])))


    def test_url_to_df_DataFrame_rows(self):
        url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data'
        df_rows = url_to_df(url).shape[0]
        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            csv_rows = len(my_list)
        self.assertEqual(df_rows, csv_rows)


if __name__ == '__main__':
    unittest.main()