import unittest
from requester import url_to_csv, batch_url_to_csv, url_to_df
import numpy as np
import pandas as pd
import requests
import csv
import os
import warnings
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

# def valid_url_csv(urls):
#     valid_urls = []
#     for elem in urls:
#         r = requests.get(elem)
#         content_type = r.headers['Content-type'].split('/')[1][:4]
#         if r.status_code >= 400 or r.text == '404 File Not Found' or content_type == 'html' :
#             continue
#         valid_urls += [elem]
#     return valid_urls


class TestRequester(unittest.TestCase):

    def test_url_to_csv_fails_csv_parse(self):
        url='http://earthquake.usgs.gov/'
        # url='http://www.yahoo.com'
        with self.assertRaises(TypeError):
            url_to_csv(url, 'tmp_inval_csv.csv')


    def test_url_to_csv_invalid_url(self):
        url='http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv'
        with self.assertRaises(ValueError):
            url_to_csv(url, 'tmp_inval_url.csv')


    # def test_batch_runtime_warn(self):
    #     urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
    #             'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
    #             'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
    #     fnames = ['usgs10.csv', 'usgs12.cvs', 'usgs25.csv']
    #     with warnings.catch_warnings(record=True) as warn:
    #         warnings.simplefilter("always")
    #
    #         batch_url_to_csv(urls, fnames)


    def test_batch_generate(self):
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://www.yahoo.com',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12_gen.csv', 'usgs10_gen.cvs', 'yahoo_gen.csv', 'usgs25_gen.csv']
        generated_f = batch_url_to_csv(urls, fnames)
        truth_generated = []
        for f in fnames:
            f_path = os.path.join(os.path.dirname(__file__), f)
            if os.path.exists(f_path):
                truth_generated += [f_path]
        self.assertListEqual(generated_f, truth_generated)


    def test_batch_diff(self):
        urls = ['https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv',
                'https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data']
        fnames = ['car_d.csv', 'usgs_d.cvs', 'cencus_d.csv']
        batch_files = batch_url_to_csv(urls, fnames)
        csv_snip = []
        for f in batch_files:
            with open(f) as csv_f:
                reader = csv.reader(csv_f)
                csv_snip += [list(reader)[:2]]
        for i in range(len(csv_snip)):
            for j in range(len(csv_snip)):
                if j == i:
                    continue
                self.assertNotEquals(csv_snip[i], csv_snip[j])


    def test_batch_valid_filenames(self):
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
               'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
               'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12_valid.csv', 'usgs10_valid.csv','usgs25_valid.csv']
        truth_fnames = ['usgs10_valid.csv','usgs25_valid.csv']
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
        fnames = ['usgs12_num.csv', 'usgs10_num.cvs', 'usgs25_num.csv']
        n_valid_f = batch_url_to_csv(urls, fnames)
        # n_valid_urls = valid_url_csv(urls) #len(n_valid_urls)
        self.assertEqual(len(n_valid_f), 2)


    def test_batch_duplicate_urls(self):
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12_dup.csv', 'usgs10_dup.csv', 'usgs25_dup.csv']
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
            decoded_f = download.content.decode('utf-8')
            rows_lst = list(csv.reader(decoded_f.splitlines(), delimiter=','))
            csv_rows = len(rows_lst)
        self.assertEqual(df_rows, csv_rows)


if __name__ == '__main__':
    unittest.main()