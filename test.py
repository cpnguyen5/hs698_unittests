import unittest
from requester import url_to_csv, batch_url_to_csv, url_to_df
import numpy as np
from fun_things import add
from numpy.testing import assert_array_almost_equal
import urllib3

class TestFunThings(unittest.TestCase):

    def test_add(self):
        res = add(3, 4)
        self.assertEqual(res, 7)


    def test_add_fails_when_input_is_string(self):
        with self.assertRaises(TypeError):
            add(3, 'hello')


    def test_numpy_array_almost_equal(self):
        # arr1=np.array([0.0,0.1,0.15])
        # arr2=np.array([0.0,0.09,0.15])
        arr1 = np.array([0.0, 0.10000000, 0.15])
        arr2 = np.array([0.0, 0.10000089, 0.15])
        assert_array_almost_equal(arr1, arr2, decimal=6)


class TestRequester(unittest.TestCase):

    def test_url_to_csv_fails_csv_parse(self):
        url='http://earthquake.usgs.gov/'
        # url='http://www.yahoo.com'
        with self.assertRaises(TypeError, msg="URL cannot be parsed as CSV"):
            url_to_csv(url, 'tmp.csv')

    def test_url_to_csv_invalid_url(self):
        url='http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv'
        with self.assertRaises(ValueError):
            url_to_csv(url, 'tmp.csv')

