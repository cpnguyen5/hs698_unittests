import unittest
from requester import url_to_csv, batch_url_to_csv, url_to_df
import pandas as pd
import requests
import csv
import os
import warnings


class TestRequester(unittest.TestCase):

    def test_url_to_csv_fails_csv_parse(self):
        """Raise TypeError if URL cannot be parsed as CSV in function
            url_to_csv()."""
        url='http://earthquake.usgs.gov/'
        with self.assertRaises(TypeError):
            url_to_csv(url, 'tmp_inval_csv.csv')


    def test_url_to_csv_invalid_url(self):
        """Raise ValueError if URL is invalid in function
            url_to_csv()."""
        url='http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv'
        with self.assertRaises(ValueError):
            url_to_csv(url, 'tmp_inval_url.csv')


    def test_batch_runtime_warn(self):
        """Emits RuntimeWarning for invalid URL or if URL cannot be parsed as CSV in
            function batch_url_to_csv()."""
        urls = ['https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data',
                'https://archive.ics.uci.edu/ml/machine-learning-databases/housig/housing.data']
        fnames = ['cars', 'house']
        with warnings.catch_warnings(record=True) as warn:
            warnings.simplefilter("always")
            batch_url_to_csv(urls, fnames)
            assert len(warn) == 1
            assert issubclass(warn[-1].category, RuntimeWarning) #confirms RuntimeWarning
            assert "Invalid URL" in str(warn[-1].message) #confirms message


    def test_batch_generate(self):
        """Ensures batch_url_to_csv() generates same number of files as
            valid CSV URLs."""
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://www.yahoo.com',
                'https://archive.ics.uci.edu/ml/machine-learning-databases/housing/housing.data']
        fnames = ['usgs12_gen.csv', 'usgs10_gen.csv', 'yahoo_gen.csv', 'house_gen.csv']
        fnames_true = ['usgs10_gen.csv', 'house_gen.csv']
        generated_f = batch_url_to_csv(urls, fnames) # generated files from function
        truth_generated = [] # expected generated files
        for f in fnames_true:
            f_path = os.path.join(os.path.dirname(__file__), f)
            truth_generated += [f_path]
        self.assertListEqual(generated_f, truth_generated)


    def test_batch_diff(self):
        """Ensures batch_url_to_csv() outputs files with different contents."""
        urls = ['https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv',
                'https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data']
        fnames = ['car_d.csv', 'usgs_d.cvs', 'cencus_d.csv']
        batch_files = batch_url_to_csv(urls, fnames)

        csv_snip = []
        for f in batch_files:
            with open(f) as csv_f:
                reader = csv.reader(csv_f)
                csv_snip += [list(reader)[:2]] #compare snippet/first 2 rows
        for i in range(len(csv_snip)):
            for j in range(len(csv_snip)):
                if j == i: # file does not compare with itself
                    continue
                self.assertNotEquals(csv_snip[i], csv_snip[j])


    def test_batch_valid_filenames(self):
        """Ensures batch_url_to_csv() returns the correct filenames of valid URLs."""
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
               'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
               'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12_valid.csv', 'usgs10_valid.csv','usgs25_valid.csv']
        truth_fnames = ['usgs10_valid.csv','usgs25_valid.csv']
        valid_filenames = batch_url_to_csv(urls, fnames) # actual valid filenames
        # expected valid filenames
        truth_filenames = []
        for f in truth_fnames:
            truth_filenames += [os.path.join(os.path.dirname(__file__), f)]
        self.assertListEqual(valid_filenames, truth_filenames)


    def test_batch_num_valid_filenames(self):
        """Ensures batch_url_to_csv() returns the correct number of filenames of valid URLs."""
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.2_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12_num.csv', 'usgs10_num.cvs', 'usgs25_num.csv']
        n_valid_f = batch_url_to_csv(urls, fnames)
        self.assertEqual(len(n_valid_f), 2)


    def test_batch_duplicate_urls(self):
        """Raises AssertionError if duplicate URLs are passed to batch_url_to_csv()."""
        urls = ['http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv',
                'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv']
        fnames = ['usgs12_dup.csv', 'usgs10_dup.csv', 'usgs25_dup.csv']
        with self.assertRaises(AssertionError):
            batch_url_to_csv(urls, fnames)


    def test_url_to_df_DataFrame_obj(self):
        """Ensures url_to_df() returns a Pandas DataFrame object."""
        url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_week.csv'
        url_type = type(url_to_df(url))
        self.assertEqual(url_type, type(pd.DataFrame([1, 1])))


    def test_url_to_df_DataFrame_rows(self):
        """Ensures url_to_df() returns the correct number of rows, equaling the number of rows in
            the CSV file when there is no header row in the CSV file."""
        url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/car/car.data'
        df_rows = url_to_df(url, header=None).shape[0] # num of rows of DataFrame
        # num of rows of CSV
        with requests.Session() as s:
            download = s.get(url)
            decoded_f = download.content.decode('utf-8')
            rows_lst = list(csv.reader(decoded_f.splitlines(), delimiter=','))
            csv_rows = len(rows_lst)
        self.assertEqual(df_rows, csv_rows)


if __name__ == '__main__':
    unittest.main()