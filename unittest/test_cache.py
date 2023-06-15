import os
import sys
import unittest

import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src import cache


class TestCache(unittest.TestCase):
    def setUp(self):
        self.cache = cache.Cache()
        self.input_file = 'database/'

    def test_read_parquet_data(self):
        data = self.cache.read_parquet_data(input_file=self.input_file)
        self.assertEqual(len(data), 500)
        self.assertEqual(len(data.columns), 5)

    def test_cache_trans_data(self):
        self.assertIsNone(self.cache.trans_data)
        self.cache.cache_trans_data(input_file=self.input_file)
        self.assertIsNotNone(self.cache.trans_data)
        self.assertEqual(len(self.cache.trans_data), 500)
        self.assertEqual(len(self.cache.trans_data.columns), 5)

    
    def test_net_amt_data(self):
        self.assertIsNone(self.cache.net_amt_data)
        self.cache.cache_net_amt_data(input_file=self.input_file)
        self.assertIsNotNone(self.cache.net_amt_data)
        self.assertEqual(len(self.cache.net_amt_data), 500)
        self.assertEqual(len(self.cache.net_amt_data.columns), 5)
