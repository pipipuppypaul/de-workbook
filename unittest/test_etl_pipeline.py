import os
import sys
import unittest

import pandas as pd

from config import configs

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src import etl_pipeline


class TestETLPipeLine(unittest.TestCase):
    def setUp(self):
        self.etl_pipeline = etl_pipeline.ETLPipeLine(
            input_file='data/combined_transactions.csv', 
            output_file='database/')

    def test_read_data(self):
        self.etl_pipeline.read_data()
        self.assertEqual(len(self.etl_pipeline.df), 500)
        self.assertEqual(len(self.etl_pipeline.df.columns), 5)

    def test_transform_data(self):
        self.etl_pipeline.read_data()
        self.etl_pipeline.transform_data()
        self.assertEqual(len(self.etl_pipeline.df.columns), 5)
        for i in range(len(configs.db_data_column_mapping['columns'])):
            col = configs.db_data_column_mapping['columns'][i]
            tp = configs.db_data_column_mapping['types'][i]
            self.assertEqual(self.etl_pipeline.df[col].dtype, tp)
