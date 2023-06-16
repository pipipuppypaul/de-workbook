import argparse
import logging
import os

import pandas as pd

from config.configs import db_data_column_mapping

logging.basicConfig(level=logging.INFO)

class ETLPipeLine:
    def __init__(self, input_file, output_file):
        if not os.path.exists(input_file):
            raise Exception('Input file does not exist')
        self.input_file = input_file
        self.output_file = output_file
        self.df = None

    def read_data(self):
        # file: /Users/yuhaibo/Downloads/combined_transactions.csv
        self.df = pd.read_csv(self.input_file, parse_dates=['datetime'])
        logging.info('Data read from: {}'.format(self.input_file))
        return

    def transform_data(self):
        assert len(db_data_column_mapping['columns']) == len(db_data_column_mapping['types'])
        for i in range(len(db_data_column_mapping['columns'])):
            col = db_data_column_mapping['columns'][i]
            tp = db_data_column_mapping['types'][i]
            self.df[col] = self.df[col].astype(tp)
        self.df = self.df.sort_values(by=['datetime']) #used for future fast fetch
        return

    def write_data(self):
        if not os.path.exists(self.output_file):
            os.makedirs(self.output_file)
        self.df.to_parquet(self.output_file, index=False, partition_cols=['transaction_type'])
        logging.info('Data written to: {}'.format(self.output_file))
        return 



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, help='Input path and file name')
    parser.add_argument('--output_file', type=str, help='Output path and file name')
    args = parser.parse_args()

    etl_pipeline = ETLPipeLine(args.input_file, args.output_file)
    etl_pipeline.read_data()
    etl_pipeline.transform_data()
    etl_pipeline.write_data()