import pandas as pd
import argparse
import os
import logging
logging.basicConfig(level=logging.INFO)

class ETLPipeLine:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def read_data(self):
        # file: /Users/yuhaibo/Downloads/combined_transactions.csv
        self.df = pd.read_csv(self.input_file, parse_dates=['datetime'])
        logging.info('Data read from: {}'.format(self.input_file))

    def transform_data(self):
        self.df = self.df.sort_values(by=['datetime'])

    def write_data(self):
        self.df.to_parquet(self.output_file, index=False, partition_cols=['transaction_type'])
        logging.info('Data written to: {}'.format(self.output_file))



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, help='Input path and file name')
    parser.add_argument('--output_file', type=str, help='Output path and file name')
    args = parser.parse_args()

    etl_pipeline = ETLPipeLine(args.input_file, args.output_file)
    etl_pipeline.read_data()
    etl_pipeline.transform_data()
    etl_pipeline.write_data()