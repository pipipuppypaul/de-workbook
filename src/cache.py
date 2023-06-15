import pandas as pd
import argparse
import os
import logging
logging.basicConfig(level=logging.INFO)


class Cache:
    def __init__(self):
        self.trans_data = None
        self.net_amt_data = None

    def read_parquet_data(self, input_file, columns=None, filters=None):
        df = pd.read_parquet(input_file, 
                                engine='pyarrow', 
                                columns=columns, 
                                filters=filters)
        if columns is None:
            logging.info(f'Read all columns of data from: {input_file}'.format(input_file))
        else:
            logging.info(f'Read columns {len(columns)} of data from: {input_file}'.format(input_file))
        return df
    
    def cache_trans_data(self, input_file, columns=None, filters=None):
        if self.trans_data is None:
            self.trans_data = self.read_parquet_data(input_file=input_file,
                                                     columns=columns,
                                                     filters=filters)
            logging.info("Transaction data cached")

    def decache_trans_data(self):
        self.trans_data = None
        logging.info("Transaction data de-cached")
    
    def cache_net_amt_data(self, input_file, columns=None, filters=None):
        if self.net_amt_data is None:
            self.net_amt_data = self.read_parquet_data(input_file=input_file,
                                                     columns=columns,
                                                     filters=filters)
            logging.info("Net amount data cached")

    def decache_net_amt_data(self):
        self.net_amt_data = None
        logging.info("Net amount data de-cached")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, help='Input path and file name')
    args = parser.parse_args()

    ch = Cache()
    data = ch.read_parquet_data(input_file=args.input_file)
    # columns=['transaction_type', 'merchant_type_code'], filters=[('transaction_type', '==', 'PurchaseActivity')]
    print(data)