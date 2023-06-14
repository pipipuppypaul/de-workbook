import pandas as pd
import argparse
import os
import logging
logging.basicConfig(level=logging.INFO)


def read_parquet_data(input_file, columns=None, filters=None):
    df = pd.read_parquet(input_file, 
                            engine='pyarrow', 
                            columns=columns, 
                            filters=filters)
    if columns is None:
        logging.info(f'Read all columns of data from: {input_file}'.format(input_file))
    else:
        logging.info(f'Read columns {len(columns)} of data from: {input_file}'.format(input_file))
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, help='Input path and file name')
    args = parser.parse_args()

    
    data = read_parquet_data(input_file=args.input_file)
    # columns=['transaction_type', 'merchant_type_code'], filters=[('transaction_type', '==', 'PurchaseActivity')]
    print(data)