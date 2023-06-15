import logging
import os

import pandas as pd
from flask import Flask, jsonify

from config.configs import default_data_path, server_data_column_mapping
from src.cache import Cache

logging.basicConfig(level=logging.INFO)
pd.set_option('display.max_columns', 100)
app = Flask(__name__)
cache = Cache()


@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/api/v1/data/transactions/<userId>')
def get_transaction(userId):
    logging.info("userId: {}".format(userId))
    if not userId.isdigit():
        return jsonify({'error': 'userId must be an integer'}), 400
    if cache.trans_data is None:
        cache.cache_trans_data(input_file=default_data_path, 
                               columns=server_data_column_mapping.get("transaction").get("input_col"),)
    dataset = cache.trans_data.copy()
    dataset = dataset[dataset['user_id'] == int(userId)]
    dataset.loc[:, 'amount_in_dollars'] = round(dataset.loc[:, 'amount_cents'] / 100, 0)
    dataset.loc[:, 'amount_in_dollars'] = dataset.loc[:,'amount_in_dollars'].astype(int)
    dataset.loc[:, 'datetime'] = dataset['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    result_data = dataset.loc[:, server_data_column_mapping.get("transaction").get("output_col")]
    logging.info("{} records returned".format(result_data.shape[0]))
    return result_data.to_json(orient='records')

@app.route('/api/v1/data/netAmount/<merchantTypeCode>')
def get_net_return(merchantTypeCode):
    logging.info("merchantTypeCode: {}".format(merchantTypeCode))
    if not merchantTypeCode.isdigit():
        return jsonify({'error': 'merchantTypeCode must be an integer'}), 400
    if cache.net_amt_data is None:
        cache.cache_net_amt_data(input_file=default_data_path, 
                                columns=server_data_column_mapping.get("net_return").get("input_col"),)
    all_data = cache.net_amt_data.copy()
    all_data = all_data[all_data['merchant_type_code'] == int(merchantTypeCode)]
    all_data.loc[:, 'date'] = all_data.loc[:, 'datetime'].dt.strftime('%Y-%m-%d')
    purchase_data = all_data[all_data['transaction_type'] == 'PurchaseActivity']
    return_data = all_data[all_data['transaction_type'] == 'ReturnActivity']
    purchase_data = purchase_data.groupby(['date']).agg({'amount_cents': 'sum'}).reset_index()
    return_data = return_data.groupby(['date']).agg({'amount_cents': 'sum'}).reset_index()
    result_data = pd.merge(purchase_data, return_data, on='date', how='outer', suffixes=('_purchase', '_return'))
    result_data.loc[:, 'amount_cents_purchase'] = result_data.loc[:, 'amount_cents_purchase'].fillna(0)
    result_data.loc[:, 'amount_cents_return'] = result_data.loc[:, 'amount_cents_return'].fillna(0)
    result_data.loc[:, 'net_amount_in_dollars'] = round((result_data.loc[:, 'amount_cents_purchase'] - result_data['amount_cents_return']) / 100, 0)
    result_data.loc[:, 'net_amount_in_dollars'] = result_data.loc[:, 'net_amount_in_dollars'].astype(int)
    result_data.loc[:, 'merchant_type_code'] = int(merchantTypeCode)
    result_data = result_data.loc[:, server_data_column_mapping.get("net_return").get("output_col")]
    logging.info("{} records returned".format(result_data.shape[0]))
    return result_data.to_json(orient='records')


if __name__ == '__main__':
    app.run(host='0.0.0.0')