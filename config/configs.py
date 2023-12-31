default_data_path = './database/'

transaction_type_list = [
    "PurchaseActivity",
    "ReturnActivity",
]

server_data_column_mapping = {
    "transaction": {
        "input_col": ["user_id", "transaction_type", "merchant_type_code", "amount_cents", "datetime"],
        "input_type": ["int", "string", "int", "int", "datetime"],
        "output_col": ["user_id", "amount_in_dollars", "datetime", "merchant_type_code"],
        "output_type": ["int", "int", "datetime", "int"]
        },
    "net_return": {
        "input_col": ["merchant_type_code", "transaction_type", "amount_cents", "datetime"],
        "input_type": ["int", "string", "int", "datetime"],
        "output_col": ["merchant_type_code", "net_amount_in_dollars", "date"],
        "output_type": ["int", "int", "date"]
    }
}

db_data_column_mapping = {
    "columns": ["user_id", "transaction_type", "merchant_type_code", "amount_cents", "datetime"],
    "types": ["int", "string", "int", "int", "datetime64[ns]"]
}