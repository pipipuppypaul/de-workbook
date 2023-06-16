# de-workbook
--------------------------------------------

- Set up the environment:
    - Clone the repo:
		- `git clone https://github.com/pipipuppypaul/de-workbook  kasheesh-de-workbook`
	- Install third-party packages:
		- `cd ./kasheesh-de-workbook`
		- `pip3 install -r requirements.txt`
    - Set environment variables:
        - `export PYTHONPATH=/Users/yuhaibo/workspace/kasheesh-de-workbook`
	- Prepare data:
		- `python3 ./src/etl_pipeline.py --input_file /Users/yuhaibo/Downloads/combined_transactions.csv --output_file /Users/yuhaibo/workspace/kasheesh-de-workbook/database`
	- Launch the server:
		- `export FLASK_APP=server.py; python3 -m flask run -p 3025`
	- Send data request:
		1. http://127.0.0.1:3025/api/v1/data/transactions/<user_id>
		2. http://127.0.0.1:3025/api/v1/data/netAmount/<merchant_type_code>
