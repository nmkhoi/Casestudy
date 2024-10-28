from dune_client.client import DuneClient
import pandas as pd
import requests
import time
import json


dune = DuneClient("<YOUR DUNE API>")
query_result = dune.get_latest_result(4204053)
df = pd.DataFrame.from_dict(query_result.result.rows)

final_data = pd.DataFrame(columns=['index','trans_id','address','token_decimals','pre_balance'])

for index,row in df.iterrows():

    url = "https://pro-api.solscan.io/v2.0/account/balance_change?address={}&token=JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN&page_size=100&page=1".format(row['claimed_wallet'])
    headers = {"token":"<YOUR SOLSCAN API>"}
    response = requests.get(url, headers=headers)

    try:
        response_dict = json.loads(response.text)
        for tx in response_dict['data']:
            if tx['trans_id'] == row['tx_id']:
                trans_id = tx['trans_id']
                address = tx['address']
                token_address = tx['token_address']
                token_decimals = tx['token_decimals']
                pre_balance = tx['pre_balance']

                final_data = pd.concat([final_data, pd.DataFrame.from_records([{
                    'index':index,
                    'trans_id':trans_id,
                    'address':address,
                    'token_decimals':token_decimals,
                    'pre_balance':pre_balance
                }])])
    except Exception:
        continue

final_data.to_excel('prebalance.xlsx')