import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.google.google_api_handler import GoogleAdsHandler
from cdp.domain.utils.log_helper import setup_logger
import logging
import pandas as pd
import proto
from numpy import nan
from urllib.parse import urlparse, parse_qs
from queries import insights_query, ad_accounts_query
import time
from typing import List, Dict

def fetch_insights_for_one_account(google_client, ad_account, insights_query):
    logging.info(f'Start collect insights data of account:  {ad_account}')
    df = google_client.run_query(query=insights_query, output_type='dataframe', boolean_as='int', customer_id=str(ad_account))
    df['account_id'] = ad_account
    logging.info(f'Complete collect insights data of account:  {ad_account}')
    return df

def fetch_insights_for_all_accounts(google_credentials: List[Dict], insights_query: str, ad_accounts: pd.DataFrame):
    dfs = []

    for credential in google_credentials:
        mcc_id = int(credential['mcc_id'])
        team = credential.get('team')
        try:
            google_client = GoogleAdsHandler(
                developer_token=credential['developer_token'],
                client_id=credential['client_id'],
                client_secret=credential['client_secret'],
                refresh_token=credential['refresh_token'],
                mcc_id=mcc_id
            )
        except Exception as e:
            # logging.error(f"[ERROR] Failed to create GoogleAdsHandler for team '{team}', mcc_id '{mcc_id}'")
            # logging.exception(e)
            continue
        
        ad_accounts_this_mcc = ad_accounts.loc[ad_accounts['mcc_id'] == mcc_id, 'id']
        
        for ad_account in ad_accounts_this_mcc:
            df = fetch_insights_for_one_account(google_client, ad_account, insights_query)
            dfs.append(df)
            time.sleep(2)

    new_df = pd.concat(dfs, ignore_index=True)
    new_df.replace(nan, None, inplace=True)
    return new_df

def scale_metrics(df, columns, scale=1_000_000):
    for col in columns:
        if col in df.columns:
            df[col] = df[col] / scale
    return df

if __name__ == "__main__":
    setup_logger(__file__)
    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    db_raw_name = os.getenv("DB_RAW_NAME")
    golden_table_name = "gg_ad_insights"
    raw_table_name = f'raw_{golden_table_name}'
    metric_columns = ["spend", "average_cpm", "average_cpe", "average_cpc"]

    ad_accounts = MariaDBHandler().read_from_db(query=ad_accounts_query, output_type='dataframe')
    lark_client = LarkApiHandle()
    google_credentials = lark_client.get_google_credentials()
    
    df = fetch_insights_for_all_accounts(google_credentials, insights_query, ad_accounts)
    
    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(database=db_raw_name, table=raw_table_name, df=df, unique_columns=['ad_id', 'date'], db_type='raw')
        
        df['final_urls'] = df['final_urls'].apply(
            lambda x: list(x)[0] if isinstance(x, (list, tuple, set, proto.marshal.collections.repeated.Repeated)) and len(x) > 0 else None
        )
        df['utm_source'] = df['final_urls'].apply(
            lambda x: parse_qs(urlparse(x).query).get('utm_source', [None])[0] if isinstance(x, str) else None
        )
        df = scale_metrics(df, metric_columns)
        MariaDBHandler().insert_and_update_from_df(database=db_golden_name, table=golden_table_name, df=df, unique_columns=['ad_id', 'date'], log=True)