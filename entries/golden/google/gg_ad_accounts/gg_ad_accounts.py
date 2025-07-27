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
from typing import Dict, List
import pandas as pd
from numpy import nan
from queries import ad_accounts_query

def get_single_ad_account(google_credential: Dict):
    try:
        team = google_credential.get('team')
        
        google_client = GoogleAdsHandler(
            developer_token=google_credential['developer_token'],
            client_id=google_credential['client_id'],
            client_secret=google_credential['client_secret'],
            refresh_token=google_credential['refresh_token'],
            mcc_id=google_credential['mcc_id']
        )

        df = google_client.run_query(query=ad_accounts_query, output_type='dataframe', boolean_as='int')
        df['team'] = team
        df['mcc_id'] = int(google_credential['mcc_id'])

        return df

    except Exception as e:
        print(f"[ERROR] Failed to get ad account for team '{google_credential.get('team')}'")
        print(f"[DETAILS] {str(e)}")
        return None

def get_all_ad_accounts(google_credentials: List[Dict]):
    dfs = []
    for credential in google_credentials:
        df = get_single_ad_account(credential)
        if df is not None:
            dfs.append(df)
    
    new_df = pd.concat(dfs, ignore_index=True)
    new_df.replace(nan, None, inplace=True)
    return new_df

if __name__ == "__main__":
    setup_logger(__file__)
    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    db_raw_name = os.getenv("DB_RAW_NAME")
    golden_table_name = "gg_ad_accounts"
    raw_table_name = f'raw_{golden_table_name}'

    lark_client = LarkApiHandle()
    google_credentials = lark_client.get_google_credentials()
    ad_accounts = get_all_ad_accounts(google_credentials)
    if not ad_accounts.empty:
        MariaDBHandler().insert_and_update_from_df(
            database=db_raw_name, 
            table=raw_table_name, 
            df=ad_accounts, 
            unique_columns=['id', 'team'], 
            db_type='raw'
        )
        
        ad_accounts.drop_duplicates(subset=['id'], keep='first', inplace=True)
        MariaDBHandler().insert_and_update_from_df(
            database=db_golden_name, 
            table=golden_table_name, 
            df=ad_accounts, 
            unique_columns=['id'], 
            log=True
        )