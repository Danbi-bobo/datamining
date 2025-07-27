import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
from cdp.domain.utils.log_helper import setup_logger
import pandas as pd
from numpy import nan
import json
from queries import get_ad_accounts

status_dict = {
  "1": "ACTIVE",
  "100": "PENDING_CLOSURE",
  "101": "CLOSED",
  "2": "DISABLED",
  "201": "ANY_ACTIVE",
  "3": "UNSETTLED",
  "202": "ANY_CLOSED",
  "7": "PENDING_RISK_REVIEW",
  "8": "PENDING_SETTLEMENT",
  "9": "IN_GRACE_PERIOD"
}

def get_ad_accounts_from_api(token_dict):
    dfs = []
    for team, token in token_dict.items():
        endpoint = "me/adaccounts"
        params = {
            'fields': 'id, name, currency, balance, account_status, disable_reason, timezone_offset_hours_utc, user_tasks',
            'limit': '200'
        }

        fb_api_handler = FacebookAPIHandler(token)

        try:
            accounts = fb_api_handler.get_all(endpoint=endpoint, params=params)

            df = pd.DataFrame(accounts)
            df['team'] = team
            dfs.append(df)

        except Exception as e:
            continue
    return dfs

def prepare_golden_df(raw_df, get_ad_accounts_query) -> pd.DataFrame:
    raw_df.rename(columns={'timezone_offset_hours_utc': 'timezone'}, inplace=True, errors='ignore')
    if 'user_tasks'in raw_df.columns:
        raw_df['user_tasks'] = raw_df['user_tasks'].apply(json.dumps)
    raw_df.replace(nan, None, inplace=True)
    raw_df = raw_df.copy()
    raw_df.drop_duplicates(subset=['id'], keep='first', inplace=True)

    if 'id' in raw_df.columns:
        db_ad_accounts = MariaDBHandler().read_from_db(query=get_ad_accounts_query, output_type='dataframe')
        missing_ids = db_ad_accounts[~db_ad_accounts['id'].isin(raw_df['id'])]
        missing_ids.loc[:, 'account_status'] = -1

        final_df = pd.concat([raw_df, missing_ids], ignore_index=True)
        final_df['status_name'] = final_df['account_status'].astype(str).map(status_dict)
        final_df.replace(nan, None, inplace=True)
    else:
        return raw_df
    
    return final_df

if __name__ == "__main__":
    setup_logger(__file__)

    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    golden_table_name = "fb_ad_accounts"
    db_raw_name = os.getenv("DB_RAW_NAME")
    raw_table_name = "raw_fb_ad_accounts"
    fb_token = LarkApiHandle().get_fb_tokens_in_lark()
    all_accessible_ad_accounts = get_ad_accounts_from_api(fb_token)

    raw_df = pd.concat(all_accessible_ad_accounts, ignore_index=True) if all_accessible_ad_accounts else pd.DataFrame()
    golden_df = prepare_golden_df(raw_df, get_ad_accounts)

    if not raw_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database=db_raw_name, 
            table=raw_table_name,
            df=raw_df, 
            unique_columns=["id", "team"],
            db_type='raw'
        )
    
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database=db_golden_name, 
            table=golden_table_name, 
            df=golden_df, 
            unique_columns=["id"],
            log=True,
            db_type='golden'
        )