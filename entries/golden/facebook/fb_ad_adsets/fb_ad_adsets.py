import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.domain.utils.list import split_list
import pandas as pd
import numpy as np
import time
from queries import get_all_adsets_query

def get_adsets_detail(adsets_df, fb_token, base_params):
    dfs = []

    for team, token in fb_token.items():
        fb_api_handler = FacebookAPIHandler(token)
        ad_id_list = adsets_df[adsets_df['team'] == team]['adset_id'].tolist()
        ad_id_list_split = split_list(ad_id_list, max_items_per_sublist=50)

        adsets_detail = []
        for sublist in ad_id_list_split:
            ids_query_str = ','.join(sublist)
            base_params['ids'] = ids_query_str
            try:
                adsets = fb_api_handler.get_all(endpoint='', params=base_params)
                adsets_detail.extend(list(adsets[0].values()))
                time.sleep(0.5)
            except Exception as e:
                continue

        df = pd.DataFrame(adsets_detail)
        df['team'] = team
        dfs.append(df)

    return dfs

def prepare_golden_df(df):
    df = df.copy()
    
    str_cols = ['account_id', 'campaign_id', 'adset_id', 'adset_name', 'optimization_goal']
    numeric_cols = ['daily_budget', 'lifetime_budget', 'budget_remaining']
    existing_numeric_cols = df.columns.intersection(numeric_cols)
    datetime_cols = ['created_time', 'adset_last_updated_time']
    
    df[existing_numeric_cols] = df[existing_numeric_cols].apply(pd.to_numeric, errors='coerce')
    df[str_cols] = df[str_cols].astype(str)
    df[datetime_cols] = df[datetime_cols].apply(lambda x: pd.to_datetime(x, errors='coerce').dt.tz_convert('UTC').dt.tz_localize(None))
    
    df.loc[:, 'account_id'] = df['account_id'].where(df['account_id'].str.startswith('act_'), 'act_' + df['account_id'])
    df.replace(np.nan, None, inplace=True)
    
    df.rename(columns={'created_time': 'created_time_utc', 'adset_last_updated_time': 'adset_last_updated_time_utc'}, inplace=True, errors='ignore')
    
    return df

if __name__ == "__main__":
    setup_logger(__file__)

    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    golden_table_name = "fb_ad_adsets"
    db_raw_name = os.getenv("DB_RAW_NAME")
    raw_table_name = "raw_fb_ad_adsets"

    base_params = {
        'fields': 'id, name, account_id, campaign_id, created_time, updated_time, status, daily_budget, lifetime_budget, optimization_goal, budget_remaining',
        'limit': 50
    }

    fb_token = LarkApiHandle().get_fb_tokens_in_lark()
    adsets_df = MariaDBHandler().read_from_db(database=db_golden_name, query=get_all_adsets_query, output_type='dataframe')

    all_adsets = get_adsets_detail(adsets_df=adsets_df, fb_token=fb_token, base_params=base_params)
    raw_df = pd.concat(all_adsets, ignore_index=True) if all_adsets else pd.DataFrame()
    raw_df.rename(columns={'id': 'adset_id', 'name': 'adset_name', 'updated_time': 'adset_last_updated_time'}, inplace=True, errors='ignore')

    golden_df = prepare_golden_df(raw_df)

    if not raw_df.empty:
        raw_df = raw_df.astype(str)
        MariaDBHandler().insert_and_update_from_df(db_raw_name, raw_table_name, raw_df, unique_columns=["adset_id"])
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(db_golden_name, golden_table_name, golden_df, unique_columns=["adset_id"], log=True)