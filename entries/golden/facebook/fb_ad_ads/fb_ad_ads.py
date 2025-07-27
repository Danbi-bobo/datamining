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
import time
from queries import get_all_ads_query

def get_ads_detail(ads_df, fb_token, base_params):
    dfs = []

    for team, token in fb_token.items():
        fb_api_handler = FacebookAPIHandler(token)
        ad_id_list = ads_df[ads_df['team'] == team]['ad_id'].tolist()
        ad_id_list_split = split_list(ad_id_list, max_items_per_sublist=50)

        ads_detail = []
        for sublist in ad_id_list_split:
            ids_query_str = ','.join(sublist)
            base_params['ids'] = ids_query_str
            try:
                ads = fb_api_handler.get_all(endpoint='', params=base_params)
                ads_detail.extend(list(ads[0].values()))
                time.sleep(0.5)
            except Exception as e:
                continue

        df = pd.DataFrame(ads_detail)
        df['team'] = team
        dfs.append(df)

    return dfs


def safe_convert(x):
    try:
        x = pd.to_datetime(x, errors='coerce', utc=True)
        x = x.dt.tz_localize(None)  # Bỏ timezone UTC, trả về dạng naive
        return x
    except Exception:
        return x


def prepare_golden_df(df):
    df = df.copy()
    return_cols = ['team', 'ad_id', 'ad_name', 'status', 'account_id', 'campaign_id', 'adset_id', 'creative_id', 'created_time_utc', 'ad_last_updated_time_utc']
    str_cols = ['creative_id', 'account_id', 'campaign_id', 'adset_id', 'ad_id']
    datetime_cols = ['created_time', 'ad_last_updated_time']
    creative_cols = ['creative_id']

    df[datetime_cols] = df[datetime_cols].apply(safe_convert)

    df['creative_id'] = df['creative'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
    df[str_cols] = df[str_cols].astype(str)
    
    df.loc[:, 'account_id'] = df['account_id'].where(df['account_id'].str.startswith('act_'), 'act_' + df['account_id'])

    df.rename(columns={'created_time': 'created_time_utc', 'ad_last_updated_time': 'ad_last_updated_time_utc'}, inplace=True, errors='ignore')

    return df[return_cols]

if __name__ == "__main__":
    setup_logger(__file__)

    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    golden_table_name = "fb_ad_ads"
    db_raw_name = os.getenv("DB_RAW_NAME")
    raw_table_name = "raw_fb_ad_ads"

    base_params = {
        'fields': 'id, name, account_id, campaign_id, adset_id, created_time, updated_time, status, creative',
        'limit': 50
    }

    fb_token = LarkApiHandle().get_fb_tokens_in_lark()
    ads_df = MariaDBHandler().read_from_db(database=db_golden_name, query=get_all_ads_query, output_type='dataframe')

    all_ads = get_ads_detail(ads_df=ads_df, fb_token=fb_token, base_params=base_params)
    raw_df = pd.concat(all_ads, ignore_index=True) if all_ads else pd.DataFrame()
    raw_df.rename(columns={'id': 'ad_id', 'name': 'ad_name', 'updated_time': 'ad_last_updated_time'}, inplace=True, errors='ignore')

    golden_df = prepare_golden_df(raw_df)

    if not raw_df.empty:
        raw_df = raw_df.astype(str)
        MariaDBHandler().insert_and_update_from_df(db_raw_name, raw_table_name, raw_df, unique_columns=["ad_id"])
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(db_golden_name, golden_table_name, golden_df, unique_columns=["ad_id"], log=True)