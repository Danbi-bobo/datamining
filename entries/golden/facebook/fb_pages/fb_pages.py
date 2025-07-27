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
from typing import List
import json

def get_pages(token_dict):
    dfs = []
    endpoint = 'me/accounts'
    params = {
        'fields': 'id, name, access_token, tasks, category',
        'limit': '200'
    }
    for team, token in token_dict.items():
        fb_client = FacebookAPIHandler(token)
        try:
            pages = fb_client.get_all(endpoint=endpoint, params=params)
            df = pd.DataFrame(pages)
            df['team'] = team
            dfs.append(df)
        except Exception as e:
            continue
    
    all_pages_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    return all_pages_df

def prepare_golden_df(raw_df, rename_dict, json_columns: List):
    raw_df = raw_df.copy()
    raw_df.rename(columns=rename_dict, inplace=True)
    raw_df.replace(nan, None, inplace=True)
    handle_json(raw_df, json_columns)
    return raw_df

def handle_json(df: pd.DataFrame, columns: List):
    for col in columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

if __name__ == "__main__":
    setup_logger(__file__)
    db_golden_name = os.getenv('DB_GOLDEN_NAME')
    db_raw_name = os.getenv('DB_RAW_NAME')
    table_golden_name = 'fb_pages'
    table_raw_name = 'raw_' + table_golden_name

    rename_dict = {
        'id': 'page_id',
        'name': 'page_name',
        'access_token': 'page_access_token',
        'tasks': 'user_tasks'
    }
    json_columns = ['user_tasks']

    lark_client = LarkApiHandle()
    fb_tokens = lark_client.get_fb_tokens_in_lark()

    raw_df = get_pages(fb_tokens)
    golden_df = prepare_golden_df(raw_df, rename_dict, json_columns)

    if not raw_df.empty:
        MariaDBHandler().insert_and_update_from_df(db_raw_name, table_raw_name,raw_df, unique_columns=["id", "team"], db_type='raw')
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(db_golden_name, table_golden_name, golden_df, unique_columns=["team", "page_id"], log=True)
