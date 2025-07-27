import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.domain.utils.list import split_list
import pandas as pd
import time
import re
from urllib.parse import urlparse, parse_qs
from queries import get_post_creative
import logging

def get_post_creative_detail(df, params):
    data = []

    for _, row in df.iterrows():
        access_token = row['page_access_token']
        object_story_id = row['object_story_id']

        fb_client = FacebookAPIHandler(access_token)
        
        try:
            response = fb_client.get_all(endpoint=object_story_id, params=params)
            post_data = response[0]
            data.append(post_data)
            time.sleep(0.5)
        except Exception as e:
            print(f"Lỗi với object_story_id={object_story_id}: {e}")
            continue

    result_df = pd.DataFrame(data)
    return result_df


def extract_params(url):
    if not isinstance(url, str):
        return None, None, None, None
    
    query_params = parse_qs(urlparse(url).query)

    return (
        query_params.get('utm_source', [None])[0],
        query_params.get('utm_medium', [None])[0],
        query_params.get('utm_campaign', [None])[0],
        query_params.get('utm_term', [None])[0]
    )

def get_link(row):
    call_to_action = row.get('call_to_action')
    if isinstance(call_to_action, dict):
        value = call_to_action.get('value')
        if isinstance(value, dict):
            link = value.get('link')
            if link:
                return link
    
    message = row.get('message')
    if isinstance(message, str):
        urls = re.findall(r'(https?://[^\s]+)', message)
        if urls:
            return urls[-1]
    
    return None

def extract_link(df):
    df = df.copy()
    df['link'] = df.apply(get_link, axis=1)
    return df

def prepare_golden_df(df, params, utm_columns, golden_columns):
    new_df = get_post_creative_detail(df, params)
    new_df['link'] = new_df.apply(get_link, axis=1)
    new_df[utm_columns] = new_df['link'].apply(lambda x: pd.Series(extract_params(x)))
    new_df['object_type'] = 'page_post'
    new_df.rename(columns={'id': 'object_story_id'}, inplace=True)
    merged_df = pd.merge(df, new_df, how='left', on=['object_story_id'])
    golden_df = merged_df[golden_columns]
    
    return golden_df

if __name__ == "__main__":
    # setup_logger(__file__)

    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    golden_table_name = "fb_ad_adcreatives"

    params = {
        'fields': 'call_to_action, message',
        'limit': 50
    }

    utm_cols = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term']
    golden_cols = ['creative_id', 'link', 'object_type'] + utm_cols

    db_client = MariaDBHandler()
    df = db_client.read_from_db(query=get_post_creative, output_type='dataframe')
    if df is None:
        df = pd.DataFrame()

    if df.empty:
        logging.info('No records satisfied')
    else:
        golden_df = prepare_golden_df(df, params, utm_columns=utm_cols, golden_columns=golden_cols)
        if not golden_df.empty:
            MariaDBHandler().insert_and_update_from_df(db_golden_name, golden_table_name, golden_df, unique_columns=["creative_id"], log=True)