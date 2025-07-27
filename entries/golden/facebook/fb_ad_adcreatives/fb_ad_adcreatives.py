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
import json
from urllib.parse import urlparse, parse_qs
from queries import get_all_creative

def get_creatives_detail(creatives_df, fb_token, base_params):
    dfs = []

    for team, token in fb_token.items():
        fb_api_handler = FacebookAPIHandler(token)
        creative_id_list = creatives_df[creatives_df['team'] == team]['creative_id'].tolist()
        creative_id_list_split = split_list(creative_id_list, max_items_per_sublist=50)
        creatives_detail = []
        for sublist in creative_id_list_split:
            ids_query_str = ','.join(sublist)
            base_params['ids'] = ids_query_str
            try:
                creatives = fb_api_handler.get_all(endpoint='', params=base_params)
                creatives_detail.extend(list(creatives[0].values()))
                time.sleep(0.5)
            except Exception as e:
                continue

        df = pd.DataFrame(creatives_detail)
        df['team'] = team
        dfs.append(df)

    return dfs

def extract_link(row):
    object_story = row.get('object_story_spec')
    asset_feed = row.get('asset_feed_spec')

    if isinstance(object_story, str):
        try:
            object_story = json.loads(object_story)
        except json.JSONDecodeError:
            object_story = None

    if isinstance(asset_feed, str):
        try:
            asset_feed = json.loads(asset_feed)
        except json.JSONDecodeError:
            asset_feed = None

    if isinstance(object_story, dict):
        data = (
            object_story.get('link_data') or 
            object_story.get('video_data') or 
            {}
        )
        link = data.get('call_to_action', {}).get('value', {}).get('link')
        if link:
            return link, 'object_story_spec'

    if isinstance(asset_feed, dict):
        link_urls = asset_feed.get('link_urls', [])
        if link_urls:
            return link_urls[0].get('website_url'), 'asset_feed_spec'

    return None, None


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

def prepare_golden_df(df):
    df = df.copy()
    object_story_cols = ['link', 'object_type']
    utm_cols = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term']
    base_cols = ['team', 'creative_id', 'creative_name', 'account_id', 'status', 'actor_id', 'call_to_action_type', 'object_story_id']
    return_cols = base_cols + object_story_cols + utm_cols

    df[['link', 'object_type']] = df.apply(extract_link, axis=1, result_type='expand')
    df[utm_cols] = df['link'].apply(lambda x: pd.Series(extract_params(x)))
    
    df.loc[:, 'account_id'] = df['account_id'].where(df['account_id'].str.startswith('act_'), 'act_' + df['account_id'])

    return df[return_cols]

if __name__ == "__main__":
    setup_logger(__file__)

    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    golden_table_name = "fb_ad_adcreatives"
    db_raw_name = os.getenv("DB_RAW_NAME")
    raw_table_name = "raw_" + golden_table_name

    base_params = {
        'fields': 'actor_id, call_to_action_type, object_story_spec, id, name, status, account_id, asset_feed_spec, object_story_id',
        'limit': 50
    }

    fb_token = LarkApiHandle().get_fb_tokens_in_lark()
    creatives_df = MariaDBHandler().read_from_db(database=db_golden_name, query=get_all_creative, output_type='dataframe')

    all_ads = get_creatives_detail(creatives_df=creatives_df, fb_token=fb_token, base_params=base_params)
    raw_df = pd.concat(all_ads, ignore_index=True) if all_ads else pd.DataFrame()
    raw_df.rename(columns={'id': 'creative_id', 'name': 'creative_name'}, inplace=True, errors='ignore')

    golden_df = prepare_golden_df(raw_df)

    if not raw_df.empty:
        raw_df = raw_df.astype(str)
        MariaDBHandler().insert_and_update_from_df(db_raw_name, raw_table_name, raw_df, unique_columns=["creative_id"])
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(db_golden_name, golden_table_name, golden_df, unique_columns=["creative_id"], log=True)