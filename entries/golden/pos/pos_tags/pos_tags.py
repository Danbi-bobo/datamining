import sys
import os
import json
import pandas as pd
from dotenv import load_dotenv
from numpy import nan

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.domain.utils.config_helper import get_pos_shop_ids

def get_tags_from_api(shop_ids):
    result = []
    for shop_id in shop_ids:
        api_key = os.getenv(f"POS_API_KEY_{shop_id}")
        if not (shop_id and api_key):
            return result
        pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
        order_sources_from_api = pos_handler.get_all(endpoint='orders/tags')
        result.extend([{**item, 'shop_id': shop_id} for item in order_sources_from_api])
    
    return result

def prepare_golden_df(raw_df):
    df = raw_df.copy()

    numeric_cols = ['group_id', 'tag_id']
    string_cols = ['shop_id', 'tag_name', 'group_name']
    golden_cols = numeric_cols + string_cols

    df_exploded = df.explode('groups', ignore_index=True)
    group_details = pd.json_normalize(df_exploded['groups']).add_prefix('group_')
    golden_df = pd.concat([df_exploded.drop(columns='groups'), group_details], axis=1)

    golden_df.rename(columns={'id': 'tag_id', 'name': 'tag_name'}, inplace=True, errors='ignore')

    golden_df[numeric_cols] = golden_df[numeric_cols].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    golden_df[string_cols] = golden_df[string_cols].astype(str)
    golden_df = golden_df[golden_cols]

    golden_df.replace(['None', nan, 'nan'], None, inplace=True)
    
    return golden_df

if __name__ == "__main__":
    setup_logger(__file__)

    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    golden_table_name = "pos_tags"
    db_raw_name = os.getenv("DB_RAW_NAME")
    raw_table_name = "raw_" + golden_table_name

    shop_ids = get_pos_shop_ids()
    tags = get_tags_from_api(shop_ids)
    raw_df = pd.DataFrame(tags)

    golden_df = prepare_golden_df(raw_df)    

    if not raw_df.empty:
        raw_df['groups'] = raw_df['groups'].apply(lambda x: json.dumps(x, ensure_ascii=False))
        raw_df = raw_df.astype(str)
        MariaDBHandler().insert_and_update_from_df(db_raw_name, raw_table_name,raw_df, unique_columns=["id", "shop_id"])
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(db_golden_name, golden_table_name, golden_df, unique_columns=["id", "shop_id"], log=True)
