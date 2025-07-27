import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
from entries.golden.pos.pos_order_sources.pos_order_sources import get_order_sources
from entries.golden.lark.lark_mkt_sources.lark_mkt_sources import get_mkt_sources
from queries import undeclared_page_ads

if __name__ == "__main__":
    setup_logger(__file__)

    base_id = 'SFvnb4FlYaQmKssHsjIlyGbyguh'
    ad_table_id = 'tblZO22t5w4XaotL'
    page_table_id = 'tbltSBTZxueKTcIq'
    lark_api_handler = LarkApiHandle()

    page_df_cols = ['team', 'page_id', 'reason']

    params = {
        'filter': 'CurrentValue.[Ngày tạo] >= TODAY() && CurrentValue.[Trạng thái] = "Đã xử lý"'
    }

    current_ad_mapping_dict = {
        'ad_id': {'path': 'ad_id', 'type': 'str'}
    }
    
    current_page_mapping_dict = {
        'page_id': {'path': 'page_id', 'type': 'str'}
    }

    current_ad_df = lark_api_handler.extract_table_to_df(base_id=base_id, table_id=ad_table_id, mapping_dict=current_ad_mapping_dict, params=params)
    current_page_df = lark_api_handler.extract_table_to_df(base_id=base_id, table_id=page_table_id, mapping_dict=current_page_mapping_dict, params=params)

    new_ad_df = MariaDBHandler().read_from_db(query=undeclared_page_ads, output_type='dataframe')
    new_page_df = new_ad_df[page_df_cols].drop_duplicates()

    pos_order_sources = get_order_sources()
    lark_mkt_sources = get_mkt_sources()

    if not current_ad_df.empty:
        new_ad_df = new_ad_df[~new_ad_df["ad_id"].isin(current_ad_df["ad_id"])]

    page_excluded_ids = set(current_page_df["page_id"])
    page_excluded_ids.update(pos_order_sources["id"])
    page_excluded_ids.update(lark_mkt_sources["order_source_id"])
    new_page_df = new_page_df.loc[~new_page_df["page_id"].isin(page_excluded_ids)]

    if not new_ad_df.empty:
        lark_api_handler.overwrite_table(base_id=base_id, table_id=ad_table_id, df=new_ad_df)

    if not new_page_df.empty:
        new_page_df['Trạng thái'] = "Chờ xử lý"
        lark_api_handler.batch_create_from_df(base_id=base_id, table_id=page_table_id, df=new_page_df)