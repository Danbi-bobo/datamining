import sys
import os
import pandas as pd
import json
from numpy import nan
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
setup_logger(__file__)

base_id = 'XFA4bClwka1T7es3xuMlaoTEg9f'
table_id = 'tblVYxiPwo7xCegY'
db_golden_name = os.getenv("DB_GOLDEN_NAME")
golden_table_name = "lark_render_utm"
db_raw_name = os.getenv("DB_RAW_NAME")
raw_table_name = "raw_" + golden_table_name

lark_base_params = {
    'filter': 'CurrentValue.[Last Modified Date] > TODAY() - 7',
    'user_id_type': 'user_id'
}

mapping_dict = {
    'form_subinfo': {'path': 'Nhóm quảng cáo', 'type': 'str'},
    'form': {'path': 'Form Content', 'type': 'str'},
    'channel': {'path': 'Kênh', 'type': 'str'},
    'ldp_url': {'path': 'Landing Page URL.link', 'type': 'str'},
    'order_source_name': {'path': 'Nguồn đơn hàng.[0].text', 'type': 'str'},
    'order_source_id': {'path': 'ID Nguồn đơn hàng', 'type': 'lark_formula'},
    'mkt_employee_id': {'path': 'Người tạo.id', 'type': 'str'},
    'product': {'path': 'Sản phẩm', 'type': 'str'},
    'team': {'path': 'Team', 'type': 'str'}
}

df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict, params=lark_base_params)
df.rename(columns={'record_id': 'utm_source'}, inplace=True)

if not df.empty:
    MariaDBHandler().insert_and_update_from_df(
        database=db_raw_name, 
        table=raw_table_name, 
        df=df, 
        unique_columns=["utm_source"],
        create_table=True, 
        mapping_dict=mapping_dict, 
        db_type="raw"
    )

    MariaDBHandler().insert_and_update_from_df(
        database=db_golden_name, 
        table=golden_table_name, 
        df=df, 
        unique_columns=["utm_source"], 
        log=True, 
        create_table=True, 
        mapping_dict=mapping_dict,
        db_type="golden"
    )