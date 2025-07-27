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

base_id = 'XFA4bClwka1T7es3xuMlaoTEg9f'
table_id = 'tbl5s5hwNlDPEG4U'
db_golden_name = os.getenv("DB_GOLDEN_NAME")
golden_table_name = "lark_mkt_sources"
db_raw_name = os.getenv("DB_RAW_NAME")
raw_table_name = "raw_" + golden_table_name

lark_base_params = {
    'filter': 'CurrentValue.[Last Modified Date] > TODAY() - 7',
    'user_id_type': 'user_id'
}

mapping_dict = {
    'order_source_id': {'path': 'ID Nguồn đơn hàng', 'type': 'str'},
    'order_source_name': {'path': 'Tên nguồn đơn hàng (*)', 'type': 'lark_formula'},
    'mkt_employee': {'path': 'employee_email[0].text', 'type': 'str'},
    'from_at': {'path': 'Từ ngày', 'type': 'ms_timestamp'},
    'until_at': {'path': 'Đến ngày', 'type': 'ms_timestamp'},
    'team': {'path': 'Team', 'type': 'str'},
    'product': {'path': 'Sản phẩm', 'type': 'str'},
    'mkt_employee_id': {'path': 'Nhân sự Marketing.[0].id', 'type': 'str'},
}

if __name__ == "__main__":
    setup_logger(__file__)
    
    df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict, params=lark_base_params)

    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database=db_raw_name, 
            table=raw_table_name, 
            df=df, 
            unique_columns=["record_id"],
            create_table=True, 
            mapping_dict=mapping_dict, 
            db_type="raw"
        )

        MariaDBHandler().insert_and_update_from_df(
            database=db_golden_name, 
            table=golden_table_name, 
            df=df, 
            unique_columns=["record_id"], 
            log=True, 
            create_table=True, 
            mapping_dict=mapping_dict,
            db_type="golden"
        )
