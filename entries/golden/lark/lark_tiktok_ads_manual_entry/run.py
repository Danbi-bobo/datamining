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
import pandas as pd

base_id = 'XFA4bClwka1T7es3xuMlaoTEg9f'
table_id = 'tblsPOv6BEDYX9XY'
golden_table_name = "lark_tiktok_ads_manual_entry"


mapping_dict = {
    'date': {'path': 'Ngày', 'type': 'ms_timestamp'},
    'team': {'path': 'Team', 'type': 'str'},
    'spend': {'path': 'Chi phí', 'type': 'double'},
    'employee_id': {'path': 'Nhân sự.[0].id', 'type': 'str'},
    'employee_email': {'path': 'Nhân sự.[0].email', 'type': 'str'},
    'creator_id': {'path': 'Người tạo.id', 'type': 'str'},
    'creator_email': {'path': 'Người tạo.email', 'type': 'str'},
}

db_golden_name = os.getenv("DB_GOLDEN_NAME")
db_raw_name = os.getenv("DB_RAW_NAME")
raw_table_name = "raw_" + golden_table_name

lark_base_params = {
    'user_id_type': 'user_id'
}

if __name__ == "__main__":
    setup_logger(__file__)
    
    df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict, params=lark_base_params)
    df['date'] = df['date'] + pd.Timedelta(hours=7)
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
df