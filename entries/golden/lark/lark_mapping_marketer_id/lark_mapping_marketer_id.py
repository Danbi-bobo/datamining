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

if __name__ == "__main__":
    setup_logger(__file__)

    base_id = 'XFA4bClwka1T7es3xuMlaoTEg9f'
    table_id = 'tbljQnJz2J0MO0nK'
    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    golden_table_name = "lark_mapping_marketer"
    db_raw_name = os.getenv("DB_RAW_NAME")
    raw_table_name = "raw_" + golden_table_name

    mapping_dict = {
        'employee_name': {'path': 'Nhân viên[0].name', 'type': 'str'},
        'marketer_id': {'path': 'marketer_id', 'type': 'str'},
        'team': {'path': 'Team', 'type': 'str'},
        'email': {'path': 'email[0].text', 'type': 'str'},
    }

    df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict)

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