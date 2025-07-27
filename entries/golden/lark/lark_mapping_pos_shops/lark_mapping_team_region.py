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
    table_id = 'tblNDb5sohFoTGOF'
    golden_table_name = "lark_mapping_team_region"
    raw_table_name = "raw_" + golden_table_name

    mapping_dict = {
        'region': {'path': 'Region', 'type': 'str'},
        'team': {'path': 'Các Team phụ trách', 'type': ''},
    }

    df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict)
    df = df.explode("team").reset_index(drop=True)

    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            table=golden_table_name, 
            df=df, 
            unique_columns=["region", "team"], 
            log=True, 
            create_table=True, 
            mapping_dict=mapping_dict,
            db_type="golden"
        )
df