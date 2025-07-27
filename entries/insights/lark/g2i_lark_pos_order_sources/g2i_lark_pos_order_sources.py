import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from queries import list_order_sources
from cdp.domain.utils.log_helper import setup_logger

if __name__ == "__main__":
    setup_logger(__file__)

    base_id = 'XFA4bClwka1T7es3xuMlaoTEg9f'
    table_id = 'tblXfgn8I3U3brIq'
    db_golden_name = os.getenv("DB_GOLDEN_NAME")
    table_name = "pos_order_sources"
    lark_api_handler = LarkApiHandle()

    order_sources = MariaDBHandler().read_from_db(query=list_order_sources, output_type='dataframe')
    lark_api_handler.overwrite_table(base_id=base_id, table_id=table_id, input_type='dataframe', df=order_sources)