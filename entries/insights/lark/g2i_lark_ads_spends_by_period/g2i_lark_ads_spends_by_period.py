import sys
import os
from queries import query
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

if __name__ == '__main__':
    setup_logger(__file__)

    spend_by_period = MariaDBHandler().read_from_db(query=query, output_type='dataframe')
    lark_client = LarkApiHandle()

    lark_client.overwrite_table(
        base_id='JqKfbPoYZaOLwOsDekHlwMLBgWb',
        table_id='tblcpUiwf7tzobNI',
        input_type='dataframe',
        df=spend_by_period
    )