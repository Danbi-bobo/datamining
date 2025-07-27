import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from queries import query
from cdp.domain.utils.log_helper import setup_logger

if __name__ == "__main__":
    setup_logger(__file__)

    base_id = 'XFA4bClwka1T7es3xuMlaoTEg9f'
    table_id = 'tblWtykFsgG7JuEv'

    lark_client = LarkApiHandle()

    data = MariaDBHandler().read_from_db(query=query, output_type='dataframe')
    data['Nhân sự phụ trách hiện tại'] = data['Nhân sự phụ trách hiện tại'].apply(lambda x: [{'id': x}])
    
    lark_client.overwrite_table(
        base_id=base_id, 
        table_id=table_id, 
        input_type='dataframe', 
        df=data,
        params={
            'user_id_type': 'user_id'
        }
    )