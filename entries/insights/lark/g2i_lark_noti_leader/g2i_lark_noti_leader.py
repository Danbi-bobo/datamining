import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from queries import overview, detail_team, detail_market
from cdp.domain.utils.log_helper import setup_logger
import pandas as pd
import numpy as np

if __name__ == "__main__":
    setup_logger(__file__)

    base_id = 'IuUObFJacaDHbEs7ONEl6Bvegeb'
    detail_team_table_id = 'tblAvAzmKF58xNCH'
    detail_market_table_id = 'tblpooLzfLv11qdI'
    overview_table_id = 'tblHFdqSqOVHX6xI'

    lark_api_handler = LarkApiHandle()
    numeric_cols = ['Doanh số', 'Tỉ lệ chốt', 'Chi tiêu Ads', '%Ads']
    
    query_table_pairs = [
        (overview, overview_table_id),
        (detail_market, detail_market_table_id),
        (detail_team, detail_team_table_id)
    ]

    for query, table_id in query_table_pairs:
        df = MariaDBHandler().read_from_db(query=query, output_type='dataframe')

        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df['Ngày'] = pd.to_datetime(df['Ngày'])
        df['Ngày'] = df['Ngày'].astype('int64') // 10**6
        df.replace(np.nan, None, inplace=True)
        
        lark_api_handler.overwrite_table(base_id=base_id, table_id=table_id, input_type='dataframe', df=df)
