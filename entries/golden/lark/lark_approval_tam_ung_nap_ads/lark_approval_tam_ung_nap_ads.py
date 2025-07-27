import sys
import os
from numpy import nan
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

    base_id = 'KtX2bORHPa57TssgOnUlHoBIgme'
    table_id = 'tbliDn7p6Jr96Prn'
    golden_table_name = "lark_approval_tam_ung_nap_ads"
    raw_table_name = "raw_" + golden_table_name

    mapping_dict = {
        'request_no': {'path': ['Request No.', 'text'], 'type': 'str'},
        'status': {'path': 'Status', 'type': 'str'},
        'approval_process': {'path': 'Approval steps', 'type': 'str'},
        'team': {'path': 'Team Marketing', 'type': 'str'},
        'channel': {'path': 'Kênh chạy', 'type': 'str'},
        'currency': {'path': 'Loại tiền tệ', 'type': 'str'},
        'date': {'path': 'Ngày nạp', 'type': 'ms_timestamp'},
        'requester': {'path': 'Requester[0].email', 'type': 'str'},
        'account_name': {'path': 'Tên TK chạy Ads', 'type': 'str'},
        'amount': {'path': 'Số tiền thực nạp', 'type': 'double'},
    }

    df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict, fields_return='all')
    df['team'] = df['team'].apply(lambda x: 'Team ' + x if not x.startswith('Team') else x)

    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            table=raw_table_name, 
            df=df, 
            unique_columns=["record_id"],
            create_table=True, 
            mapping_dict=mapping_dict, 
            db_type="raw"
        )

        MariaDBHandler().insert_and_update_from_df(
            table=golden_table_name, 
            df=df, 
            unique_columns=["request_no"], 
            log=True, 
            create_table=True, 
            mapping_dict=mapping_dict,
            db_type="golden"
        )