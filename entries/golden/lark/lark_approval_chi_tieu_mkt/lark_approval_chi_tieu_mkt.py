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

def handle_assignee(df):
    df = df.copy()
    df = df.explode("assignee").reset_index(drop=True)
    df['assignee'] = df['assignee'].apply(lambda x: x['email'] if isinstance(x, dict) else None)

    return df

if __name__ == "__main__":
    setup_logger(__file__)

    base_id = 'PpjYb1Z9ca8XzzsOOp7lAG6Qghd'
    table_id = 'tblfTX67iZJnighB'
    golden_table_name = "lark_approval_chi_tieu_mkt"
    raw_table_name = "raw_" + golden_table_name

    mapping_dict = {
        'request_no': {'path': ['Request No.', 'text'], 'type': 'str'},
        'date': {'path': 'Submitted at', 'type': 'ms_timestamp'},
        'status': {'path': 'Status', 'type': 'str'},
        'team': {'path': 'Chi phí cho team nào?[0]', 'type': 'str'},
        'requester': {'path': 'Người đề nghị[0].email', 'type': 'str'},
        'assignee': {'path': 'Chi phí tính cho (những) ai?', 'type': ''},
        'amount': {'path': 'Số tiền', 'type': 'double'},
        'currency': {'path': 'Số tiền-Currency', 'type': 'str'},
        'expense_group_1': {'path': 'Loại chi phí', 'type': 'str'},
        'expense_group_2': {'path': 'Chi phí cụ thể', 'type': 'str'},
        'description': {'path': 'Nội dung đề nghị thanh toán', 'type': 'str'},
    }

    df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict, fields_return='all')
    df['team'] = df['team'].apply(lambda x: 'Team ' + x if not x.startswith('Team') else x)
    df = handle_assignee(df)
    
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
            unique_columns=["request_no", "assignee"], 
            log=True, 
            create_table=True, 
            mapping_dict=mapping_dict,
            db_type="golden"
        )