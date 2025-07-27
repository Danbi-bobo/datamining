import sys
import os
import pandas as pd
import ast
from dotenv import load_dotenv
from numpy import nan
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from queries import new_ad_accounts_query
from cdp.domain.utils.log_helper import setup_logger

def update_status(row):
        if row["Trạng thái TKQC"] == "Mất quyền truy cập" and (pd.isna(row["Trạng thái mới"]) or row["Trạng thái mới"] == ""):
            return pd.Series([False, row["Trạng thái mới"]])
        
        elif row["Trạng thái TKQC"] != "Mất quyền truy cập" and (pd.isna(row["Trạng thái mới"]) or row["Trạng thái mới"] == ""):
            return pd.Series([True, "Mất quyền truy cập"])
        
        elif row["Trạng thái TKQC"] != row["Trạng thái mới"] and pd.notna(row["Trạng thái mới"]):
            return pd.Series([True, row["Trạng thái mới"]])
        
        return pd.Series([False, row["Trạng thái mới"]])

if __name__ == "__main__":
    setup_logger(__file__)

    ad_accounts_base_id = 'SFvnb4FlYaQmKssHsjIlyGbyguh'
    ad_accounts_table_id = 'tblpcusIffYlJOh4'
    token_base_id = 'A3lIbpE9Ba68p6sJ0AzlWcyMgxf'
    token_table_id = 'tblKL27drw9MI9S2'

    tokens_mapping_dict = {
        'Leader': {'path': 'Leader[0].id', 'type': 'lark_user'},
        'Team': {'path': 'Team', 'type': 'str'},
    }

    current_ad_accounts_mapping_dict = {
        'ID TKQC': {'path': 'ID TKQC', 'type': 'str'},
        'Trạng thái TKQC': {'path': 'Trạng thái TKQC', 'type': 'str'},
        'Team': {'path': 'Team', 'type': 'str'}
    }

    lark_api_handler = LarkApiHandle()
    
    tokens_df = lark_api_handler.extract_table_to_df(base_id=token_base_id, table_id=token_table_id, mapping_dict=tokens_mapping_dict, has_record_id=False)
    current_ad_accounts_df = lark_api_handler.extract_table_to_df(base_id=ad_accounts_base_id, table_id=ad_accounts_table_id, mapping_dict=current_ad_accounts_mapping_dict)
    new_ad_accounts_df = MariaDBHandler().read_from_db(query=new_ad_accounts_query, output_type='dataframe')

    if not current_ad_accounts_df.empty:
        result_df = current_ad_accounts_df.merge(new_ad_accounts_df, how='outer', on=['ID TKQC', 'Team'])
        result_df[["Thay đổi trạng thái", "Trạng thái mới"]] = result_df.apply(update_status, axis=1)
    else:
        result_df = new_ad_accounts_df.copy()
        result_df["record_id"] = None

    result_df["Quyền của Via với TKQC"] = result_df["Quyền của Via với TKQC"].map(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )

    result_df = result_df.merge(tokens_df, how='left', on=['Team'])
    result_df.replace(nan, None, inplace=True)

    create_df = result_df[result_df["record_id"].isna()].drop(columns=["record_id", "Thay đổi trạng thái"], errors='ignore')
    update_df = result_df[result_df["record_id"].notna()].drop(columns=["Tên TKQC"], errors='ignore')

    create_df.rename(columns={'Trạng thái mới': 'Trạng thái TKQC'}, inplace=True)

    if not create_df.empty:
        lark_api_handler.batch_create_from_df(base_id=ad_accounts_base_id, table_id=ad_accounts_table_id, df=create_df)

    if not update_df.empty:
        lark_api_handler.batch_update_from_df(base_id=ad_accounts_base_id, table_id=ad_accounts_table_id, df=update_df)
