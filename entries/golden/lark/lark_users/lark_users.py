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
setup_logger(__file__)

base_id = 'D5j1beUpjaRbslsdgVBld3b7gbd'
table_id = 'tblxXblGc4PUp6NH'
db_golden_name = os.getenv("DB_GOLDEN_NAME")
golden_table_name = "lark_users"
db_raw_name = os.getenv("DB_RAW_NAME")
raw_table_name = "raw_" + golden_table_name

mapping_dict = {
    'employee_name': {'path': 'employee_name', 'type': 'str'},
    'user_id': {'path': 'user_id', 'type': 'str'},
    'enterprise_email': {'path': 'enterprise_email', 'type': 'str'},
    'department': {'path': 'department[0].text', 'type': 'str'},
    'status': {'path': 'status', 'type': 'str'},
    'mobile': {'path': 'mobile', 'type': 'str'},
    'job_title': {'path': 'job_title', 'type': 'str'},
    'gender': {'path': 'gender', 'type': 'str'},
    'avatar_url': {'path': 'user[0].avatar_url', 'type': 'str'},
}

df = LarkApiHandle().extract_table_to_df(base_id=base_id, table_id=table_id, mapping_dict=mapping_dict)
df["avatar_url"] = df["avatar_url"].str.replace(r"image_size=\d+x\d+", "image_size=500x500", regex=True)

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
        unique_columns=["user_id"], 
        log=True, 
        create_table=True, 
        mapping_dict=mapping_dict,
        db_type="golden"
    )