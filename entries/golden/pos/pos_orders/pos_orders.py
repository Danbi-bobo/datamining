import datetime
import sys
import os
import json
import numpy as np
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.domain.utils.config_helper import get_pos_shop_ids

golden_table_name = 'pos_orders'
raw_table_name = 'raw_' + golden_table_name

raw_columns = [
    "creator", "updated_at", "status", "fee_marketplace", "warehouse_info", 
    "order_sources_name", "order_sources","id", "note", "sub_status", "histories", "customer", "items_length", 
    "assigning_care_id", "money_to_collect", "inserted_at", "customer_pay_fee", "partner_fee", 
    "prepaid", "assigning_seller", "status_history", "cod", "bill_phone_number", "warehouse_id", 
    "bill_full_name", "returned_reason_name", "total_quantity", "assigning_seller_id", 
    "total_discount", "time_assign_care", "partner", "p_utm_source", "time_assign_seller", 
    "status_name", "shipping_address", "marketer", "transfer_money",
    "total_price_after_sub_discount", "pke_mkter", "time_send_partner", "total_price", 
    "account", "tags", "assigning_care", "items", "partner_account", 
    "partner_name", "first_delivery_at", "shop_id", "order_currency"
]

golden_columns = {
    "id", "shop_id", "status", "partner_name", "extend_code", "first_delivery_at","status_name","items", "transfer_money",
    "delivery_status", "warehouse_name", "partner_account", "total_price", "cod", "partner_fee", "order_sources_name", "total_discount", "bill_phone_number",
    "account", "time_assign_seller", "time_assign_care", "shipping_address", "bill_full_name", "order_sources", "partner",
    "warehouse_id", "assigning_seller_id", "assigning_care_id", "note", "tags", "reason", "marketer_id", "marketer_name", "marketer_email", "pke_mkter",
    "inserted_at", "updated_at", "province_id", "p_utm_source", "product", "returned_reason", "returned_reason_detail", "order_currency",
    "sent_time", "returned_time", "receive_time", "tags_id", "money_to_collect", "partial_return_time", "status_history", "total_price_after_sub_discount","fee_marketplace"
}

def load_config():
    """Đọc file config.json"""
    config_file = rf"{CDP_PATH}/entries/golden/pos/pos_orders/config.json"
    with open(config_file, 'r') as file:
        return json.load(file)

def update_last_run_time(end_time):
    """Cập nhật last_run trong config.json mà không ghi đè toàn bộ file."""
    config_file = rf"{CDP_PATH}/entries/golden/pos/pos_orders/config.json"

    new_value = end_time

    with open(config_file, 'r+') as file:
        config = json.load(file)
        config["last_run"] = new_value
        file.seek(0)
        json.dump(config, file, indent=4)
        file.truncate()
    logging.info(f"Update last_run inin config.json to '{new_value}'")

def extract_ids(tag_str):
    try:
        tags = json.loads(tag_str) if isinstance(tag_str, str) else []
        return [tag["id"] for tag in tags if isinstance(tag, dict) and "id" in tag]
    except json.JSONDecodeError:
        return []

def convert_all_dicts_to_json(df):
    """Tự động tìm và chuyển tất cả các giá trị kiểu dict/list thành JSON string."""
    return df.apply(lambda col: col.map(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x))

def parse_json(json_str):
    """Chuyển JSON string thành dictionary, nếu lỗi thì trả về None."""
    if isinstance(json_str, str):  # Kiểm tra nếu là string
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None
    return json_str  # Nếu đã là dict hoặc None thì giữ nguyên

def get_updated_at(history_list, old_status=None, new_status=None):
    """Tìm updated_at khi status chuyển từ old_status sang new_status."""
    if isinstance(history_list, str):  # Nếu là chuỗi, chuyển thành danh sách
        try:
            history_list = json.loads(history_list)
        except json.JSONDecodeError:
            return None  # Nếu lỗi thì trả về None

    if isinstance(history_list, list):
        for item in history_list:
            if isinstance(item, dict) and "status" in item and isinstance(item["status"], dict):
                old = item["status"].get("old")
                new = item["status"].get("new")
                if (old_status is None or old == old_status) and (new_status is None or new == new_status):
                    return item.get("updated_at")
    return None  # Nếu không có trạng thái phù hợp

def get_shop_orders(shop_id, api_key, raw_cols, start_time, end_time):
    pos_handler = PosAPIHandler(shop_id, api_key)
    orders = pos_handler.get_all(
        endpoint="orders",
        params={
            "page_number": 1, 
            "page_size": 200, 
            "startDateTime": start_time,
            "endDateTime":end_time, 
            "updateStatus":"updated_at"
        }
    )
    if not orders:
        logging.warning("No orders retrieved from API.")
        return pd.DataFrame()
    
    df = pd.DataFrame(orders)
    df = convert_all_dicts_to_json(df)
    df.replace(np.nan, None, inplace=True)
    
    df = df.loc[:, df.columns.intersection(raw_cols)]

    return df

def prepare_golden_df(df, golden_cols):
    numeric_cols = ["total_price","cod", "partner_fee","total_discount","money_to_collect", "transfer_money"]
    transformed_cols = ["extend_code", "warehouse_name", "shipping_address", "province_id", 
                        "sent_time", "returned_time", "receive_time", "collect_money_time", "partial_return_time"]

    df = df.copy()
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    df["partner"] = df["partner"].apply(parse_json)
    df["warehouse_info"] = df["warehouse_info"].apply(parse_json)
    df["shipping_address"] = df["shipping_address"].apply(parse_json)
    df["marketer"] = df["marketer"].apply(parse_json)

    # Trích xuất thông tin từ JSON
    df["extend_code"] = df["partner"].apply(lambda x: x.get("extend_code") if isinstance(x, dict) else None)
    df["partner_name"] = df["partner"].apply(lambda x: x.get("partner_name") if isinstance(x, dict) else None)
    df["first_delivery_at"] = df["partner"].apply(lambda x: x.get("first_delivery_at") if isinstance(x, dict) else None)
    df["warehouse_name"] = df["warehouse_info"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
    df["province_id"] = df["shipping_address"].apply(lambda x: x.get("province_id") if isinstance(x, dict) else None)
    df["shipping_address"] = df["shipping_address"].apply(lambda x: x.get("full_address") if isinstance(x, dict) else None)
    
    # Trích xuất thời gian từ histories
    df["sent_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status = None, new_status=2))
    df["returned_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=4, new_status=5))
    df["receive_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=2, new_status=3))
    df["collect_money_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=3, new_status=16))
    df["partial_return_time"] = df["histories"].apply(lambda x: get_updated_at(x, old_status=4, new_status=15))
    
    df["tags_id"] = df["tags"].apply(extract_ids)
    
    df["returned_reason"] = df["returned_reason_name"].apply(lambda x: x.split("/")[0] if isinstance(x, str) and "/" in x else x)
    df["returned_reason_detail"] = df["returned_reason_name"].apply(lambda x: x.split("/")[1] if isinstance(x, str) and "/" in x else None)
    df["marketer_id"] = df["marketer"].apply(lambda x: x['fb_id'] if isinstance(x, dict) else None)
    df["marketer_email"] = df["marketer"].apply(lambda x: x['email'] if isinstance(x, dict) else None)
    df["marketer_name"] = df["marketer"].apply(lambda x: x['name'] if isinstance(x, dict) else None)
    
    original_cols = [col for col in df.columns if col not in transformed_cols]
    df_transformed = df[original_cols + transformed_cols]
    
    df_transformed = convert_all_dicts_to_json(df_transformed)
    df_transformed = df_transformed.loc[:, df_transformed.columns.intersection(golden_cols)]
    df_transformed.rename(columns={'id': 'order_id'}, inplace=True, errors='ignore')
    df_transformed.rename(columns={'items': 'product'}, inplace=True, errors='ignore')
    df_transformed.rename(columns={'status_name': 'delivery_status'}, inplace=True, errors='ignore')
    df_transformed.replace(np.nan, None, inplace=True)
    df_transformed[['order_id', 'shop_id']] = df_transformed[['order_id', 'shop_id']].astype(str)

    return df_transformed
  
def get_orders_for_all_shops(shop_ids, raw_cols, start_time, end_time):
    dfs = []
    for shop_id in shop_ids:
        api_key = os.getenv(f"POS_API_KEY_{shop_id}")
        if shop_id and api_key:
            logging.info(f"Fetching orders for shop {shop_id}...")
            df = get_shop_orders(
                shop_id=shop_id, 
                api_key=api_key, 
                raw_cols=raw_cols, 
                start_time=start_time, 
                end_time=end_time
            )
            if df is not None and not df.empty:
                dfs.append(df)

    if not dfs:
        logging.warning("No dataframes collected from any shop.")
        return pd.DataFrame()
    
    final_df = pd.concat(dfs, ignore_index=True)
    
    return final_df

def main(start_time=None, end_time=None):
    if not end_time:
        end_time = int(datetime.datetime.now().timestamp())
    
    if not start_time:
        config = load_config()
        last_run = config.get("last_run", end_time - 20*60)
        start_time = last_run - 5 * 60

    shop_ids = get_pos_shop_ids()

    raw_df = get_orders_for_all_shops(
        shop_ids=shop_ids, 
        raw_cols=raw_columns, 
        start_time=start_time, 
        end_time=end_time
    )

    golden_df = prepare_golden_df(df=raw_df, golden_cols=golden_columns)
    if not raw_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            table=raw_table_name,
            df=raw_df,
            unique_columns=["shop_id","id"],
            db_type='raw'
        )

    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            table=golden_table_name,
            df=golden_df,
            unique_columns=["shop_id","order_id"],
            db_type='golden',
            log=True
        )

    update_last_run_time(end_time=end_time)
    logging.info("Orders processing completed.")

if __name__ == "__main__":
    setup_logger(__file__)
    main()