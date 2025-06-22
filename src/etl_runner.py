# src/etl_runner.py
import pandas as pd
import os
from sqlalchemy import text
from .config import logger
from .db_utils import get_db_engine, load_df_to_db, fetch_distinct_business_entity_ids 
from .etl_pipelines import (
    etl_customers, etl_products,
    etl_order_items_from_reconciliation,
    etl_order_items_from_unstructured,
    etl_combine_orders_and_create_orders_table
)
from .main_etl import load_single_raw_data

def generate_current_id_maps_from_db(engine):
    existing_customer_ids = fetch_distinct_business_entity_ids(engine, 'Customers', 'customer_id')
    existing_product_ids = fetch_distinct_business_entity_ids(engine, 'Products', 'product_id')
    product_id_map_for_orders = {}
    try:
        df_prod_map_source = pd.read_sql_query(
            "SELECT DISTINCT source_item_id_int, product_id FROM Products WHERE source_item_id_int IS NOT NULL", engine)
        for _, row in df_prod_map_source.iterrows():
            product_id_map_for_orders[str(int(row['source_item_id_int']))] = row['product_id']
        for pid in existing_product_ids: # Ensure canonical IDs map to themselves
            product_id_map_for_orders.setdefault(str(pid), str(pid))
    except Exception as e: logger.error(f"Error generating product_id_map: {e}", exc_info=True)
    logger.info(f"Generated ID maps: {len(existing_customer_ids)} cust, {len(existing_product_ids)} prod, {len(product_id_map_for_orders)} prod_map.")
    return existing_customer_ids, existing_product_ids, product_id_map_for_orders

def process_and_load_customer_file(file_path, source_file_name_for_db, engine):
    logger.info(f"Processing customer file: {file_path} (source: {source_file_name_for_db})")
    df_raw = load_single_raw_data(file_path)
    if df_raw.empty: return False, "Raw data empty"
    df_cleaned = etl_customers(df_raw, source_file_name_for_db)
    if df_cleaned.empty: return False, "ETL resulted in empty data"
    try:
        load_df_to_db(df_cleaned, 'Customers', engine)
        return True, f"Loaded {len(df_cleaned)} customers"
    except Exception as e: return False, f"DB load error: {str(e)}"

def process_and_load_product_file(file_path, source_file_name_for_db, engine):
    logger.info(f"Processing product file: {file_path} (source: {source_file_name_for_db})")
    df_raw = load_single_raw_data(file_path)
    if df_raw.empty: return False, "Raw data empty"
    df_cleaned, _ = etl_products(df_raw, source_file_name_for_db)
    if df_cleaned.empty: return False, "ETL resulted in empty data"
    try:
        load_df_to_db(df_cleaned, 'Products', engine)
        return True, f"Loaded {len(df_cleaned)} products"
    except Exception as e: return False, f"DB load error: {str(e)}"

def process_and_load_order_file(file_path, source_file_name_for_db, entity_type, engine):
    logger.info(f"Processing order file: {file_path} (source: {source_file_name_for_db}, type: {entity_type})")
    df_raw = load_single_raw_data(file_path)
    if df_raw.empty: return False, "Raw data empty"
    
    cust_ids, prod_ids, prod_map = generate_current_id_maps_from_db(engine)
    df_items = pd.DataFrame()
    if entity_type == 'order_items_reconciliation':
        df_items = etl_order_items_from_reconciliation(df_raw, source_file_name_for_db, cust_ids, prod_ids, prod_map)
    elif entity_type == 'order_items_unstructured':
        df_items = etl_order_items_from_unstructured(df_raw, source_file_name_for_db, cust_ids, prod_ids, prod_map)
    else: return False, f"Unknown order entity type: {entity_type}"
    if df_items.empty: return False, "Order items ETL empty"

    try:
        # For combine, pass source_file_name associated with this batch of items
        df_final_items, df_final_orders = etl_combine_orders_and_create_orders_table([df_items], [source_file_name_for_db], cust_ids)
        if not df_final_orders.empty: load_df_to_db(df_final_orders, 'Orders', engine)
        if not df_final_items.empty: load_df_to_db(df_final_items, 'OrderItems', engine)
        return True, f"Loaded {len(df_final_orders)} orders, {len(df_final_items)} items"
    except Exception as e: return False, f"DB load error: {str(e)}"

def run_etl_for_registered_file(file_id, entity_type_override=None):
    engine = get_db_engine()
    file_info_df = pd.read_sql_query("SELECT file_name, file_path, entity_type_guess FROM SourceFileRegistry WHERE file_id = ?", engine, params=(int(file_id),)) # Ensure file_id is int
    if file_info_df.empty: return False, "File not found in registry"

    file_path = file_info_df['file_path'].iloc[0]
    source_file_name_for_db = file_info_df['file_name'].iloc[0] # Use actual file name as source identifier
    entity_type = entity_type_override if entity_type_override else file_info_df['entity_type_guess'].iloc[0]

    if not entity_type:
        message = f"Entity type not specified for file_id {file_id}. Cannot process."
        logger.error(message)
        try:
            with engine.connect() as conn:
                conn.execute(text("UPDATE SourceFileRegistry SET processing_status = 'error_entity_unknown', error_message = :err WHERE file_id = :fid"), {"err": message, "fid": file_id}); conn.commit()
        except Exception as e_db: logger.error(f"DB status update error: {e_db}")
        return False, message
    
    logger.info(f"Running ETL for file: {file_path} (source: {source_file_name_for_db}), entity: {entity_type}")
    success, message = False, "Processing not defined for entity type."
    new_status = 'error_processing'

    if entity_type.lower() == 'customer':
        success, message = process_and_load_customer_file(file_path, source_file_name_for_db, engine)
    elif entity_type.lower() == 'product':
        success, message = process_and_load_product_file(file_path, source_file_name_for_db, engine)
    elif entity_type.lower() in ['order_items_unstructured', 'order_items_reconciliation', 'order']:
        actual_order_type = entity_type.lower()
        if actual_order_type == 'order': # Basic heuristic if 'order' is passed generally
            actual_order_type = 'order_items_reconciliation' if 'recon' in source_file_name_for_db.lower() else 'order_items_unstructured'
        success, message = process_and_load_order_file(file_path, source_file_name_for_db, actual_order_type, engine)
    else:
        message = f"No ETL process defined for entity type: {entity_type}"
        logger.warning(message)

    new_status = 'processed' if success else 'error_processing'
    err_msg_for_db = None if success else message[:1000] # Limit error message length
    try:
        with engine.connect() as conn:
            conn.execute(text("UPDATE SourceFileRegistry SET processing_status = :st, last_processed_timestamp = CURRENT_TIMESTAMP, error_message = :em WHERE file_id = :fid"), {"st": new_status, "em": err_msg_for_db, "fid": file_id}); conn.commit()
    except Exception as e_db_upd: logger.error(f"DB status update error post-ETL: {e_db_upd}")
    return success, message

logger.info("ETL runner functions defined in src/etl_runner.py.")