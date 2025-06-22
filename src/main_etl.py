# src/main_etl.py
import pandas as pd
import os
from .config import (
    logger, DATA_DIR_RAW,
    CUSTOMERS_MESSY_JSON_ORIG, PRODUCTS_INCONSISTENT_JSON_ORIG,
    ORDERS_UNSTRUCTURED_CSV_ORIG, RECONCILIATION_DATA_CSV_ORIG,
    KNOWN_FILE_SOURCES_METADATA, CUSTOMERS_MESSY_JSON_ORIG_NAME,
    PRODUCTS_INCONSISTENT_JSON_ORIG_NAME, ORDERS_UNSTRUCTURED_CSV_ORIG_NAME,
    RECONCILIATION_DATA_CSV_ORIG_NAME
)
from .db_utils import get_db_engine, create_tables, load_df_to_db, fetch_distinct_business_entity_ids
from .etl_pipelines import (
    etl_customers, etl_products,
    etl_order_items_from_reconciliation,
    etl_order_items_from_unstructured,
    etl_combine_orders_and_create_orders_table
)

def load_single_raw_data(file_path, file_metadata=None):
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    file_name = os.path.basename(file_path)
    if file_metadata is None:
        file_metadata = KNOWN_FILE_SOURCES_METADATA.get(file_name, {})
    file_ext = os.path.splitext(file_name)[1].lower()
    parser_func_name = file_metadata.get('parser_func')
    if not parser_func_name:
        if file_ext == '.json': parser_func_name = 'read_json'
        elif file_ext == '.csv': parser_func_name = 'read_csv'
        else: logger.error(f"Unsupported file ext '{file_ext}' for {file_path}"); return pd.DataFrame()
    try:
        if hasattr(pd, parser_func_name):
            parser_func = getattr(pd, parser_func_name)
            logger.info(f"Loading {file_path} using pandas.{parser_func_name}...")
            if file_ext == '.csv': return parser_func(file_path, low_memory=False)
            return parser_func(file_path)
        else: logger.error(f"Pandas has no parser '{parser_func_name}' for {file_path}"); return pd.DataFrame()
    except Exception as e: logger.error(f"Error loading {file_path}: {e}", exc_info=True); return pd.DataFrame()


def run_full_etl_pipeline(input_data_dir=DATA_DIR_RAW):
    logger.info(f"===== Starting Full ETL Pipeline from {input_data_dir} =====")
    engine = get_db_engine()
    create_tables(engine) 

    # --- 1. Process Customers ---
    df_customers_raw = load_single_raw_data(CUSTOMERS_MESSY_JSON_ORIG)
    if not df_customers_raw.empty:
        df_customers_cleaned = etl_customers(df_customers_raw, CUSTOMERS_MESSY_JSON_ORIG_NAME) # Pass source file name
        if not df_customers_cleaned.empty:
            load_df_to_db(df_customers_cleaned, 'Customers', engine)
    existing_customer_ids = fetch_distinct_business_entity_ids(engine, 'Customers', 'customer_id')
    logger.info(f"Processed Customers. Distinct business customers: {len(existing_customer_ids)}.")

    # --- 2. Process Products ---
    df_products_raw = load_single_raw_data(PRODUCTS_INCONSISTENT_JSON_ORIG)
    product_id_map_for_orders = {}
    if not df_products_raw.empty:
        df_products_cleaned, product_id_map_for_orders = etl_products(df_products_raw, PRODUCTS_INCONSISTENT_JSON_ORIG_NAME) # Pass source
        if not df_products_cleaned.empty:
            load_df_to_db(df_products_cleaned, 'Products', engine)
    existing_product_ids = fetch_distinct_business_entity_ids(engine, 'Products', 'product_id')
    logger.info(f"Processed Products. Distinct business products: {len(existing_product_ids)}. Order map size: {len(product_id_map_for_orders)}")

    # --- 3. Process Order Item Files ---
    all_processed_order_items_dfs = []
    source_file_names_for_combine = [] # To pass to combine if needed

    order_files_info = [
        (RECONCILIATION_DATA_CSV_ORIG_NAME, RECONCILIATION_DATA_CSV_ORIG, 'order_items_reconciliation'),
        (ORDERS_UNSTRUCTURED_CSV_ORIG_NAME, ORDERS_UNSTRUCTURED_CSV_ORIG, 'order_items_unstructured')
    ]

    for file_name, file_path, entity_type in order_files_info:
        df_raw_orders = load_single_raw_data(file_path)
        if df_raw_orders.empty: continue
        df_processed_items = pd.DataFrame()
        if entity_type == 'order_items_reconciliation':
            df_processed_items = etl_order_items_from_reconciliation(
                df_raw_orders, file_name, existing_customer_ids, existing_product_ids, product_id_map_for_orders # Pass file_name
            )
        elif entity_type == 'order_items_unstructured':
            df_processed_items = etl_order_items_from_unstructured(
                df_raw_orders, file_name, existing_customer_ids, existing_product_ids, product_id_map_for_orders # Pass file_name
            )
        if not df_processed_items.empty:
            all_processed_order_items_dfs.append(df_processed_items)
            source_file_names_for_combine.append(file_name)


    # --- 4. Combine and Load Final Orders and OrderItems ---
    if all_processed_order_items_dfs:
        df_final_order_items, df_final_orders = etl_combine_orders_and_create_orders_table(
            all_processed_order_items_dfs,
            source_file_names_for_combine, # Pass list of source file names
            existing_customer_ids 
        )
        if not df_final_orders.empty: load_df_to_db(df_final_orders, 'Orders', engine)
        if not df_final_order_items.empty: load_df_to_db(df_final_order_items, 'OrderItems', engine)
    else:
        logger.warning("No order item data processed. Orders/OrderItems empty.")
    logger.info("===== Full ETL Pipeline Finished =====")

if __name__ == '__main__':
    run_full_etl_pipeline()