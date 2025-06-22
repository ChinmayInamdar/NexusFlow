# src/main_etl.py
import pandas as pd
import os
import glob
from .config import (
    logger, DATA_DIR_RAW,
    CUSTOMERS_MESSY_JSON_ORIG, PRODUCTS_INCONSISTENT_JSON_ORIG,
    ORDERS_UNSTRUCTURED_CSV_ORIG, RECONCILIATION_DATA_CSV_ORIG,
    KNOWN_FILE_SOURCES_METADATA, CUSTOMERS_MESSY_JSON_ORIG_NAME,
    PRODUCTS_INCONSISTENT_JSON_ORIG_NAME, ORDERS_UNSTRUCTURED_CSV_ORIG_NAME,
    RECONCILIATION_DATA_CSV_ORIG_NAME
)
from .db_utils import get_db_engine, create_tables, load_df_to_db, fetch_existing_ids
from .etl_pipelines import (
    etl_customers, etl_products,
    etl_order_items_from_reconciliation,
    etl_order_items_from_unstructured,
    etl_combine_orders_and_create_orders_table # Make sure this name matches your definition
)

def load_single_raw_data(file_path, file_metadata=None):
    """Loads a single raw data file using pandas, with better error handling."""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()

    file_name = os.path.basename(file_path)
    # If file_metadata is not passed, try to get it from KNOWN_FILE_SOURCES_METADATA
    if file_metadata is None:
        file_metadata = KNOWN_FILE_SOURCES_METADATA.get(file_name, {})
    
    file_ext = os.path.splitext(file_name)[1].lower()
    parser_func_name = file_metadata.get('parser_func')
    
    if not parser_func_name: # Infer from extension if not in metadata
        if file_ext == '.json':
            parser_func_name = 'read_json'
        elif file_ext == '.csv':
            parser_func_name = 'read_csv'
        else:
            logger.error(f"Unsupported file extension '{file_ext}' for {file_path} without explicit parser_func in metadata.")
            return pd.DataFrame()
            
    try:
        if hasattr(pd, parser_func_name):
            parser_func = getattr(pd, parser_func_name)
            logger.info(f"Loading {file_path} using pandas.{parser_func_name}...")
            if file_ext == '.csv':
                return parser_func(file_path, low_memory=False)
            return parser_func(file_path)
        else:
            logger.error(f"Pandas has no parser '{parser_func_name}' for {file_path}")
            return pd.DataFrame()
    except FileNotFoundError: # Should be caught by os.path.exists, but good to have
        logger.error(f"File not found during pandas read: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading {file_path} using {parser_func_name}: {e}", exc_info=True)
        return pd.DataFrame()


def run_full_etl_pipeline(input_data_dir=DATA_DIR_RAW):
    logger.info(f"===== Starting Full ETL Pipeline from {input_data_dir} =====")
    engine = get_db_engine()
    create_tables(engine) # Clears and recreates tables

    # --- 1. Process Customers ---
    customer_file_path = os.path.join(input_data_dir, CUSTOMERS_MESSY_JSON_ORIG_NAME)
    customer_metadata = KNOWN_FILE_SOURCES_METADATA.get(CUSTOMERS_MESSY_JSON_ORIG_NAME)
    df_customers_raw = load_single_raw_data(customer_file_path, customer_metadata)
    
    df_customers_cleaned = pd.DataFrame()
    if not df_customers_raw.empty:
        df_customers_cleaned = etl_customers(df_customers_raw)
        if not df_customers_cleaned.empty:
            load_df_to_db(df_customers_cleaned, 'Customers', engine, if_exists='append') # Changed to append after create_tables
    existing_customer_ids = fetch_existing_ids(engine, 'Customers', 'customer_id')
    logger.info(f"Processed Customers. Loaded {len(existing_customer_ids)} distinct customers.")

    # --- 2. Process Products ---
    product_file_path = os.path.join(input_data_dir, PRODUCTS_INCONSISTENT_JSON_ORIG_NAME)
    product_metadata = KNOWN_FILE_SOURCES_METADATA.get(PRODUCTS_INCONSISTENT_JSON_ORIG_NAME)
    df_products_raw = load_single_raw_data(product_file_path, product_metadata)
    
    df_products_cleaned = pd.DataFrame()
    product_id_map_for_orders = {} # Initialize
    if not df_products_raw.empty:
        df_products_cleaned, product_id_map_for_orders = etl_products(df_products_raw)
        if not df_products_cleaned.empty:
            load_df_to_db(df_products_cleaned, 'Products', engine, if_exists='append')
    existing_product_ids = fetch_existing_ids(engine, 'Products', 'product_id')
    logger.info(f"Processed Products. Loaded {len(existing_product_ids)} distinct products. Mapping dict size: {len(product_id_map_for_orders)}")

    # --- 3. Process Order Item Files ---
    all_processed_order_items_dfs = []
    
    # Using the specific file names from config for this iteration
    order_files_to_process_info = [
        (RECONCILIATION_DATA_CSV_ORIG_NAME, RECONCILIATION_DATA_CSV_ORIG, 'order_items_reconciliation'),
        (ORDERS_UNSTRUCTURED_CSV_ORIG_NAME, ORDERS_UNSTRUCTURED_CSV_ORIG, 'order_items_unstructured')
    ]

    for file_name, file_path, entity_type in order_files_to_process_info:
        logger.info(f"Processing order file: {file_path} (name: {file_name}) for entity type: {entity_type}")
        metadata = KNOWN_FILE_SOURCES_METADATA.get(file_name)
        if not metadata:
            logger.warning(f"No metadata for {file_name}, skipping.")
            continue
        
        df_raw_orders = load_single_raw_data(file_path, metadata)
        if df_raw_orders.empty:
            logger.warning(f"Raw order data empty for {file_name}, skipping.")
            continue

        df_processed_items = pd.DataFrame()
        if entity_type == 'order_items_reconciliation':
            df_processed_items = etl_order_items_from_reconciliation(
                df_raw_orders, existing_customer_ids, existing_product_ids, product_id_map_for_orders
            )
        elif entity_type == 'order_items_unstructured':
            df_processed_items = etl_order_items_from_unstructured(
                df_raw_orders, existing_customer_ids, existing_product_ids, product_id_map_for_orders
            )
        else:
            logger.warning(f"Unknown order entity type: {entity_type} for file {file_name}")
            continue
        
        if df_processed_items is not None and not df_processed_items.empty:
            logger.info(f"Successfully processed {file_name}. Shape: {df_processed_items.shape}")
            all_processed_order_items_dfs.append(df_processed_items)
        else:
            logger.warning(f"ETL for {file_name} resulted in an empty or None DataFrame.")

    # --- 4. Combine and Load Final Orders and OrderItems ---
    if all_processed_order_items_dfs:
        logger.info(f"Combining {len(all_processed_order_items_dfs)} processed order item DataFrames.")
        df_final_order_items, df_final_orders = etl_combine_orders_and_create_orders_table(
            all_processed_order_items_dfs,
            existing_customer_ids # Pass the set of valid customer IDs for FK check on Orders
        )
        if not df_final_orders.empty:
            # If create_tables already cleared, we can append.
            # If running this part multiple times without re-running create_tables, handle if_exists carefully.
            # For this script, create_tables is run once at the start.
            load_df_to_db(df_final_orders, 'Orders', engine, if_exists='append')
        else:
            logger.warning("Final Orders DataFrame is empty after combining. Nothing to load to Orders table.")

        if not df_final_order_items.empty:
            load_df_to_db(df_final_order_items, 'OrderItems', engine, if_exists='append')
        else:
            logger.warning("Final OrderItems DataFrame is empty after combining. Nothing to load to OrderItems table.")
    else:
        logger.warning("No order item data was processed. Orders and OrderItems tables will be empty.")

    logger.info("===== Full ETL Pipeline Finished =====")

if __name__ == '__main__':
    run_full_etl_pipeline()