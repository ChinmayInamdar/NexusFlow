# src/etl_pipelines.py
import pandas as pd
import numpy as np
import re
from datetime import datetime

from .config import (
    logger, DEFAULT_UNKNOWN_CATEGORICAL, DEFAULT_UNKNOWN_NUMERIC_INT,
    DEFAULT_UNKNOWN_NUMERIC_FLOAT, DEFAULT_STATUS_UNKNOWN,
    GENDER_MAP, CUSTOMER_STATUS_MAP, PAYMENT_STATUS_MAP,
    ORDER_DELIVERY_STATUS_MAP, STATE_ABBREVIATION_MAP, CITY_NORMALIZATION_MAP,
    RECONCILIATION_DATA_CSV_ORIG_NAME, ORDERS_UNSTRUCTURED_CSV_ORIG_NAME
)
from .data_processing_utils import (
    clean_string, standardize_categorical, parse_date_robustly,
    to_numeric_safe, standardize_boolean_strict, standardize_phone_strict,
    standardize_postal_code, get_current_timestamp_str
)

def etl_customers(df_raw_cust):
    logger.info("Starting Customers ETL process...")
    if df_raw_cust.empty:
        logger.warning("Raw customer DataFrame is empty. Skipping ETL.")
        return pd.DataFrame()

    df = df_raw_cust.copy()
    pipeline_timestamp = get_current_timestamp_str()

    df.reset_index(drop=True, inplace=True)
    df['_original_index_for_missing_id'] = df.index.astype(str)

    # 1. ID Unification and Source ID
    df['source_customer_id_int_val'] = df.get('customer_id', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=int))
    
    if 'customer_id' in df.columns and 'cust_id' in df.columns :
        df.rename(columns={'customer_id': '_original_int_customer_id_temp'}, inplace=True, errors='ignore')

    df['customer_id_canon'] = df['cust_id'].apply(lambda x: clean_string(x, case='upper'))
    missing_cust_id_mask = df['customer_id_canon'].isnull()
    temp_source_id_str = df.loc[missing_cust_id_mask, 'source_customer_id_int_val'].apply(
        lambda x: str(int(x)) if pd.notna(x) else "NO_INT_ID"
    ).astype(str)
    df.loc[missing_cust_id_mask, 'customer_id_canon'] = 'CUST_UNKNOWN_' + temp_source_id_str + "_" + df.loc[missing_cust_id_mask, '_original_index_for_missing_id']
    
    cols_to_drop_intermediate = ['_original_int_customer_id_temp', 'cust_id', '_original_index_for_missing_id']

    # 2. Name: Coalesce and standardize
    def choose_name(row):
        cn = str(row.get('customer_name', ''))
        fn = str(row.get('full_name', ''))
        is_cn_username = bool(re.match(r'^[a-z\._\-0-9@]+$', cn.lower())) and ('@' in cn or ('.' in cn and not ' ' in cn) or '_' in cn or cn.count(' ') == 0)
        is_fn_username = bool(re.match(r'^[a-z\._\-0-9@]+$', fn.lower())) and ('@' in fn or ('.' in fn and not ' ' in fn) or '_' in fn or fn.count(' ') == 0)
        if pd.notna(row.get('customer_name')) and cn.strip() and (not is_cn_username or ' ' in cn): return clean_string(cn, 'title')
        if pd.notna(row.get('full_name')) and fn.strip() and (not is_fn_username or ' ' in fn): return clean_string(fn, 'title')
        if pd.notna(row.get('customer_name')) and cn.strip(): return clean_string(cn, 'title')
        if pd.notna(row.get('full_name')) and fn.strip(): return clean_string(fn, 'title')
        return DEFAULT_UNKNOWN_CATEGORICAL
    df['customer_name_final'] = df.apply(choose_name, axis=1)
    cols_to_drop_intermediate.extend(['customer_name', 'full_name'])

    # 3. Email: Coalesce, clean, handle empty.
    df['email_temp'] = df.get('email', pd.Series(dtype=object)).replace('', pd.NA).apply(lambda x: clean_string(x, 'lower') if pd.notna(x) else None)
    df['email_address_temp'] = df.get('email_address', pd.Series(dtype=object)).replace('', pd.NA).apply(lambda x: clean_string(x, 'lower') if pd.notna(x) else None)
    df['email_final'] = df['email_temp'].fillna(df['email_address_temp'])
    df.loc[df['email_final'] == '', 'email_final'] = None
    cols_to_drop_intermediate.extend(['email', 'email_address', 'email_temp', 'email_address_temp'])

    # 4. Phone: Coalesce and standardize
    df['phone_temp'] = df.get('phone', pd.Series(dtype=object)).replace('', pd.NA)
    df['phone_number_temp'] = df.get('phone_number', pd.Series(dtype=object)).replace('', pd.NA)
    df['phone_final'] = df['phone_number_temp'].fillna(df['phone_temp']).apply(standardize_phone_strict)
    cols_to_drop_intermediate.extend(['phone', 'phone_number', 'phone_temp', 'phone_number_temp'])

    # 5. Address
    df['address_street_final'] = df.get('address', pd.Series(dtype=object)).apply(lambda x: clean_string(x, 'title'))
    df['address_city_cleaned'] = df.get('city', pd.Series(dtype=object)).apply(lambda x: clean_string(x, case='upper'))
    df['address_city_final'] = df['address_city_cleaned'].apply(
        lambda x: CITY_NORMALIZATION_MAP.get(x, x.title() if pd.notna(x) and x else DEFAULT_UNKNOWN_CATEGORICAL) if pd.notna(x) and x else DEFAULT_UNKNOWN_CATEGORICAL
    )
    df['address_state_cleaned'] = df.get('state', pd.Series(dtype=object)).apply(lambda x: clean_string(x, case='upper'))
    df['address_state_final'] = df['address_state_cleaned'].apply(
        lambda x: STATE_ABBREVIATION_MAP.get(x, DEFAULT_UNKNOWN_CATEGORICAL if pd.isna(x) or not x else x) if pd.notna(x) and x else DEFAULT_UNKNOWN_CATEGORICAL
    )
    df['postal_code_temp'] = df.get('postal_code', pd.Series(dtype=object)).replace('', pd.NA).fillna(df.get('zip_code', pd.Series(dtype=object)).replace('', pd.NA))
    df['address_postal_code_final'] = df['postal_code_temp'].apply(standardize_postal_code)
    cols_to_drop_intermediate.extend(['address', 'city', 'state', 'zip_code', 'postal_code', 
                                     'address_city_cleaned', 'address_state_cleaned', 'postal_code_temp'])

    # 6. Dates
    df['reg_date_temp'] = df.get('reg_date', pd.Series(dtype=object)).replace('', pd.NA)
    df['registration_date_temp'] = df.get('registration_date', pd.Series(dtype=object)).replace('', pd.NA)
    df['registration_date_final'] = df['registration_date_temp'].fillna(df['reg_date_temp']).apply(parse_date_robustly)
    df['birth_date_final'] = df.get('birth_date', pd.Series(dtype=object)).apply(parse_date_robustly)
    cols_to_drop_intermediate.extend(['reg_date', 'registration_date', 'birth_date', 
                                     'reg_date_temp', 'registration_date_temp'])

    # 7. Status
    df['status_temp'] = df.get('status', pd.Series(dtype=object)).replace('', pd.NA).replace(' ', pd.NA).replace('None', pd.NA)
    df['customer_status_temp'] = df.get('customer_status', pd.Series(dtype=object)).replace('', pd.NA).replace(' ', pd.NA).replace('None', pd.NA)
    df['status_coalesced'] = df['customer_status_temp'].fillna(df['status_temp'])
    df['status_cleaned_for_map'] = df['status_coalesced'].apply(lambda x: clean_string(x, case='upper'))
    df['status_final'] = df['status_cleaned_for_map'].apply(lambda x: CUSTOMER_STATUS_MAP.get(x, DEFAULT_STATUS_UNKNOWN) if pd.notna(x) else DEFAULT_STATUS_UNKNOWN)
    cols_to_drop_intermediate.extend(['status', 'customer_status', 'status_temp', 'customer_status_temp', 
                                     'status_coalesced', 'status_cleaned_for_map'])
    
    # 8. Numeric
    df['total_spent_final'] = df.get('total_spent', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['total_orders_final'] = df.get('total_orders', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=0))
    df['loyalty_points_final'] = df.get('loyalty_points', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=0))
    cols_to_drop_intermediate.extend(['total_spent', 'total_orders', 'loyalty_points'])

    # 9. Age
    def calculate_age(birth_date_str):
        if pd.isna(birth_date_str): return pd.NA
        try:
            birth_dt = datetime.strptime(birth_date_str, '%Y-%m-%d')
            today = datetime.today()
            return today.year - birth_dt.year - ((today.month, today.day) < (birth_dt.month, birth_dt.day))
        except (ValueError, TypeError): return pd.NA
    df['age_calculated'] = df['birth_date_final'].apply(calculate_age)
    df['age_provided_numeric'] = df.get('age', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=pd.NA))
    df['age_final'] = df['age_calculated'].fillna(df['age_provided_numeric']).astype('Int64')
    cols_to_drop_intermediate.extend(['age', 'age_calculated', 'age_provided_numeric'])

    # 10. Gender
    df['gender_cleaned'] = df.get('gender', pd.Series(dtype=object)).apply(lambda x: clean_string(x, case='upper'))
    df['gender_final'] = df['gender_cleaned'].apply(lambda x: GENDER_MAP.get(x, DEFAULT_UNKNOWN_CATEGORICAL) if pd.notna(x) else DEFAULT_UNKNOWN_CATEGORICAL)
    cols_to_drop_intermediate.extend(['gender', 'gender_cleaned'])

    # 11. Segment & Payment Method
    df['segment_final'] = df.get('segment', pd.Series(dtype=object)).apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    df['preferred_payment_method_final'] = df.get('preferred_payment', pd.Series(dtype=object)).apply(lambda x: clean_string(x, 'lower', DEFAULT_UNKNOWN_CATEGORICAL))
    cols_to_drop_intermediate.extend(['segment', 'preferred_payment'])
    
    # Drop the original and intermediate columns that have been processed
    actual_cols_to_drop = [col for col in cols_to_drop_intermediate if col in df.columns]
    df.drop(columns=actual_cols_to_drop, inplace=True, errors='ignore')

    # Rename final columns to target schema names
    df_final_customers = df.rename(columns={
        'customer_id_canon': 'customer_id',
        'customer_name_final': 'customer_name',
        'email_final': 'email',
        'phone_final': 'phone',
        'address_street_final': 'address_street',
        'address_city_final': 'address_city',
        'address_state_final': 'address_state',
        'address_postal_code_final': 'address_postal_code',
        'registration_date_final': 'registration_date',
        'status_final': 'status',
        'total_orders_final': 'total_orders',
        'total_spent_final': 'total_spent',
        'loyalty_points_final': 'loyalty_points',
        'preferred_payment_method_final': 'preferred_payment_method',
        'birth_date_final': 'birth_date',
        'age_final': 'age',
        'gender_final': 'gender',
        'segment_final': 'segment',
        'source_customer_id_int_val': 'source_customer_id_int'
    })

    target_columns = [
        'customer_id', 'customer_name', 'email', 'phone',
        'address_street', 'address_city', 'address_state', 'address_postal_code',
        'registration_date', 'status', 'total_orders', 'total_spent',
        'loyalty_points', 'preferred_payment_method', 'birth_date', 'age', 'gender',
        'segment', 'source_customer_id_int'
    ]
    
    for col in target_columns:
        if col not in df_final_customers.columns:
            if col in ['age', 'source_customer_id_int','total_orders','loyalty_points']: df_final_customers[col] = pd.NA
            elif col == 'total_spent': df_final_customers[col] = np.nan
            else: df_final_customers[col] = None
            logger.warning(f"Column '{col}' was missing in Customers final df after rename, added as NA/None.")
    
    if df_final_customers.columns.duplicated().any():
        logger.error(f"CRITICAL: Duplicate column names STILL found before selecting target columns: {df_final_customers.columns[df_final_customers.columns.duplicated()].tolist()}")
        # Force unique columns by taking the first occurrence if this still happens
        df_final_customers = df_final_customers.loc[:, ~df_final_customers.columns.duplicated()]
        logger.info("Attempted to fix duplicate columns by taking first occurrence.")


    df_final_customers = df_final_customers[target_columns].copy()
    
    df_final_customers.dropna(subset=['customer_id'], inplace=True)
    logger.info(f"Customers after column selection and NA drop on customer_id: {df_final_customers.shape[0]} rows.")
    
    df_final_customers['source_customer_id_int_sort'] = df_final_customers['source_customer_id_int'].fillna(999999999)
    df_final_customers.sort_values(by=['customer_id', 'source_customer_id_int_sort'], inplace=True)
    df_final_customers.drop_duplicates(subset=['customer_id'], keep='first', inplace=True)
    df_final_customers.drop(columns=['source_customer_id_int_sort'], inplace=True)
    
    df_final_customers.reset_index(drop=True, inplace=True)
    logger.info(f"Customers after dropping duplicates on customer_id and resetting index: {df_final_customers.shape[0]} rows.")

    if 'email' in df_final_customers.columns:
        temp_df = df_final_customers.copy()
        temp_df['_has_valid_email'] = temp_df['email'].notna() & (temp_df['email'].astype(str).str.strip() != '')
        
        df_with_valid_email = temp_df[temp_df['_has_valid_email']].copy()
        df_without_valid_email = temp_df[~temp_df['_has_valid_email']].copy()
        
        if not df_with_valid_email.empty:
            df_with_valid_email.sort_values(by=['email', 'customer_id'], inplace=True)
            df_with_valid_email_deduped = df_with_valid_email.drop_duplicates(subset=['email'], keep='first')
            df_final_customers = pd.concat([df_with_valid_email_deduped, df_without_valid_email], ignore_index=True)
        else:
            df_final_customers = df_without_valid_email.copy() 
        
        if '_has_valid_email' in df_final_customers.columns:
            df_final_customers.drop(columns=['_has_valid_email'], inplace=True)
        
        df_final_customers.reset_index(drop=True, inplace=True)
        logger.info(f"Customers after newest email deduplication: {df_final_customers.shape[0]} rows.")

    df_final_customers['last_updated_pipeline'] = pipeline_timestamp
    return df_final_customers


def etl_products(df_raw_prod):
    logger.info("Starting Products ETL process...")
    if df_raw_prod.empty:
        logger.warning("Raw product DataFrame is empty. Skipping ETL.")
        return pd.DataFrame(), {}

    df = df_raw_prod.copy()
    pipeline_timestamp = get_current_timestamp_str()
    product_id_mapping_dict_local = {}

    # Store original item_id and product_id before they might be dropped or renamed
    df['source_item_id_int_val'] = df.get('item_id', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=int))
    
    # If 'product_id' (original from raw) exists, rename it to avoid conflict
    if 'product_id' in df.columns:
        df.rename(columns={'product_id': '_original_raw_product_id'}, inplace=True, errors='ignore')

    df['product_id_canon'] = df.get('_original_raw_product_id', pd.Series(index=df.index, dtype='object')).apply(lambda x: clean_string(x, 'upper'))

    cols_to_drop_intermediate = ['item_id', '_original_raw_product_id']

    df['product_name_final'] = df.get('product_name', pd.Series(index=df.index, dtype='object')).fillna(df.get('item_name', pd.Series(index=df.index, dtype='object'))).apply(lambda x: clean_string(x, 'title'))
    cols_to_drop_intermediate.extend(['product_name', 'item_name'])

    df['description_final'] = df.get('description', pd.Series(index=df.index, dtype='object')).apply(lambda x: clean_string(x) if pd.notna(x) else "No description available")
    cols_to_drop_intermediate.append('description')

    df['category_temp'] = df.get('category', pd.Series(index=df.index, dtype='object')).fillna(df.get('product_category', pd.Series(index=df.index, dtype='object'))).apply(lambda x: clean_string(x, 'title'))
    df['category_final'] = df['category_temp'].fillna(DEFAULT_UNKNOWN_CATEGORICAL)
    cols_to_drop_intermediate.extend(['category', 'product_category', 'category_temp'])

    brand_series = df.get('brand', pd.Series(index=df.index, dtype='object')).replace('', pd.NA)
    manufacturer_series = df.get('manufacturer', pd.Series(index=df.index, dtype='object')).replace('', pd.NA)
    df['brand_temp'] = brand_series.fillna(manufacturer_series)
    df['brand_final'] = df['brand_temp'].apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    manufacturer_fallback = df['brand_final'].replace(DEFAULT_UNKNOWN_CATEGORICAL, pd.NA) if 'brand_final' in df.columns else pd.NA
    df['manufacturer_temp'] = manufacturer_series.fillna(manufacturer_fallback)
    df['manufacturer_final'] = df['manufacturer_temp'].apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    cols_to_drop_intermediate.extend(['brand', 'manufacturer', 'brand_temp', 'manufacturer_temp'])

    df['price_final'] = df.get('price', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=DEFAULT_UNKNOWN_NUMERIC_FLOAT))
    df['cost_final'] = df.get('cost', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=DEFAULT_UNKNOWN_NUMERIC_FLOAT))
    df['weight_kg_final'] = df.get('weight', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=float)) 
    df['rating_final'] = df.get('rating', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=float))
    cols_to_drop_intermediate.extend(['price', 'cost', 'weight', 'rating'])

    def parse_dimensions_strict(dim_str):
        if pd.isna(dim_str) or str(dim_str).strip() == '': return None, None, None
        parts = [p.strip() for p in re.split(r'[xX]', str(dim_str).lower())]
        if len(parts) == 3:
            l, w, h = to_numeric_safe(parts[0]), to_numeric_safe(parts[1]), to_numeric_safe(parts[2])
            return (l if l is not None else np.nan, w if w is not None else np.nan, h if h is not None else np.nan)
        return np.nan, np.nan, np.nan
    dims_parsed = df.get('dimensions', pd.Series(index=df.index, dtype='object')).apply(parse_dimensions_strict)
    df['dim_length_cm_final'] = dims_parsed.apply(lambda x: x[0] if isinstance(x, tuple) else np.nan).astype('Float64')
    df['dim_width_cm_final'] = dims_parsed.apply(lambda x: x[1] if isinstance(x, tuple) else np.nan).astype('Float64')
    df['dim_height_cm_final'] = dims_parsed.apply(lambda x: x[2] if isinstance(x, tuple) else np.nan).astype('Float64')
    cols_to_drop_intermediate.append('dimensions')
    
    df['color_final'] = df.get('color', pd.Series(index=df.index, dtype='object')).replace('', pd.NA).apply(lambda x: clean_string(x, 'title', DEFAULT_UNKNOWN_CATEGORICAL))
    df['size_final'] = df.get('size', pd.Series(index=df.index, dtype='object')).replace('', pd.NA).apply(lambda x: clean_string(x, 'upper', 'N/A')) 
    cols_to_drop_intermediate.extend(['color', 'size'])

    df['stock_quantity_final'] = df.get('stock_quantity', pd.Series(index=df.index, dtype='object')).fillna(df.get('stock_level', pd.Series(index=df.index, dtype='object'))).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=DEFAULT_UNKNOWN_NUMERIC_INT))
    df['reorder_level_final'] = df.get('reorder_level', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=DEFAULT_UNKNOWN_NUMERIC_INT))
    cols_to_drop_intermediate.extend(['stock_quantity', 'stock_level', 'reorder_level'])
    
    df['supplier_id_final'] = df.get('supplier_id', pd.Series(index=df.index, dtype='object')).apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    cols_to_drop_intermediate.append('supplier_id')
    
    df['is_active_final'] = df.get('is_active', pd.Series(index=df.index, dtype='object')).apply(standardize_boolean_strict)
    cols_to_drop_intermediate.append('is_active')

    df['product_created_date_final'] = df.get('created_date', pd.Series(index=df.index, dtype='object')).apply(lambda x: parse_date_robustly(x))
    df['product_last_updated_source_final'] = df.get('last_updated', pd.Series(index=df.index, dtype='object')).apply(lambda x: parse_date_robustly(x, output_format='%Y-%m-%d %H:%M:%S'))
    cols_to_drop_intermediate.extend(['created_date', 'last_updated'])
    
    actual_cols_to_drop = [col for col in cols_to_drop_intermediate if col in df.columns]
    df.drop(columns=actual_cols_to_drop, inplace=True, errors='ignore')

    if not df.empty:
        for _, row in df.iterrows():
            prod_id_canon = row.get('product_id_canon')
            src_item_id = row.get('source_item_id_int_val')
            if pd.notna(src_item_id) and pd.notna(prod_id_canon):
                product_id_mapping_dict_local[str(int(src_item_id))] = prod_id_canon
            if pd.notna(prod_id_canon):
                product_id_mapping_dict_local.setdefault(str(prod_id_canon), prod_id_canon)
    logger.info(f"Local product ID mapping dictionary created with {len(product_id_mapping_dict_local)} entries during product ETL.")

    df_final_products = df.rename(columns={
        'product_id_canon': 'product_id',
        'product_name_final': 'product_name',
        'description_final': 'description',
        'category_final': 'category',
        'brand_final': 'brand',
        'manufacturer_final': 'manufacturer',
        'price_final': 'price',
        'cost_final': 'cost',
        'weight_kg_final': 'weight_kg',
        'dim_length_cm_final': 'dim_length_cm',
        'dim_width_cm_final': 'dim_width_cm',
        'dim_height_cm_final': 'dim_height_cm',
        'color_final': 'color',
        'size_final': 'size',
        'stock_quantity_final': 'stock_quantity',
        'reorder_level_final': 'reorder_level',
        'supplier_id_final': 'supplier_id',
        'is_active_final': 'is_active',
        'rating_final': 'rating',
        'product_created_date_final': 'product_created_date',
        'product_last_updated_source_final': 'product_last_updated_source',
        'source_item_id_int_val': 'source_item_id_int'
    })
    
    target_product_cols = [
        'product_id', 'product_name', 'description', 'category', 'brand', 'manufacturer',
        'price', 'cost', 'weight_kg', 'dim_length_cm', 'dim_width_cm', 'dim_height_cm',
        'color', 'size', 'stock_quantity', 'reorder_level', 'supplier_id', 'is_active',
        'rating', 'product_created_date', 'product_last_updated_source', 'source_item_id_int'
    ]
    for col in target_product_cols:
        if col not in df_final_products.columns:
            if col in ['source_item_id_int', 'stock_quantity', 'reorder_level']: df_final_products[col] = pd.NA
            elif col in ['price', 'cost', 'weight_kg', 'dim_length_cm', 'dim_width_cm', 'dim_height_cm', 'rating']: df_final_products[col] = np.nan
            else: df_final_products[col] = None
            logger.warning(f"Column '{col}' was missing in Products final df after rename, added as NA/None.")
    
    if df_final_products.columns.duplicated().any():
        logger.error(f"CRITICAL: Duplicate column names STILL found in Products before selecting target columns: {df_final_products.columns[df_final_products.columns.duplicated()].tolist()}")
        df_final_products = df_final_products.loc[:, ~df_final_products.columns.duplicated()]
        logger.info("Attempted to fix duplicate columns in Products by taking first occurrence.")

    df_final_products = df_final_products[target_product_cols].copy()
    
    df_final_products.dropna(subset=['product_id'], inplace=True)
    logger.info(f"Products after cleaning and selection: {df_final_products.shape[0]} rows.")
    
    df_final_products.drop_duplicates(subset=['product_id'], keep='first', inplace=True)
    df_final_products.reset_index(drop=True, inplace=True) 
    logger.info(f"Products after dropping duplicates on product_id: {df_final_products.shape[0]} rows.")
    
    df_final_products['last_updated_pipeline'] = pipeline_timestamp
    return df_final_products, product_id_mapping_dict_local


# --- etl_order_items_from_reconciliation ---
def etl_order_items_from_reconciliation(df_raw, current_existing_cust_ids, current_existing_prod_ids, current_prod_id_map):
    logger.info("Starting ETL for Order Items from reconciliation_challenge_data...")
    if df_raw.empty:
        logger.warning("Raw reconciliation_challenge_data DataFrame is empty. Skipping.")
        return pd.DataFrame()
        
    df = df_raw.copy()
    pipeline_timestamp = get_current_timestamp_str()

    df.rename(columns={
        'client_reference': 'customer_id_source', 
        'transaction_ref': 'order_id_source',
        'item_reference': 'product_id_source_raw', 
        'transaction_date': 'order_date_source',
        'amount_paid': 'line_item_amount_paid_source', 
        'payment_status': 'payment_status_source',
        'delivery_status': 'delivery_status_source', 
        'quantity_ordered': 'quantity',
        'unit_cost': 'unit_price_source', 
        'total_value': 'total_value_provided',
        'discount_applied': 'line_item_discount_source', 
        'shipping_fee': 'line_item_shipping_fee_source',
        'tax_amount': 'line_item_tax_source', 
        'notes_comments': 'line_item_notes_original'
    }, inplace=True)

    df['order_id'] = df['order_id_source'].apply(lambda x: clean_string(x, 'upper'))
    
    def map_recon_customer_id(client_ref_val, canonical_customer_ids_set_local):
        if pd.isna(client_ref_val): return None
        cleaned_client_ref = clean_string(str(client_ref_val), 'upper')
        if cleaned_client_ref and cleaned_client_ref.startswith('CLI_'):
            potential_cust_id = cleaned_client_ref.replace('CLI_', 'CUST_')
            if potential_cust_id in canonical_customer_ids_set_local:
                return potential_cust_id
        return None

    df['customer_id'] = df['customer_id_source'].apply(
        lambda x: map_recon_customer_id(x, current_existing_cust_ids)
    )
    
    def map_recon_product_id_corrected(item_ref_val, product_int_to_canonical_map, canonical_prod_ids_set_local):
        if pd.isna(item_ref_val): return None
        cleaned_item_ref = clean_string(item_ref_val, 'upper') 
        if not cleaned_item_ref: return None

        if cleaned_item_ref in canonical_prod_ids_set_local:
            return cleaned_item_ref
        
        item_num_str = None
        if cleaned_item_ref.startswith("ITM_"):
            item_num_str = cleaned_item_ref.replace("ITM_", "")
        elif cleaned_item_ref.isdigit():
            item_num_str = cleaned_item_ref
            
        if item_num_str and item_num_str.isdigit():
            item_num_str = str(int(item_num_str)) 
            if item_num_str in product_int_to_canonical_map:
                return product_int_to_canonical_map[item_num_str]
        return None
            
    df['product_id'] = df['product_id_source_raw'].apply(
        lambda x: map_recon_product_id_corrected(x, current_prod_id_map, current_existing_prod_ids)
    )

    initial_len = len(df)
    df.dropna(subset=['order_id', 'customer_id', 'product_id'], inplace=True)
    if len(df) < initial_len:
        logger.warning(f"Recon: Dropped {initial_len - len(df)} rows due to unmappable/missing key IDs.")
    logger.info(f"Recon: Shape after dropping NA in key IDs: {df.shape}")
        
    if df.empty:
        logger.warning("Recon: No valid records after ID mapping/filtering.")
        return pd.DataFrame()

    df['order_date'] = df['order_date_source'].apply(lambda x: parse_date_robustly(x, output_format='%Y-%m-%d %H:%M:%S'))
    df['quantity'] = df['quantity'].apply(lambda x: to_numeric_safe(x, target_type=int, default_value=1))
    df['unit_price'] = df['unit_price_source'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_total_value'] = df['quantity'] * df['unit_price']
    
    df['total_value_provided_numeric'] = df['total_value_provided'].apply(lambda x: to_numeric_safe(x, target_type=float))
    discrepancy_check = ~np.isclose(df['line_item_total_value'], df['total_value_provided_numeric'].fillna(df['line_item_total_value']))
    if discrepancy_check.any():
        logger.warning(f"{discrepancy_check.sum()} reconciliation items show discrepancy between calculated line_item_total_value and provided total_value.")

    df['line_item_discount'] = df['line_item_discount_source'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_shipping_fee'] = df['line_item_shipping_fee_source'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_tax'] = df['line_item_tax_source'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_amount_paid_final'] = df['line_item_amount_paid_source'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    
    df['payment_status_derived'] = df['payment_status_source'].apply(lambda x: standardize_categorical(x, PAYMENT_STATUS_MAP, case_transform='upper'))
    df['delivery_status_derived'] = df['delivery_status_source'].apply(lambda x: standardize_categorical(x, ORDER_DELIVERY_STATUS_MAP, case_transform='upper'))
    
    df['line_item_notes'] = df['line_item_notes_original'].apply(lambda x: clean_string(x))

    df['original_line_identifier'] = df['order_id_source'].astype(str) + "_RECON_" + df['product_id_source_raw'].astype(str) + "_" + df.index.astype(str)
    df['source_file'] = RECONCILIATION_DATA_CSV_ORIG_NAME
    df['last_updated_pipeline'] = pipeline_timestamp

    final_cols_for_df1 = [
        'order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price',
        'line_item_total_value', 'line_item_discount', 'line_item_shipping_fee', 'line_item_tax',
        'payment_status_derived', 'delivery_status_derived', 'line_item_notes', 
        'line_item_amount_paid_final', 'original_line_identifier', 'source_file', 'last_updated_pipeline'
    ]
    for col in final_cols_for_df1:
        if col not in df.columns:
            df[col] = pd.NA 
            logger.warning(f"Column '{col}' was missing in reconciliation item prep, added as NA/None.")
            
    df_final = df[final_cols_for_df1].copy()
    
    logger.info(f"Finished ETL for Order Items from {RECONCILIATION_DATA_CSV_ORIG_NAME}. Shape: {df_final.shape}")
    return df_final


# --- etl_order_items_from_unstructured function ---
def etl_order_items_from_unstructured(df_raw, current_existing_cust_ids, current_existing_prod_ids, current_prod_id_map):
    logger.info("Starting ETL for Order Items from orders_unstructured_data...")
    if df_raw.empty:
        logger.warning("Raw orders_unstructured_data DataFrame is empty. Skipping.")
        return pd.DataFrame()
        
    df_working = df_raw.copy() 
    pipeline_timestamp = get_current_timestamp_str()

    df_working['order_id'] = df_raw['order_id'].fillna(df_raw['ord_id'].astype(str)).apply(lambda x: clean_string(x, 'upper'))
    
    df_working['source_order_id_int'] = df_raw['ord_id'].fillna(
        df_raw['order_id'].apply(lambda x: to_numeric_safe(re.sub(r'\D', '', str(x)), target_type=int) if pd.notna(x) else pd.NA)
    ).astype('Int64')
    
    customer_id_str_source = df_raw['cust_id'].astype(str).apply(lambda x: clean_string(x, 'upper'))
    customer_id_int_source = df_raw['customer_id'].apply(lambda x: to_numeric_safe(x, target_type=int))
    df_working['customer_id'] = customer_id_str_source.fillna(
        customer_id_int_source.apply(lambda x: f"CUST_{str(x).zfill(4)}" if pd.notna(x) else None)
    )

    def resolve_unstructured_product_id(row_from_raw_data, product_id_lookup_map, canonical_product_id_set):
        prod_id_val = clean_string(row_from_raw_data.get('product_id'), 'upper')
        item_id_val = row_from_raw_data.get('item_id')
        item_id_val_str = str(int(item_id_val)) if pd.notna(item_id_val) else None

        if prod_id_val and prod_id_val in canonical_product_id_set:
            return prod_id_val
        if item_id_val_str and item_id_val_str in product_id_lookup_map:
            return product_id_lookup_map[item_id_val_str]
        if item_id_val_str and item_id_val_str in canonical_product_id_set:
             return item_id_val_str                                        
        return None
        
    df_working['product_id'] = df_raw.apply(
        lambda row: resolve_unstructured_product_id(row, current_prod_id_map, current_existing_prod_ids), axis=1
    )
    
    initial_len = len(df_working)
    df_working.dropna(subset=['order_id', 'customer_id', 'product_id'], inplace=True)
    if len(df_working) < initial_len: logger.warning(f"Unstructured: Dropped {initial_len - len(df_working)} rows due to missing key IDs after initial mapping.")
    df_working_index = df_working.index 

    initial_len = len(df_working)
    if not df_working.empty: 
        df_working = df_working[df_working['customer_id'].isin(current_existing_cust_ids)].copy()
        if len(df_working) < initial_len: logger.warning(f"Unstructured: Dropped {initial_len - len(df_working)} further rows due to unknown customer_id.")
        df_working_index = df_working.index 
    initial_len = len(df_working)

    if not df_working.empty: 
        df_working = df_working[df_working['product_id'].isin(current_existing_prod_ids)].copy()
        if len(df_working) < initial_len: logger.warning(f"Unstructured: Dropped {initial_len - len(df_working)} further rows due to unknown product_id.")
        df_working_index = df_working.index 
    
    if df_working.empty: 
        logger.warning("Unstructured: No valid records after ID mapping/filtering.")
        return pd.DataFrame()

    df_working.loc[:, 'order_date'] = df_raw.loc[df_working_index, 'order_datetime'].fillna(df_raw.loc[df_working_index, 'order_date']).apply(
        lambda x: parse_date_robustly(x, output_format='%Y-%m-%d %H:%M:%S')
    )
    df_working.loc[:, 'quantity'] = df_raw.loc[df_working_index, 'quantity'].fillna(df_raw.loc[df_working_index, 'qty']).apply(
        lambda x: to_numeric_safe(x, target_type=int, default_value=1)
    )
    df_working.loc[:, 'unit_price'] = df_raw.loc[df_working_index, 'unit_price'].fillna(df_raw.loc[df_working_index, 'price']).apply(
        lambda x: to_numeric_safe(x, target_type=float, default_value=0.0)
    )
    df_working.loc[:, 'calculated_line_total'] = df_working['quantity'] * df_working['unit_price']
    df_working.loc[:, 'line_item_total_value'] = df_raw.loc[df_working_index, 'total_amount'].apply(
        lambda x: to_numeric_safe(x, target_type=float)
    ).fillna(df_working['calculated_line_total'])
    
    df_working.loc[:, 'line_item_discount'] = df_raw.loc[df_working_index, 'discount'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df_working.loc[:, 'line_item_tax'] = df_raw.loc[df_working_index, 'tax'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df_working.loc[:, 'line_item_shipping_fee'] = df_raw.loc[df_working_index, 'shipping_cost'].apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    
    df_working.loc[:, 'line_item_amount_paid'] = (df_working['line_item_total_value'].fillna(0) - 
                                                 df_working['line_item_discount'].fillna(0) + 
                                                 df_working['line_item_tax'].fillna(0) + 
                                                 df_working['line_item_shipping_fee'].fillna(0))

    status_temp = df_raw.loc[df_working_index, 'status'].replace('',pd.NA)
    order_status_temp = df_raw.loc[df_working_index, 'order_status'].replace('',pd.NA)
    df_working.loc[:, 'line_item_status_source'] = order_status_temp.fillna(status_temp)
    df_working.loc[:, 'line_item_status'] = df_working['line_item_status_source'].apply(
        lambda x: standardize_categorical(x, ORDER_DELIVERY_STATUS_MAP, default_value=DEFAULT_STATUS_UNKNOWN, case_transform='upper')
    )

    df_working.loc[:, 'payment_method'] = df_raw.loc[df_working_index, 'payment_method'].apply(lambda x: clean_string(x, 'lower', DEFAULT_UNKNOWN_CATEGORICAL))
    df_working.loc[:, 'shipping_address_full'] = df_raw.loc[df_working_index, 'shipping_address'].apply(clean_string)
    df_working.loc[:, 'notes'] = df_raw.loc[df_working_index, 'notes'].apply(clean_string)
    df_working.loc[:, 'tracking_number'] = df_raw.loc[df_working_index, 'tracking_number'].apply(clean_string)
    
    df_working.loc[:, 'original_line_identifier'] = df_working['order_id'].astype(str) + "_UNSTR_" + \
                                                   df_working['product_id'].astype(str) + "_" + \
                                                   df_raw.loc[df_working_index, 'item_id'].astype(str) + "_" + \
                                                   df_working_index.astype(str)


    final_cols_for_df2 = [
        'order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price',
        'line_item_total_value', 'line_item_discount', 'line_item_shipping_fee', 'line_item_tax',
        'line_item_status', 'payment_method', 'shipping_address_full', 'notes', 'tracking_number',
        'source_order_id_int', 'line_item_amount_paid', 'original_line_identifier'
    ]
    for col in final_cols_for_df2:
        if col not in df_working.columns:
            df_working[col] = pd.NA
            logger.warning(f"Column '{col}' was missing in unstructured item prep, added as NA/None.")

    df_final = df_working[final_cols_for_df2].copy()
    
    df_final['source_file'] = ORDERS_UNSTRUCTURED_CSV_ORIG_NAME
    df_final['last_updated_pipeline'] = pipeline_timestamp
    
    logger.info(f"Finished ETL for Order Items from {ORDERS_UNSTRUCTURED_CSV_ORIG_NAME}. Shape: {df_final.shape}")
    return df_final


# --- etl_combine_orders_and_create_orders_table function ---
def etl_combine_orders_and_create_orders_table(df_items_list, current_existing_cust_ids_for_orders):
    logger.info(f"Starting to combine {len(df_items_list)} order item DataFrames and derive Orders table data...")
    pipeline_timestamp = get_current_timestamp_str()

    if not df_items_list or all(df is None or df.empty for df in df_items_list):
        logger.warning("No order item DataFrames to combine or all are empty. Cannot proceed.")
        return pd.DataFrame(), pd.DataFrame()

    processed_dfs_for_concat = []
    superset_cols = [
        'order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price',
        'line_item_total_value', 'line_item_discount', 'line_item_shipping_fee', 'line_item_tax',
        'payment_status_derived', 'delivery_status_derived', 'overall_item_status_derived', 
        'line_item_notes', 'line_item_amount_paid_final', 'payment_method_source', 
        'shipping_address_full_source', 'tracking_number_source', 'source_order_id_int_val',
        'source_file', 'original_line_identifier', 'last_updated_pipeline'
    ]

    for i, df_source_item in enumerate(df_items_list):
        if df_source_item is None or df_source_item.empty:
            logger.warning(f"DataFrame at index {i} in df_items_list is empty or None. Skipping.")
            continue
        
        df_temp = df_source_item.copy()
        current_source_file = df_temp['source_file'].iloc[0] if 'source_file' in df_temp.columns and not df_temp.empty else f"UnknownSource_{i}"
        logger.info(f"Pre-processing DataFrame from source: {current_source_file}, shape: {df_temp.shape}, Columns: {df_temp.columns.tolist()}")

        rename_map = {}
        if current_source_file == RECONCILIATION_DATA_CSV_ORIG_NAME:
            df_temp['overall_item_status_derived'] = df_temp['delivery_status_derived'].fillna(df_temp['payment_status_derived']).fillna(DEFAULT_STATUS_UNKNOWN)
            df_temp['payment_method_source'] = None 
            df_temp['shipping_address_full_source'] = None
            df_temp['tracking_number_source'] = None
            df_temp['source_order_id_int_val'] = None 
        
        elif current_source_file == ORDERS_UNSTRUCTURED_CSV_ORIG_NAME:
            rename_map = {
                'line_item_status': 'overall_item_status_derived',
                'payment_method': 'payment_method_source',
                'shipping_address_full': 'shipping_address_full_source',
                'notes': 'line_item_notes',
                'tracking_number': 'tracking_number_source',
                'source_order_id_int': 'source_order_id_int_val',
                'line_item_amount_paid': 'line_item_amount_paid_final'
            }
            df_temp.rename(columns=rename_map, inplace=True, errors='raise')
            
            if 'payment_status_derived' not in df_temp.columns:
                 df_temp['payment_status_derived'] = df_temp.get('overall_item_status_derived', pd.Series(index=df_temp.index, dtype=object)).apply(
                    lambda x: x if standardize_categorical(x, PAYMENT_STATUS_MAP, default_value=None, case_transform='upper') is not None and x in PAYMENT_STATUS_MAP.values() else DEFAULT_STATUS_UNKNOWN
                )
            if 'delivery_status_derived' not in df_temp.columns:
                df_temp['delivery_status_derived'] = df_temp.get('overall_item_status_derived', pd.Series(index=df_temp.index, dtype=object)).apply(
                    lambda x: x if standardize_categorical(x, ORDER_DELIVERY_STATUS_MAP, default_value=None, case_transform='upper') is not None and x in ORDER_DELIVERY_STATUS_MAP.values() else DEFAULT_STATUS_UNKNOWN
                )
        
        for col in superset_cols:
            if col not in df_temp.columns:
                if col in ['source_order_id_int_val', 'quantity']: df_temp[col] = pd.NA
                elif col in ['line_item_total_value', 'line_item_discount', 'line_item_shipping_fee', 'line_item_tax', 'unit_price', 'line_item_amount_paid_final']: df_temp[col] = 0.0
                else: df_temp[col] = None
        processed_dfs_for_concat.append(df_temp[superset_cols])

    if not processed_dfs_for_concat:
        logger.warning("No non-empty DataFrames after initial processing for concat. Returning empty.")
        return pd.DataFrame(), pd.DataFrame()

    df_all_order_items = pd.concat(processed_dfs_for_concat, ignore_index=True)
    
    if 'order_item_id' not in df_all_order_items.columns or df_all_order_items['order_item_id'].isnull().any():
        df_all_order_items.drop(columns=['order_item_id'], inplace=True, errors='ignore')
        df_all_order_items.insert(0, 'order_item_id', range(1, 1 + len(df_all_order_items)))
    
    logger.info(f"Combined order items. Shape: {df_all_order_items.shape}")
    if 'line_item_amount_paid_final' not in df_all_order_items.columns:
         logger.error("'line_item_amount_paid_final' is MISSING from combined items! This is critical. Adding as 0.0")
         df_all_order_items['line_item_amount_paid_final'] = 0.0
    else:
        df_all_order_items['line_item_amount_paid_final'] = pd.to_numeric(df_all_order_items['line_item_amount_paid_final'], errors='coerce').fillna(0.0)

    if df_all_order_items.empty:
        logger.warning("df_all_order_items is unexpectedly empty before deriving Orders table.")
        return pd.DataFrame(), pd.DataFrame()

    aggregation_defaults = {
        'customer_id': None, 'order_date': None, 'overall_item_status_derived': DEFAULT_STATUS_UNKNOWN,
        'payment_method_source': DEFAULT_UNKNOWN_CATEGORICAL, 
        'payment_status_derived': DEFAULT_STATUS_UNKNOWN,
        'delivery_status_derived': DEFAULT_STATUS_UNKNOWN,
        'shipping_address_full_source': None,
        'line_item_shipping_fee': 0.0, 'line_item_tax': 0.0, 'line_item_discount': 0.0,
        'line_item_total_value': 0.0, 
        'tracking_number_source': None, 'line_item_notes': None, 'source_order_id_int_val': pd.NA
    }
    for col, default_val in aggregation_defaults.items():
        if col not in df_all_order_items.columns:
            df_all_order_items[col] = default_val

    numeric_agg_cols = ['line_item_shipping_fee', 'line_item_tax', 'line_item_discount', 'line_item_total_value']
    for col in numeric_agg_cols:
        if col in df_all_order_items.columns:
            df_all_order_items[col] = pd.to_numeric(df_all_order_items[col], errors='coerce').fillna(0.0)
        else:
            df_all_order_items[col] = 0.0

    df_orders = df_all_order_items.groupby('order_id', as_index=False).agg(
        customer_id=('customer_id', 'first'),
        order_date=('order_date', 'min'), 
        order_status=('overall_item_status_derived', lambda x: x.mode(dropna=True)[0] if not x.mode(dropna=True).empty else DEFAULT_STATUS_UNKNOWN),
        payment_method=('payment_method_source', lambda x: x.dropna().iloc[0] if not x.dropna().empty else DEFAULT_UNKNOWN_CATEGORICAL),
        payment_status=('payment_status_derived', lambda x: x.dropna().iloc[0] if not x.dropna().empty else DEFAULT_STATUS_UNKNOWN),
        delivery_status=('delivery_status_derived', lambda x: x.dropna().iloc[0] if not x.dropna().empty else DEFAULT_STATUS_UNKNOWN),
        shipping_address_full=('shipping_address_full_source', 'first'),
        shipping_cost_total=('line_item_shipping_fee', 'sum'),
        tax_total=('line_item_tax', 'sum'),
        discount_total=('line_item_discount', 'sum'),
        order_total_value_gross=('line_item_total_value', 'sum'),
        amount_paid_total=('line_item_amount_paid_final', 'sum'),
        tracking_number=('tracking_number_source', 'first'),
        notes=('line_item_notes', lambda x: '; '.join(sorted(list(x.dropna().astype(str).unique()))) if not x.dropna().empty and x.dropna().astype(str).str.len().sum() > 0 else None),
        source_order_id_int=('source_order_id_int_val', 'first')
    )
    
    df_orders['order_total_value_net'] = df_orders['order_total_value_gross'] - df_orders['discount_total']
    df_orders['last_updated_pipeline'] = pipeline_timestamp
    
    initial_order_count = len(df_orders)
    if 'customer_id' in df_orders.columns and df_orders['customer_id'].notna().any():
        df_orders = df_orders[df_orders['customer_id'].isin(current_existing_cust_ids_for_orders)].copy()
        if initial_order_count > len(df_orders):
            logger.warning(f"Derived Orders: Dropped {initial_order_count - len(df_orders)} orders due to customer_id not in Customers table.")
    elif 'customer_id' in df_orders.columns:
        logger.warning("Derived Orders: All customer_ids are NA, resulting in an empty Orders table after FK check.")
        df_orders = pd.DataFrame(columns=df_orders.columns)
    else:
        logger.error("Derived Orders: customer_id column is missing. Cannot perform FK check.")
        return pd.DataFrame(), pd.DataFrame()

    logger.info(f"Derived Orders table. Shape: {df_orders.shape}")
    
    final_order_item_db_cols = [
        'order_item_id', 'order_id', 'product_id', 'customer_id', 
        'quantity', 'unit_price', 'line_item_total_value', 'line_item_discount', 
        'line_item_tax', 'line_item_shipping_fee', 'source_file', 
        'original_line_identifier', 'last_updated_pipeline'
    ]
    
    for col in final_order_item_db_cols:
        if col not in df_all_order_items.columns:
            if col in ['quantity']: df_all_order_items[col] = pd.NA
            elif col in ['unit_price', 'line_item_total_value', 'line_item_discount', 'line_item_tax', 'line_item_shipping_fee']: df_all_order_items[col] = 0.0
            else: df_all_order_items[col] = None 
            logger.warning(f"Column '{col}' for final OrderItems was missing from combined items and added as NA/None/0.0.")
            
    df_order_items_for_db = df_all_order_items[final_order_item_db_cols].copy()
    df_order_items_for_db = df_order_items_for_db[df_order_items_for_db['order_id'].isin(df_orders['order_id'])].copy()
    logger.info(f"Final OrderItems for DB after filtering by valid orders. Shape: {df_order_items_for_db.shape}")

    return df_order_items_for_db, df_orders

logger.info("ETL pipeline functions defined in src/etl_pipelines.py.")