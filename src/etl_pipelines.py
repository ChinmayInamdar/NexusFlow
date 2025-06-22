# src/etl_pipelines.py
import pandas as pd
import numpy as np
import re
from datetime import datetime

from .config import (
    logger, DEFAULT_UNKNOWN_CATEGORICAL, DEFAULT_UNKNOWN_NUMERIC_INT,
    DEFAULT_UNKNOWN_NUMERIC_FLOAT, DEFAULT_STATUS_UNKNOWN,
    GENDER_MAP, CUSTOMER_STATUS_MAP, PAYMENT_STATUS_MAP,
    ORDER_DELIVERY_STATUS_MAP, STATE_ABBREVIATION_MAP, CITY_NORMALIZATION_MAP
)
from .data_processing_utils import ( # Ensure all these are correctly defined and imported
    clean_string, standardize_categorical, parse_date_robustly,
    to_numeric_safe, standardize_boolean_strict, standardize_phone_strict,
    standardize_postal_code, get_current_timestamp_str,
    standardize_customer_name_advanced # Crucial for improved name cleaning
)

# Helper to ensure all target columns exist in the DataFrame
def _ensure_df_columns(df, target_cols_list, default_na_map=None):
    if default_na_map is None: default_na_map = {}
    output_df = pd.DataFrame() 
    for col in target_cols_list:
        if col in df.columns:
            # If df[col] is a DataFrame (due to duplicate upstream columns), take the first Series
            if isinstance(df[col], pd.DataFrame):
                logger.warning(f"Column '{col}' in input to _ensure_df_columns was a DataFrame. Taking first column.")
                output_df[col] = df[col].iloc[:, 0] 
            else:
                output_df[col] = df[col]
        else:
            output_df[col] = default_na_map.get(col, pd.NA) 
            logger.warning(f"Column '{col}' was missing from DataFrame during final column selection, added with default value.")
    # Ensure columns are in the specified order
    return output_df[target_cols_list].copy()

def etl_customers(df_raw_cust, source_file_being_processed):
    logger.info(f"Starting Customers ETL for source: {source_file_being_processed}...")
    if df_raw_cust.empty:
        logger.warning(f"Raw customer DataFrame from {source_file_being_processed} is empty. Skipping ETL.")
        return pd.DataFrame()

    df = df_raw_cust.copy()

    known_raw_customer_cols_expected = [ 
        'cust_id', 'customer_id', 'customerid', 'client_id', 'id', 'user_id', 
        'customer_name', 'full_name', 'name', 'email', 'email_address', 'e-mail',
        'phone', 'phone_number', 'contact_number', 'address', 'street_address', 'address1',
        'city', 'town', 'state', 'province', 'postal_code', 'zip_code', 'zip',
        'reg_date', 'registration_date', 'created_at', 'birth_date', 'dob',
        'status', 'customer_status', 'account_status', 'total_spent', 'total_expenditure',
        'total_orders', 'order_count', 'loyalty_points', 'points', 'age',
        'gender', 'sex', 'segment', 'customer_segment', 'tier', 'preferred_payment', 'payment_method'
    ]
    raw_input_cols_set = set(c.lower() for c in df.columns)
    known_cols_set = set(c.lower() for c in known_raw_customer_cols_expected)
    extra_cols_found_in_raw = [orig_col for orig_col in df.columns if orig_col.lower() not in known_cols_set]
    if extra_cols_found_in_raw:
        logger.info(f"[ETL Customers - {source_file_being_processed}] Found extra columns in raw input that will be ignored if not explicitly mapped: {extra_cols_found_in_raw}")

    pipeline_timestamp = get_current_timestamp_str()
    df.reset_index(drop=True, inplace=True)
    df['_original_index_for_missing_id'] = df.index.astype(str)

    # 1. ID Unification 
    # Try to get an existing string ID first from various possible column names
    df['customer_id_canon_pre'] = df.get('cust_id', 
                                   df.get('customerID', # Common variant for customer_id
                                      df.get('client_id', 
                                         df.get('id', # Generic ID, could be string or numeric
                                            df.get('user_id', pd.Series(index=df.index, dtype='object')))))).apply(lambda x: clean_string(x, case='upper') if pd.notna(x) else None)

    # Separately get a potentially numeric ID for source_customer_id_int_val
    # This specifically looks for columns that are likely to hold purely numeric representations.
    numeric_id_col_candidate = 'customer_id' # Start with the most common name
    if numeric_id_col_candidate not in df.columns or not pd.api.types.is_numeric_dtype(df[numeric_id_col_candidate].dropna()):
        if 'CustomerID_numeric' in df.columns and pd.api.types.is_numeric_dtype(df['CustomerID_numeric'].dropna()):
            numeric_id_col_candidate = 'CustomerID_numeric'
        elif 'id' in df.columns and pd.api.types.is_numeric_dtype(df['id'].dropna()): # If 'id' is numeric, and 'customer_id' wasn't
             numeric_id_col_candidate = 'id'
        # If none of the preferred numeric columns are found or numeric, numeric_id_series_for_int will be mostly NA
        
    numeric_id_series_for_int = df.get(numeric_id_col_candidate, pd.Series(index=df.index, dtype='object'))
    df['source_customer_id_int_val'] = numeric_id_series_for_int.apply(lambda x: to_numeric_safe(x, target_type=int))
    
    # Create canonical customer ID: "CUST_" prefix for numeric-like IDs, keep others as is (after cleaning)
    # Ensure zfill length matches what downstream processes expect (e.g., CUST_0082 needs zfill(4))
    ZFILL_LENGTH = 4 # Define this once, ensure it's consistent with other CUST_ ID generations

    def create_canonical_customer_id(row):
        pre_canon_id = row['customer_id_canon_pre']
        int_val = row['source_customer_id_int_val']

        if pd.notna(pre_canon_id) and pre_canon_id.strip() != "":
            # If pre_canon_id already starts with "CUST_" or is non-numeric, it's likely already canonical or a specific string ID
            if pre_canon_id.startswith("CUST_") or not pre_canon_id.isdigit():
                return pre_canon_id 
            else: # It's a plain numeric string from a string ID column, prefix it
                try:
                    return f"CUST_{str(int(float(pre_canon_id))).zfill(ZFILL_LENGTH)}"
                except ValueError: # If conversion to int fails, it wasn't purely numeric
                    return pre_canon_id # Keep as is
        
        # If pre_canon_id was missing or empty, build from int_val
        if pd.notna(int_val):
            return f"CUST_{str(int_val).zfill(ZFILL_LENGTH)}"
        
        # Fallback if both are missing
        return f"CUST_UNKNOWN_{str(row['_original_index_for_missing_id'])}"

    df['customer_id_canon'] = df.apply(create_canonical_customer_id, axis=1)
    
    cols_to_drop_intermediate = ['_original_index_for_missing_id', 'customer_id_canon_pre',
                                 'cust_id', 'customerID', 'client_id', 'id', 'user_id', 'CustomerID_numeric']
    # If 'customer_id' was the numeric_id_col_candidate, it should be dropped if different from target name.
    # It will be renamed later from 'customer_id_canon' to 'customer_id'.
    if numeric_id_col_candidate == 'customer_id' and 'customer_id' in df.columns:
         cols_to_drop_intermediate.append('customer_id') # Add original 'customer_id' if it was purely for numeric source
    
    # 2. Name: Coalesce and standardize using advanced function
    name_series = df.get('customer_name', pd.Series(dtype='object')) \
                    .fillna(df.get('full_name', pd.Series(dtype='object'))) \
                    .fillna(df.get('name', pd.Series(dtype='object')))
    df['customer_name_final'] = name_series.apply(standardize_customer_name_advanced)
    cols_to_drop_intermediate.extend(['customer_name', 'full_name', 'name'])

    # 3. Email
    df['email_temp'] = df.get('email', df.get('e-mail', pd.Series(dtype=object))).replace('', pd.NA).apply(lambda x: clean_string(x, 'lower') if pd.notna(x) else None)
    df['email_address_temp'] = df.get('email_address', df.get('user_email', pd.Series(dtype=object))).replace('', pd.NA).apply(lambda x: clean_string(x, 'lower') if pd.notna(x) else None)
    df['email_final'] = df['email_temp'].fillna(df['email_address_temp']); df.loc[df['email_final'] == '', 'email_final'] = None
    cols_to_drop_intermediate.extend(['email', 'e-mail', 'email_address', 'user_email', 'email_temp', 'email_address_temp'])

    # 4. Phone
    df['phone_temp'] = df.get('phone', df.get('contact_number', pd.Series(dtype=object))).replace('', pd.NA)
    df['phone_number_temp'] = df.get('phone_number', df.get('mobile', pd.Series(dtype=object))).replace('', pd.NA)
    df['phone_final'] = df['phone_number_temp'].fillna(df['phone_temp']).apply(standardize_phone_strict)
    cols_to_drop_intermediate.extend(['phone', 'contact_number', 'phone_number', 'mobile', 'phone_temp', 'phone_number_temp'])

    # 5. Address
    df['address_street_final'] = df.get('address', df.get('street_address', df.get('address1', pd.Series(dtype=object)))).apply(lambda x: clean_string(x, 'title'))
    df['address_city_cleaned'] = df.get('city', df.get('town', pd.Series(dtype=object))).apply(lambda x: clean_string(x, case='upper'))
    df['address_city_final'] = df['address_city_cleaned'].apply(lambda x: CITY_NORMALIZATION_MAP.get(x, x.title() if pd.notna(x) and x else DEFAULT_UNKNOWN_CATEGORICAL) if pd.notna(x) and x else DEFAULT_UNKNOWN_CATEGORICAL)
    df['address_state_cleaned'] = df.get('state', df.get('province', pd.Series(dtype=object))).apply(lambda x: clean_string(x, case='upper'))
    df['address_state_final'] = df['address_state_cleaned'].apply(lambda x: STATE_ABBREVIATION_MAP.get(x, DEFAULT_UNKNOWN_CATEGORICAL if pd.isna(x) or not x else x) if pd.notna(x) and x else DEFAULT_UNKNOWN_CATEGORICAL)
    df['postal_code_temp'] = df.get('postal_code', df.get('zip_code', df.get('zip', df.get('postcode', pd.Series(dtype=object))))).replace('', pd.NA)
    df['address_postal_code_final'] = df['postal_code_temp'].apply(standardize_postal_code)
    cols_to_drop_intermediate.extend(['address', 'street_address', 'address1', 'city', 'town', 'state', 'province', 'zip_code', 'zip', 'postcode', 'postal_code', 'address_city_cleaned', 'address_state_cleaned', 'postal_code_temp'])
    
    # 6. Dates
    df['reg_date_temp'] = df.get('reg_date', df.get('created_at', pd.Series(dtype=object))).replace('', pd.NA)
    df['registration_date_temp'] = df.get('registration_date', pd.Series(dtype=object)).replace('', pd.NA)
    df['registration_date_final'] = df['registration_date_temp'].fillna(df['reg_date_temp']).apply(parse_date_robustly)
    df['birth_date_final'] = df.get('birth_date', df.get('dob', pd.Series(dtype=object))).apply(parse_date_robustly)
    cols_to_drop_intermediate.extend(['reg_date', 'created_at', 'registration_date', 'birth_date', 'dob', 'reg_date_temp', 'registration_date_temp'])

    # 7. Status
    df['status_temp'] = df.get('status', pd.Series(dtype=object)).replace('', pd.NA).replace(' ', pd.NA).replace('None', pd.NA)
    df['customer_status_temp'] = df.get('customer_status', df.get('account_status', pd.Series(dtype=object))).replace('', pd.NA).replace(' ', pd.NA).replace('None', pd.NA)
    df['status_coalesced'] = df['customer_status_temp'].fillna(df['status_temp'])
    df['status_cleaned_for_map'] = df['status_coalesced'].apply(lambda x: clean_string(x, case='upper'))
    df['status_final'] = df['status_cleaned_for_map'].apply(lambda x: CUSTOMER_STATUS_MAP.get(x, DEFAULT_STATUS_UNKNOWN) if pd.notna(x) else DEFAULT_STATUS_UNKNOWN)
    cols_to_drop_intermediate.extend(['status', 'customer_status', 'account_status', 'status_temp', 'customer_status_temp', 'status_coalesced', 'status_cleaned_for_map'])
    
    # 8. Numeric
    df['total_spent_final'] = df.get('total_spent', df.get('total_expenditure', pd.Series(dtype=object))).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['total_orders_final'] = df.get('total_orders', df.get('order_count', pd.Series(dtype=object))).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=0))
    df['loyalty_points_final'] = df.get('loyalty_points', df.get('points', pd.Series(dtype=object))).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=0))
    cols_to_drop_intermediate.extend(['total_spent', 'total_expenditure', 'total_orders', 'order_count', 'loyalty_points', 'points'])

    # 9. Age
    def calculate_age(birth_date_str):
        if pd.isna(birth_date_str): return pd.NA
        try: birth_dt = datetime.strptime(str(birth_date_str)[:10], '%Y-%m-%d'); today = datetime.today(); return today.year - birth_dt.year - ((today.month, today.day) < (birth_dt.month, birth_dt.day))
        except: return pd.NA
    df['age_calculated'] = df['birth_date_final'].apply(calculate_age)
    df['age_provided_numeric'] = df.get('age', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=pd.NA))
    df['age_final'] = df['age_calculated'].fillna(df['age_provided_numeric']).astype('Int64')
    cols_to_drop_intermediate.extend(['age', 'age_calculated', 'age_provided_numeric'])

    # 10. Gender
    df['gender_cleaned'] = df.get('gender', df.get('sex', pd.Series(dtype=object))).apply(lambda x: clean_string(x, case='upper'))
    df['gender_final'] = df['gender_cleaned'].apply(lambda x: GENDER_MAP.get(x, DEFAULT_UNKNOWN_CATEGORICAL) if pd.notna(x) else DEFAULT_UNKNOWN_CATEGORICAL)
    cols_to_drop_intermediate.extend(['gender', 'sex', 'gender_cleaned'])

    # 11. Segment & Payment Method
    df['segment_final'] = df.get('segment', df.get('customer_segment', df.get('tier', pd.Series(dtype=object)))).apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    df['preferred_payment_method_final'] = df.get('preferred_payment', df.get('payment_method', pd.Series(dtype=object))).apply(lambda x: clean_string(x, 'lower', DEFAULT_UNKNOWN_CATEGORICAL))
    cols_to_drop_intermediate.extend(['segment', 'customer_segment', 'tier', 'preferred_payment', 'payment_method'])
    
    actual_cols_to_drop = list(set([col for col in cols_to_drop_intermediate if col in df.columns]))
    if actual_cols_to_drop:
        df.drop(columns=actual_cols_to_drop, inplace=True, errors='ignore')
    
    df_renamed = df.rename(columns={
        'customer_id_canon': 'customer_id', 'customer_name_final': 'customer_name',
        'email_final': 'email', 'phone_final': 'phone',
        'address_street_final': 'address_street', 'address_city_final': 'address_city',
        'address_state_final': 'address_state', 'address_postal_code_final': 'address_postal_code',
        'registration_date_final': 'registration_date', 'status_final': 'status',
        'total_orders_final': 'total_orders', 'total_spent_final': 'total_spent',
        'loyalty_points_final': 'loyalty_points',
        'preferred_payment_method_final': 'preferred_payment_method',
        'birth_date_final': 'birth_date', 'age_final': 'age', 'gender_final': 'gender',
        'segment_final': 'segment', 'source_customer_id_int_val': 'source_customer_id_int'
    })

    df_renamed['source_file_name'] = source_file_being_processed
    df_renamed['last_updated_pipeline'] = pipeline_timestamp

    if df_renamed.columns.duplicated().any():
        duplicated_cols = df_renamed.columns[df_renamed.columns.duplicated()].tolist()
        logger.warning(f"[ETL Customers - {source_file_being_processed}] Duplicate column names found in df_renamed: {duplicated_cols}. Taking first occurrence.")
        df_renamed = df_renamed.loc[:, ~df_renamed.columns.duplicated(keep='first')]

    final_customer_columns_ordered = [
        'customer_id', 'source_file_name', 'customer_name', 'email', 'phone',
        'address_street', 'address_city', 'address_state', 'address_postal_code',
        'registration_date', 'status', 'total_orders', 'total_spent',
        'loyalty_points', 'preferred_payment_method', 'birth_date', 'age', 'gender',
        'segment', 'source_customer_id_int', 'last_updated_pipeline'
    ]
    df_final_customers = _ensure_df_columns(df_renamed, final_customer_columns_ordered)
    
    df_final_customers.dropna(subset=['customer_id'], inplace=True)
    if df_final_customers.empty:
        logger.warning(f"No customers after NA drop on customer_id (source: {source_file_being_processed}).")
        return pd.DataFrame()
        
    df_final_customers.sort_values(by=['customer_id', 'source_customer_id_int'], na_position='last', inplace=True) 
    df_final_customers.drop_duplicates(subset=['customer_id'], keep='first', inplace=True) 
    
    if 'email' in df_final_customers.columns and not df_final_customers.empty:
        temp_df = df_final_customers.copy()
        temp_df['_has_valid_email'] = temp_df['email'].notna() & (temp_df['email'].astype(str).str.strip() != '')
        df_with_valid_email = temp_df[temp_df['_has_valid_email']].copy()
        df_without_valid_email = temp_df[~temp_df['_has_valid_email']].copy()
        if not df_with_valid_email.empty:
            df_with_valid_email.sort_values(by=['email', 'customer_id'], inplace=True)
            df_with_valid_email_deduped = df_with_valid_email.drop_duplicates(subset=['email'], keep='first')
            df_final_customers = pd.concat([df_with_valid_email_deduped, df_without_valid_email], ignore_index=True)
        elif not df_without_valid_email.empty :
            df_final_customers = df_without_valid_email.copy()
        if '_has_valid_email' in df_final_customers.columns: df_final_customers.drop(columns=['_has_valid_email'], inplace=True)
        df_final_customers.reset_index(drop=True, inplace=True)

    logger.info(f"Customers ETL for {source_file_being_processed} finished. Output shape: {df_final_customers.shape}")
    return df_final_customers


def etl_products(df_raw_prod, source_file_being_processed):
    logger.info(f"Starting Products ETL process for source: {source_file_being_processed}...")
    if df_raw_prod.empty:
        logger.warning(f"Raw product DataFrame from {source_file_being_processed} is empty. Skipping ETL.")
        return pd.DataFrame(), {}
    df = df_raw_prod.copy()

    known_raw_product_cols_expected = [
        'item_id', 'product_id', 'productid', 'id', 'product_code', 'item_code',
        'product_name', 'item_name', 'name', 'title', 'prd_name',
        'description', 'desc', 'details', 'product_description',
        'category', 'product_category', 'type', 'genre', 'producttype',
        'brand', 'manufacturer',
        'price', 'unit_price', 'sale_price', 'list_price', 'prd_price',
        'cost', 'unit_cost', 'purchase_price',
        'weight', 'dimensions', 'color', 'size',
        'stock_quantity', 'stock_level', 'qty_on_hand',
        'reorder_level', 'supplier_id', 'is_active', 'active',
        'rating', 'customer_rating',
        'created_date', 'date_added', 'last_updated', 'modified_date'
    ]
    raw_input_cols_set = set(c.lower() for c in df.columns)
    known_cols_set = set(c.lower() for c in known_raw_product_cols_expected)
    extra_cols_found_in_raw = [orig_col for orig_col in df.columns if orig_col.lower() not in known_cols_set]
    if extra_cols_found_in_raw:
        logger.info(f"[ETL Products - {source_file_being_processed}] Found extra columns in raw input that will be ignored if not explicitly mapped: {extra_cols_found_in_raw}")

    pipeline_timestamp = get_current_timestamp_str()
    product_id_mapping_dict_local = {} 

    df['source_item_id_int_val'] = df.get('item_id', df.get('id', pd.Series(index=df.index, dtype='object'))).apply(lambda x: to_numeric_safe(x, target_type=int))
    original_product_id_col_val = df.get('product_id', df.get('productid', df.get('item_code', df.get('product_code', pd.Series(index=df.index, dtype='object')))))
    df['product_id_canon'] = original_product_id_col_val.apply(lambda x: clean_string(x, 'upper') if pd.notna(x) else None)
    cols_to_drop_intermediate = ['item_id', 'product_id', 'productid', 'id', 'item_code', 'product_code'] 
    df['product_name_final'] = df.get('product_name', df.get('item_name', df.get('name', df.get('title', df.get('prd_name', pd.Series(index=df.index, dtype='object')))))).apply(lambda x: clean_string(x, 'title'))
    cols_to_drop_intermediate.extend(['product_name', 'item_name', 'name', 'title', 'prd_name'])
    df['description_final'] = df.get('description', df.get('desc', df.get('details', df.get('product_description', pd.Series(index=df.index, dtype='object'))))).apply(lambda x: clean_string(x) if pd.notna(x) else "No description available")
    cols_to_drop_intermediate.extend(['description', 'desc', 'details', 'product_description'])
    df['category_temp'] = df.get('category', df.get('product_category', df.get('type', df.get('genre', df.get('producttype', pd.Series(index=df.index, dtype='object')))))).apply(lambda x: clean_string(x, 'title'))
    df['category_final'] = df['category_temp'].fillna(DEFAULT_UNKNOWN_CATEGORICAL); cols_to_drop_intermediate.extend(['category', 'product_category', 'type', 'genre', 'producttype', 'category_temp'])
    brand_series = df.get('brand', pd.Series(index=df.index, dtype='object')).replace('', pd.NA); manufacturer_series = df.get('manufacturer', pd.Series(index=df.index, dtype='object')).replace('', pd.NA)
    df['brand_temp'] = brand_series.fillna(manufacturer_series); df['brand_final'] = df['brand_temp'].apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    manufacturer_fallback = df['brand_final'].replace(DEFAULT_UNKNOWN_CATEGORICAL, pd.NA) if 'brand_final' in df.columns else pd.NA
    df['manufacturer_temp'] = manufacturer_series.fillna(manufacturer_fallback); df['manufacturer_final'] = df['manufacturer_temp'].apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    cols_to_drop_intermediate.extend(['brand', 'manufacturer', 'brand_temp', 'manufacturer_temp'])
    df['price_final'] = df.get('price', df.get('unit_price', df.get('sale_price', df.get('list_price', df.get('prd_price', pd.Series(index=df.index, dtype='object')))))).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=DEFAULT_UNKNOWN_NUMERIC_FLOAT))
    df['cost_final'] = df.get('cost', df.get('unit_cost', df.get('purchase_price', pd.Series(index=df.index, dtype='object')))).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=DEFAULT_UNKNOWN_NUMERIC_FLOAT))
    df['weight_kg_final'] = df.get('weight', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=np.nan))
    df['rating_final'] = df.get('rating', df.get('customer_rating', pd.Series(index=df.index, dtype='object'))).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=np.nan))
    cols_to_drop_intermediate.extend(['price', 'unit_price', 'sale_price', 'list_price', 'prd_price', 'cost', 'unit_cost', 'purchase_price', 'weight', 'rating', 'customer_rating'])
    def parse_dimensions_strict(dim_str):
        if pd.isna(dim_str) or str(dim_str).strip() == '': return None, None, None
        parts = [p.strip() for p in re.split(r'[xX]', str(dim_str).lower())];
        if len(parts) == 3: l, w, h = to_numeric_safe(parts[0]), to_numeric_safe(parts[1]), to_numeric_safe(parts[2]); return (l if l is not None else np.nan, w if w is not None else np.nan, h if h is not None else np.nan)
        return np.nan, np.nan, np.nan
    dims_parsed = df.get('dimensions', pd.Series(index=df.index, dtype='object')).apply(parse_dimensions_strict)
    df['dim_length_cm_final'] = dims_parsed.apply(lambda x: x[0] if isinstance(x, tuple) else np.nan).astype('Float64')
    df['dim_width_cm_final'] = dims_parsed.apply(lambda x: x[1] if isinstance(x, tuple) else np.nan).astype('Float64')
    df['dim_height_cm_final'] = dims_parsed.apply(lambda x: x[2] if isinstance(x, tuple) else np.nan).astype('Float64')
    cols_to_drop_intermediate.append('dimensions')
    df['color_final'] = df.get('color', pd.Series(index=df.index, dtype='object')).replace('', pd.NA).apply(lambda x: clean_string(x, 'title', DEFAULT_UNKNOWN_CATEGORICAL))
    df['size_final'] = df.get('size', pd.Series(index=df.index, dtype='object')).replace('', pd.NA).apply(lambda x: clean_string(x, 'upper', 'N/A')) 
    cols_to_drop_intermediate.extend(['color', 'size'])
    df['stock_quantity_final'] = df.get('stock_quantity', df.get('stock_level', df.get('qty_on_hand', pd.Series(index=df.index, dtype='object')))).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=DEFAULT_UNKNOWN_NUMERIC_INT))
    df['reorder_level_final'] = df.get('reorder_level', pd.Series(index=df.index, dtype='object')).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=DEFAULT_UNKNOWN_NUMERIC_INT))
    cols_to_drop_intermediate.extend(['stock_quantity', 'stock_level', 'qty_on_hand', 'reorder_level'])
    df['supplier_id_final'] = df.get('supplier_id', pd.Series(index=df.index, dtype='object')).apply(lambda x: clean_string(x, 'upper', DEFAULT_UNKNOWN_CATEGORICAL))
    cols_to_drop_intermediate.append('supplier_id')
    df['is_active_final'] = df.get('is_active', df.get('active', pd.Series(index=df.index, dtype='object'))).apply(standardize_boolean_strict)
    cols_to_drop_intermediate.extend(['is_active', 'active'])
    df['product_created_date_final'] = df.get('created_date', df.get('date_added', pd.Series(index=df.index, dtype='object'))).apply(lambda x: parse_date_robustly(x))
    df['product_last_updated_source_final'] = df.get('last_updated', df.get('modified_date', pd.Series(index=df.index, dtype='object'))).apply(lambda x: parse_date_robustly(x, output_format='%Y-%m-%d %H:%M:%S'))
    cols_to_drop_intermediate.extend(['created_date', 'date_added', 'last_updated', 'modified_date'])
    
    actual_cols_to_drop = list(set([col for col in cols_to_drop_intermediate if col in df.columns]))
    if actual_cols_to_drop:
        df.drop(columns=actual_cols_to_drop, inplace=True, errors='ignore')

    if not df.empty: 
        for _, row in df.iterrows():
            prod_id_canon = row.get('product_id_canon'); src_item_id_int = row.get('source_item_id_int_val')
            if pd.notna(src_item_id_int) and pd.notna(prod_id_canon): product_id_mapping_dict_local[str(src_item_id_int)] = str(prod_id_canon)
            if pd.notna(prod_id_canon): product_id_mapping_dict_local.setdefault(str(prod_id_canon), str(prod_id_canon))
    
    df_renamed = df.rename(columns={
        'product_id_canon': 'product_id', 'product_name_final': 'product_name',
        'description_final': 'description', 'category_final': 'category', 'brand_final': 'brand',
        'manufacturer_final': 'manufacturer', 'price_final': 'price', 'cost_final': 'cost',
        'weight_kg_final': 'weight_kg', 'dim_length_cm_final': 'dim_length_cm',
        'dim_width_cm_final': 'dim_width_cm', 'dim_height_cm_final': 'dim_height_cm',
        'color_final': 'color', 'size_final': 'size', 'stock_quantity_final': 'stock_quantity',
        'reorder_level_final': 'reorder_level', 'supplier_id_final': 'supplier_id',
        'is_active_final': 'is_active', 'rating_final': 'rating',
        'product_created_date_final': 'product_created_date',
        'product_last_updated_source_final': 'product_last_updated_source',
        'source_item_id_int_val': 'source_item_id_int'
    })

    df_renamed['source_file_name'] = source_file_being_processed
    df_renamed['last_updated_pipeline'] = pipeline_timestamp
    
    if df_renamed.columns.duplicated().any():
        duplicated_cols = df_renamed.columns[df_renamed.columns.duplicated()].tolist()
        logger.warning(f"[ETL Products - {source_file_being_processed}] Duplicate column names found in df_renamed: {duplicated_cols}. Taking first occurrence.")
        df_renamed = df_renamed.loc[:, ~df_renamed.columns.duplicated(keep='first')]

    final_product_columns_ordered = [
        'product_id', 'source_file_name', 'product_name', 'description', 'category', 'brand', 'manufacturer',
        'price', 'cost', 'weight_kg', 'dim_length_cm', 'dim_width_cm', 'dim_height_cm',
        'color', 'size', 'stock_quantity', 'reorder_level', 'supplier_id', 'is_active',
        'rating', 'product_created_date', 'product_last_updated_source', 'source_item_id_int', 'last_updated_pipeline'
    ]
    df_final_products = _ensure_df_columns(df_renamed, final_product_columns_ordered)

    df_final_products.dropna(subset=['product_id'], inplace=True)
    if df_final_products.empty:
        logger.warning(f"No products after NA drop on product_id (source: {source_file_being_processed}).")
        return pd.DataFrame(), product_id_mapping_dict_local
        
    df_final_products.sort_values(by=['product_id', 'source_item_id_int'], na_position='last', inplace=True)
    df_final_products.drop_duplicates(subset=['product_id'], keep='first', inplace=True)
    
    logger.info(f"Products ETL for {source_file_being_processed} finished. Output shape: {df_final_products.shape}. Local map size: {len(product_id_mapping_dict_local)}")
    return df_final_products, product_id_mapping_dict_local


def etl_order_items_from_reconciliation(df_raw, source_file_being_processed, current_existing_cust_ids, current_existing_prod_ids, current_prod_id_map):
    logger.info(f"Recon ETL for {source_file_being_processed}: Sample current_existing_cust_ids: {list(current_existing_cust_ids)[:5] if current_existing_cust_ids else 'None'}")
    logger.info(f"Recon ETL for {source_file_being_processed}: Sample current_existing_prod_ids: {list(current_existing_prod_ids)[:5] if current_existing_prod_ids else 'None'}")
    logger.info(f"Recon ETL for {source_file_being_processed}: Sample current_prod_id_map (int_id -> canon_id): {dict(list(current_prod_id_map.items())[:5]) if current_prod_id_map else 'None'}")

    logger.info(f"Starting ETL for Order Items from reconciliation (source: {source_file_being_processed})...")
    if df_raw.empty:
        logger.warning(f"Raw recon data from {source_file_being_processed} is empty. Skipping.")
        return pd.DataFrame()
        
    df = df_raw.copy()
    pipeline_timestamp = get_current_timestamp_str()

    df.rename(columns={
        'client_reference': 'customer_id_source', 'transaction_ref': 'order_id_source',
        'item_reference': 'product_id_source_raw', 'transaction_date': 'order_date_source',
        'amount_paid': 'line_item_amount_paid_source', 'payment_status': 'payment_status_source',
        'delivery_status': 'delivery_status_source', 'quantity_ordered': 'quantity',
        'unit_cost': 'unit_price_source', 'total_value': 'total_value_provided',
        'discount_applied': 'line_item_discount_source', 'shipping_fee': 'line_item_shipping_fee_source',
        'tax_amount': 'line_item_tax_source', 'notes_comments': 'line_item_notes_original'
    }, inplace=True)

    df['order_id'] = df.get('order_id_source', pd.Series(dtype=object)).apply(lambda x: clean_string(x, 'upper') if pd.notna(x) else None)
    
    def map_recon_customer_id(client_ref_val, canonical_customer_ids_set_local):
        if pd.isna(client_ref_val): 
            return None
        
        cleaned_client_ref = clean_string(str(client_ref_val), 'upper')
        
        if cleaned_client_ref and cleaned_client_ref.startswith('CLI_'):
            potential_cust_id = cleaned_client_ref.replace('CLI_', 'CUST_') # This now matches the CUST_ prefix from etl_customers
            if potential_cust_id in canonical_customer_ids_set_local:
                return potential_cust_id

        # Also check if the cleaned_client_ref (if it was already in CUST_ format or other canonical format) matches
        if cleaned_client_ref in canonical_customer_ids_set_local:
            return cleaned_client_ref
        
        # logger.info(f"map_recon_customer_id (Recon): FAIL - No match for raw '{client_ref_val}' (cleaned: '{cleaned_client_ref}')")
        return None 
    
    df['customer_id'] = df.get('customer_id_source', pd.Series(dtype=object)).apply(
        lambda x: map_recon_customer_id(x, current_existing_cust_ids)
    )
    
    def map_recon_product_id_corrected(item_ref_val, product_int_to_canonical_map, canonical_prod_ids_set_local):
        if pd.isna(item_ref_val): return None
        
        cleaned_item_ref = clean_string(str(item_ref_val), 'upper')
        if not cleaned_item_ref: return None

        if cleaned_item_ref in canonical_prod_ids_set_local:
            return cleaned_item_ref
        
        item_num_str = None
        if cleaned_item_ref.startswith("ITM_"): 
            item_num_str = cleaned_item_ref.replace("ITM_", "")
        elif cleaned_item_ref.isdigit(): 
            item_num_str = cleaned_item_ref
        
        if item_num_str and item_num_str.isdigit():
            if item_num_str in product_int_to_canonical_map:
                return product_int_to_canonical_map[item_num_str]
                
        # logger.info(f"map_recon_product_id (Recon): FAIL - No match for raw '{item_ref_val}' (cleaned: '{cleaned_item_ref}')")
        return None

    df['product_id'] = df.get('product_id_source_raw', pd.Series(dtype=object)).apply(
        lambda x: map_recon_product_id_corrected(x, current_prod_id_map, current_existing_prod_ids)
    )

    initial_len = len(df)
    df.dropna(subset=['order_id', 'customer_id', 'product_id'], inplace=True) 
    if len(df) < initial_len: 
        logger.warning(f"Recon({source_file_being_processed}): Dropped {initial_len - len(df)} rows due to unmappable/missing key IDs (order_id, customer_id, or product_id).")
    if df.empty: 
        logger.warning(f"Recon({source_file_being_processed}): No valid records after ID mapping and NA drop of key IDs.")
        return pd.DataFrame()
    
    df['order_date'] = df.get('order_date_source', pd.Series(dtype=object)).apply(lambda x: parse_date_robustly(x, output_format='%Y-%m-%d %H:%M:%S'))
    df['quantity'] = df.get('quantity', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=1))
    df['unit_price'] = df.get('unit_price_source', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_total_value'] = df['quantity'] * df['unit_price']
    df['total_value_provided_numeric'] = df.get('total_value_provided', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float))
    discrepancy_check = ~np.isclose(df['line_item_total_value'], df['total_value_provided_numeric'].fillna(df['line_item_total_value']))
    if discrepancy_check.any(): logger.warning(f"{discrepancy_check.sum()} recon items from {source_file_being_processed} show discrepancy: calc total vs provided total.")
    df['line_item_discount'] = df.get('line_item_discount_source', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_shipping_fee'] = df.get('line_item_shipping_fee_source', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_tax'] = df.get('line_item_tax_source', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['line_item_amount_paid_final'] = df.get('line_item_amount_paid_source', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df['payment_status_derived'] = df.get('payment_status_source', pd.Series(dtype=object)).apply(lambda x: standardize_categorical(x, PAYMENT_STATUS_MAP, case_transform='upper'))
    df['delivery_status_derived'] = df.get('delivery_status_source', pd.Series(dtype=object)).apply(lambda x: standardize_categorical(x, ORDER_DELIVERY_STATUS_MAP, case_transform='upper'))
    df['line_item_notes'] = df.get('line_item_notes_original', pd.Series(dtype=object)).apply(lambda x: clean_string(x))
    df['original_line_identifier'] = df.get('order_id_source', pd.Series(dtype=str)).astype(str).fillna("NO_ORDER_ID_SRC") + "_RECON_" + \
                                  df.get('product_id_source_raw', pd.Series(dtype=str)).astype(str).fillna("NO_PROD_ID_SRC_RAW") + "_" + \
                                  df.index.astype(str)
    df['source_file_name'] = source_file_being_processed 
    df['last_updated_pipeline'] = pipeline_timestamp
    final_cols_for_recon_items_to_combine = [
        'order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price', 'line_item_total_value', 'line_item_discount', 
        'line_item_shipping_fee', 'line_item_tax', 'payment_status_derived', 'delivery_status_derived', 'line_item_notes', 
        'line_item_amount_paid_final', 'original_line_identifier', 'source_file_name', 'last_updated_pipeline']
    df_final = _ensure_df_columns(df, final_cols_for_recon_items_to_combine)
    logger.info(f"Finished ETL for Order Items from {source_file_being_processed}. Shape: {df_final.shape}")
    return df_final

def etl_order_items_from_unstructured(df_raw, source_file_being_processed, current_existing_cust_ids, current_existing_prod_ids, current_prod_id_map):
    logger.info(f"Unstructured ETL for {source_file_being_processed}: Sample current_existing_cust_ids: {list(current_existing_cust_ids)[:5] if current_existing_cust_ids else 'None'}")
    logger.info(f"Unstructured ETL for {source_file_being_processed}: Sample current_existing_prod_ids: {list(current_existing_prod_ids)[:5] if current_existing_prod_ids else 'None'}")
    logger.info(f"Unstructured ETL for {source_file_being_processed}: Sample current_prod_id_map (int_id -> canon_id): {dict(list(current_prod_id_map.items())[:5]) if current_prod_id_map else 'None'}")
    ZFILL_LENGTH = 4 # Ensure this matches etl_customers

    logger.info(f"Starting ETL for Order Items from unstructured (source: {source_file_being_processed})...")
    if df_raw.empty: logger.warning(f"Raw unstructured order data from {source_file_being_processed} is empty."); return pd.DataFrame()
    df_working = df_raw.copy(); pipeline_timestamp = get_current_timestamp_str()
    df_working['order_id'] = df_raw.get('order_id', pd.Series(dtype=object)).fillna(df_raw.get('ord_id', pd.Series(dtype=object)).astype(str)).apply(lambda x: clean_string(x, 'upper') if pd.notna(x) else None)
    df_working['source_order_id_int_val'] = df_raw.get('ord_id', pd.Series(dtype=object)).fillna(df_raw.get('order_id', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(re.sub(r'\D', '', str(x)), target_type=int) if pd.notna(x) else pd.NA)).astype('Int64')
    
    # Customer ID derivation logic (consistent with etl_customers creating CUST_ prefixed IDs)
    customer_id_str_source = df_raw.get('cust_id', pd.Series(dtype=str)).astype(str).apply(lambda x: clean_string(x, 'upper') if pd.notna(x) else None)
    customer_id_int_source = df_raw.get('customer_id', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=int)) # This is the numeric 'customer_id' column
    
    def derive_unstructured_customer_id(row):
        str_id = row['customer_id_str_source_col'] # Using a temp name to avoid conflict
        int_id = row['customer_id_int_source_col']

        if pd.notna(str_id) and str_id.strip() != "":
            if str_id.startswith("CUST_") or not str_id.isdigit(): # Already canonical or non-numeric string
                return str_id
            else: # It's a plain numeric string, prefix it like in etl_customers
                try:
                    return f"CUST_{str(int(float(str_id))).zfill(ZFILL_LENGTH)}"
                except ValueError:
                     return str_id # Should not happen if isdigit() was true
        if pd.notna(int_id):
            return f"CUST_{str(int_id).zfill(ZFILL_LENGTH)}"
        return None

    df_working['customer_id_str_source_col'] = customer_id_str_source
    df_working['customer_id_int_source_col'] = customer_id_int_source
    df_working['customer_id_derived_temp'] = df_working.apply(derive_unstructured_customer_id, axis=1)
    df_working.drop(columns=['customer_id_str_source_col', 'customer_id_int_source_col'], inplace=True)


    def resolve_unstructured_product_id(row_from_raw_data, product_id_lookup_map, canonical_product_id_set):
        prod_id_val = clean_string(row_from_raw_data.get('product_id'), 'upper')
        item_id_val = row_from_raw_data.get('item_id')
        item_id_val_str = str(int(item_id_val)) if pd.notna(item_id_val) and isinstance(item_id_val, (int, float)) and not pd.isna(item_id_val) else str(item_id_val) if pd.notna(item_id_val) else None
        if prod_id_val and prod_id_val in canonical_product_id_set: return prod_id_val
        if item_id_val_str and item_id_val_str in product_id_lookup_map: return product_id_lookup_map[item_id_val_str]
        if item_id_val_str and item_id_val_str in canonical_product_id_set: return item_id_val_str                                        
        # logger.info(f"resolve_unstructured_product_id: FAIL - No match for raw prod_id '{row_from_raw_data.get('product_id')}' or item_id '{item_id_val}'")
        return None
    df_working['product_id_derived_temp'] = df_raw.apply(lambda row: resolve_unstructured_product_id(row, current_prod_id_map, current_existing_prod_ids), axis=1)
    
    initial_len_full = len(df_working); df_working.dropna(subset=['order_id', 'customer_id_derived_temp', 'product_id_derived_temp'], inplace=True)
    if len(df_working) < initial_len_full: logger.warning(f"Unstructured({source_file_being_processed}): Dropped {initial_len_full - len(df_working)} rows due to missing key IDs before further filtering.")
    if df_working.empty: logger.warning(f"Unstructured({source_file_being_processed}): No records after initial key ID NA drop."); return pd.DataFrame()
    
    initial_len_after_na = len(df_working)
    df_working = df_working[df_working['customer_id_derived_temp'].isin(current_existing_cust_ids)].copy()
    if len(df_working) < initial_len_after_na: logger.warning(f"Unstructured({source_file_being_processed}): Dropped {initial_len_after_na - len(df_working)} rows: derived_customer_id not in known Customers set.")
    if df_working.empty: logger.warning(f"Unstructured({source_file_being_processed}): No records after customer_id filter vs known Customers."); return pd.DataFrame()
    
    initial_len_after_cust_filter = len(df_working)
    df_working = df_working[df_working['product_id_derived_temp'].isin(current_existing_prod_ids)].copy()
    if len(df_working) < initial_len_after_cust_filter: logger.warning(f"Unstructured({source_file_being_processed}): Dropped {initial_len_after_cust_filter - len(df_working)} rows: derived_product_id not in known Products set.")
    if df_working.empty: logger.warning(f"Unstructured({source_file_being_processed}): No records after product_id filter vs known Products."); return pd.DataFrame()
    
    df_working['customer_id'] = df_working['customer_id_derived_temp']; df_working['product_id'] = df_working['product_id_derived_temp']
    df_working_index = df_working.index 
    df_working['order_date'] = df_raw.loc[df_working_index].get('order_datetime', pd.Series(dtype=object)).fillna(df_raw.loc[df_working_index].get('order_date', pd.Series(dtype=object))).apply(lambda x: parse_date_robustly(x, output_format='%Y-%m-%d %H:%M:%S'))
    df_working['quantity'] = df_raw.loc[df_working_index].get('quantity', pd.Series(dtype=object)).fillna(df_raw.loc[df_working_index].get('qty', pd.Series(dtype=object))).apply(lambda x: to_numeric_safe(x, target_type=int, default_value=1))
    df_working['unit_price'] = df_raw.loc[df_working_index].get('unit_price', pd.Series(dtype=object)).fillna(df_raw.loc[df_working_index].get('price', pd.Series(dtype=object))).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df_working['calculated_line_total'] = df_working['quantity'] * df_working['unit_price']
    df_working['line_item_total_value'] = df_raw.loc[df_working_index].get('total_amount', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float)).fillna(df_working['calculated_line_total'])
    df_working['line_item_discount'] = df_raw.loc[df_working_index].get('discount', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df_working['line_item_tax'] = df_raw.loc[df_working_index].get('tax', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df_working['line_item_shipping_fee'] = df_raw.loc[df_working_index].get('shipping_cost', pd.Series(dtype=object)).apply(lambda x: to_numeric_safe(x, target_type=float, default_value=0.0))
    df_working['line_item_amount_paid_final'] = (df_working['line_item_total_value'].fillna(0) - df_working['line_item_discount'].fillna(0) + df_working['line_item_tax'].fillna(0) + df_working['line_item_shipping_fee'].fillna(0))
    status_temp = df_raw.loc[df_working_index].get('status', pd.Series(dtype=object)).replace('',pd.NA); order_status_temp = df_raw.loc[df_working_index].get('order_status', pd.Series(dtype=object)).replace('',pd.NA)
    df_working['overall_item_status_derived'] = order_status_temp.fillna(status_temp).apply(lambda x: standardize_categorical(x, ORDER_DELIVERY_STATUS_MAP, default_value=DEFAULT_STATUS_UNKNOWN, case_transform='upper'))
    df_working['payment_method_source'] = df_raw.loc[df_working_index].get('payment_method', pd.Series(dtype=object)).apply(lambda x: clean_string(x, 'lower', DEFAULT_UNKNOWN_CATEGORICAL))
    df_working['shipping_address_full_source'] = df_raw.loc[df_working_index].get('shipping_address', pd.Series(dtype=object)).apply(clean_string)
    df_working['line_item_notes'] = df_raw.loc[df_working_index].get('notes', pd.Series(dtype=object)).apply(clean_string)
    df_working['tracking_number_source'] = df_raw.loc[df_working_index].get('tracking_number', pd.Series(dtype=object)).apply(clean_string)
    original_line_id_series = df_working['order_id'].astype(str) + "_UNSTR_" + df_working['product_id'].astype(str) + "_" + df_raw.loc[df_working_index].get('item_id', pd.Series(dtype=str)).astype(str).fillna("NO_ITEM_ID") + "_" + df_working_index.astype(str)
    df_working['original_line_identifier'] = original_line_id_series
    df_working['source_file_name'] = source_file_being_processed; df_working['last_updated_pipeline'] = pipeline_timestamp
    final_cols_for_unstructured_items_to_combine = [
        'order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price', 'line_item_total_value', 'line_item_discount', 
        'line_item_shipping_fee', 'line_item_tax', 'overall_item_status_derived', 'payment_method_source', 'shipping_address_full_source', 
        'line_item_notes', 'tracking_number_source', 'source_order_id_int_val', 'line_item_amount_paid_final', 'original_line_identifier',
        'source_file_name', 'last_updated_pipeline']
    if 'source_order_id_int' in df_working.columns and 'source_order_id_int_val' not in df_working.columns: 
        df_working.rename(columns={'source_order_id_int': 'source_order_id_int_val'}, inplace=True)
    df_final = _ensure_df_columns(df_working, final_cols_for_unstructured_items_to_combine)
    logger.info(f"Finished ETL for Order Items from {source_file_being_processed}. Shape: {df_final.shape}")
    return df_final

def etl_combine_orders_and_create_orders_table(df_items_list, source_file_names_of_item_batches_UNUSED, current_existing_cust_ids_for_orders):
    logger.info(f"Starting to combine {len(df_items_list)} order item DataFrames and derive Orders table data...")
    pipeline_timestamp = get_current_timestamp_str()
    if not df_items_list or all(df is None or df.empty for df in df_items_list):
        logger.warning("No order item DataFrames to combine or all are empty. Cannot proceed."); return pd.DataFrame(), pd.DataFrame()
    standardized_item_dfs = []
    for i, df_source_item_batch in enumerate(df_items_list):
        if df_source_item_batch is None or df_source_item_batch.empty: continue
        temp_df = df_source_item_batch.copy()
        if 'source_file_name' not in temp_df.columns: 
            temp_df['source_file_name'] = f"MISSING_SOURCE_IN_ITEM_BATCH_{i}"; logger.error(f"CRITICAL: Item batch {i} missing 'source_file_name'.")
        expected_cols_from_item_etls = [ 
            'order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price', 'line_item_total_value', 'line_item_discount', 
            'line_item_shipping_fee', 'line_item_tax', 'payment_status_derived', 'delivery_status_derived', 'overall_item_status_derived', 
            'line_item_notes', 'line_item_amount_paid_final', 'payment_method_source', 'shipping_address_full_source', 
            'tracking_number_source', 'source_order_id_int_val', 'source_file_name', 'original_line_identifier', 'last_updated_pipeline']
        current_batch_processed = _ensure_df_columns(temp_df, expected_cols_from_item_etls)
        standardized_item_dfs.append(current_batch_processed)
    if not standardized_item_dfs: logger.warning("No valid item dataframes to combine after standardization."); return pd.DataFrame(), pd.DataFrame()
    df_all_order_items = pd.concat(standardized_item_dfs, ignore_index=True); logger.info(f"Combined all order items. Initial shape: {df_all_order_items.shape}")
    numeric_agg_cols = ['line_item_shipping_fee', 'line_item_tax', 'line_item_discount', 'line_item_total_value', 'line_item_amount_paid_final']
    for col in numeric_agg_cols: df_all_order_items[col] = pd.to_numeric(df_all_order_items[col], errors='coerce').fillna(0.0)
    df_orders = df_all_order_items.groupby('order_id', as_index=False, sort=False).agg(
        customer_id=('customer_id', 'first'), source_file_name=('source_file_name', 'first'), order_date=('order_date', 'min'), 
        order_status=('overall_item_status_derived', lambda x: x.mode(dropna=False).iat[0] if not x.mode(dropna=False).empty else DEFAULT_STATUS_UNKNOWN),
        payment_method=('payment_method_source', lambda x: x.dropna().iloc[0] if not x.dropna().empty else DEFAULT_UNKNOWN_CATEGORICAL),
        payment_status=('payment_status_derived', lambda x: x.dropna().iloc[0] if not x.dropna().empty else DEFAULT_STATUS_UNKNOWN),
        delivery_status=('delivery_status_derived', lambda x: x.dropna().iloc[0] if not x.dropna().empty else DEFAULT_STATUS_UNKNOWN),
        shipping_address_full=('shipping_address_full_source', 'first'), shipping_cost_total=('line_item_shipping_fee', 'sum'),
        tax_total=('line_item_tax', 'sum'), discount_total=('line_item_discount', 'sum'),
        order_total_value_gross=('line_item_total_value', 'sum'), amount_paid_total=('line_item_amount_paid_final', 'sum'),
        tracking_number=('tracking_number_source', 'first'),
        notes=('line_item_notes', lambda x: '; '.join(sorted(list(x.dropna().astype(str).unique()))) if not x.dropna().empty and x.dropna().astype(str).str.len().sum() > 0 else None),
        source_order_id_int=('source_order_id_int_val', 'first'))
    df_orders['order_total_value_net'] = df_orders['order_total_value_gross'] - df_orders['discount_total']
    df_orders['last_updated_pipeline'] = pipeline_timestamp; initial_order_count = len(df_orders)
    if 'customer_id' in df_orders.columns and df_orders['customer_id'].notna().any():
        valid_cust_ids_set = set(map(str, current_existing_cust_ids_for_orders))
        df_orders = df_orders[df_orders['customer_id'].astype(str).isin(valid_cust_ids_set)].copy()
        if initial_order_count > len(df_orders): logger.warning(f"Derived Orders: Dropped {initial_order_count - len(df_orders)} orders due to customer_id not in Customers table.")
    logger.info(f"Derived Orders table. Shape: {df_orders.shape}")
    final_order_item_db_cols_ordered = [
        'order_id', 'product_id', 'customer_id', 'source_file_name', 'quantity', 'unit_price', 'line_item_total_value', 
        'line_item_discount', 'line_item_tax', 'line_item_shipping_fee', 'original_line_identifier', 'last_updated_pipeline']
    df_order_items_for_db = _ensure_df_columns(df_all_order_items, final_order_item_db_cols_ordered)
    if not df_orders.empty and 'order_id' in df_orders.columns:
        df_order_items_for_db = df_order_items_for_db[df_order_items_for_db['order_id'].isin(df_orders['order_id'])].copy()
    else:
        logger.warning("Orders DataFrame empty or missing 'order_id'; OrderItems for DB will be empty.")
        df_order_items_for_db = pd.DataFrame(columns=final_order_item_db_cols_ordered)
    logger.info(f"Final OrderItems for DB. Shape: {df_order_items_for_db.shape}")
    return df_order_items_for_db, df_orders

logger.info("ETL pipeline functions defined in src/etl_pipelines.py.")