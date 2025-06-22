# streamlit_app/pages/08_Process_Uploaded_Files.py
import streamlit as st
import pandas as pd
import os
import sys

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(streamlit_app_dir)

if project_root not in sys.path: sys.path.append(project_root)
if streamlit_app_dir not in sys.path: sys.path.append(streamlit_app_dir)

from src.db_utils import get_db_engine
from src.config import (logger, EXPECTED_RAW_COLS_CUSTOMER, 
                        EXPECTED_RAW_COLS_PRODUCT, EXPECTED_RAW_COLS_ORDER)
from src.etl_runner import run_etl_for_registered_file
from src.main_etl import load_single_raw_data # To load a sample

st.title("⚙️ Process Registered Data Files")
st.markdown("Select a file and its entity type to run the ETL pipeline. Schema differences will be highlighted.")

db_engine = get_db_engine()

@st.cache_data(ttl=15)
def get_files_for_processing_p08(): # Renamed to avoid conflict if another page has similar func
    if not db_engine: st.error("Database connection not available."); return pd.DataFrame()
    try:
        query = """
            SELECT file_id, file_name, file_path, upload_timestamp, 
                   processing_status, entity_type_guess, row_count, col_count 
            FROM SourceFileRegistry 
            WHERE processing_status IN ('raw_uploaded', 'profiled', 
                                        'error_processing', 'error_entity_unknown', 
                                        'error_schema_mismatch')
            ORDER BY upload_timestamp DESC
        """
        return pd.read_sql_query(query, db_engine)
    except Exception as e: st.error(f"Error fetching files: {e}"); return pd.DataFrame()

files_to_process_df = get_files_for_processing_p08()

if files_to_process_df.empty:
    st.info("No files currently pending processing or requiring attention.")
else:
    files_to_process_df["display_label"] = files_to_process_df["file_name"] + \
                                       " (Status: " + files_to_process_df["processing_status"] + \
                                       ", Guessed Entity: " + files_to_process_df["entity_type_guess"].fillna("N/A") + ")"
    
    file_options_dict = pd.Series(files_to_process_df.file_id.values, index=files_to_process_df.display_label).to_dict()
    
    selected_display_label = st.selectbox(
        "Select File to Process:",
        options=list(file_options_dict.keys()),
        index=0 if not files_to_process_df.empty else None,
        key="file_to_process_select_p08" # Unique key for this page
    )

    if selected_display_label:
        selected_file_id = file_options_dict[selected_display_label]
        selected_file_info = files_to_process_df[files_to_process_df["file_id"] == selected_file_id].iloc[0]
        
        st.markdown("---")
        st.subheader(f"Preparing to process: {selected_file_info['file_name']}")
        st.caption(f"Path: `{selected_file_info['file_path']}` | Status: `{selected_file_info['processing_status']}`")

        entity_types_available_ui = ["(Select Entity Type)", "Customer", "Product", "Order Items (Unstructured)", "Order Items (Reconciliation)", "Order (Generic)"]
        guessed_entity = selected_file_info['entity_type_guess']
        default_index_ui = 0
        if guessed_entity and pd.notna(guessed_entity):
            try:
                # Attempt to match guess to UI options
                normalized_guessed_entity = guessed_entity.lower().replace("_", " ")
                ui_options_lower = [opt.lower() for opt in entity_types_available_ui]
                if normalized_guessed_entity in ui_options_lower:
                     default_index_ui = ui_options_lower.index(normalized_guessed_entity)
            except ValueError: pass

        chosen_entity_type_ui = st.selectbox(
            "**Step 1: Confirm or Select the Entity Type for this file:**",
            options=entity_types_available_ui,
            index=default_index_ui,
            key=f"entity_type_select_p08_{selected_file_id}"
        )

        if chosen_entity_type_ui != "(Select Entity Type)":
            st.markdown("**Step 2: Review Schema and Confirm Processing**")
            
            # Load a sample of the raw file for schema check
            df_sample_raw = load_single_raw_data(selected_file_info['file_path']) # Load full for accurate column list
            
            if df_sample_raw.empty:
                st.error(f"Could not load data from {selected_file_info['file_name']} for schema review.")
            else:
                raw_file_cols_lower = [col.lower() for col in df_sample_raw.columns]
                
                expected_cols_map = None
                entity_category_for_check = None # For check_essential_columns
                
                ui_entity_lower = chosen_entity_type_ui.lower()
                if ui_entity_lower == 'customer':
                    expected_cols_map = EXPECTED_RAW_COLS_CUSTOMER
                    entity_category_for_check = 'customer'
                elif ui_entity_lower == 'product':
                    expected_cols_map = EXPECTED_RAW_COLS_PRODUCT
                    entity_category_for_check = 'product'
                elif 'order' in ui_entity_lower: # Covers all order types for this check
                    expected_cols_map = EXPECTED_RAW_COLS_ORDER
                    entity_category_for_check = 'order'

                missing_essential_groups = []
                extra_cols_detected = list(raw_file_cols_lower) # Start with all, then remove expected

                if expected_cols_map:
                    st.write(f"**Schema Check for '{chosen_entity_type_ui}' Type:**")
                    all_expected_variants_flat = set()
                    for category, variants in expected_cols_map.items():
                        all_expected_variants_flat.update(variants)
                        # Check if at least one variant from this essential category is present
                        if not any(variant in raw_file_cols_lower for variant in variants):
                            missing_essential_groups.append(f"'{category}' (e.g., one of: {', '.join(variants[:3])}{', ...' if len(variants) > 3 else ''})")
                        
                        # Remove found expected columns from extra_cols_detected
                        for variant in variants:
                            if variant in extra_cols_detected:
                                extra_cols_detected.remove(variant)
                    
                    if missing_essential_groups:
                        st.warning(f"**Potential Issue:** The file seems to be missing essential data groups for a '{chosen_entity_type_ui}' entity: **{', '.join(missing_essential_groups)}**. Processing might lead to poor quality data or errors.")
                    else:
                        st.success(f"Essential column groups for '{chosen_entity_type_ui}' appear to be present.")

                    # List original column names for extras for better user readability
                    original_extra_cols = [orig_col for orig_col, lower_col in zip(df_sample_raw.columns, raw_file_cols_lower) if lower_col in extra_cols_detected]
                    if original_extra_cols:
                        st.info(f"**Note:** The file also contains these extra columns not typically standard for '{chosen_entity_type_ui}': `{', '.join(original_extra_cols)}`. These columns will be **ignored** by the current ETL process.")
                    else:
                        st.caption("No unexpected extra columns detected based on common variants.")
                else:
                    st.caption("No predefined expected columns for this entity type to check against.")


                proceed_button_label = f"Process '{selected_file_info['file_name']}' as '{chosen_entity_type_ui}'"
                if missing_essential_groups:
                    proceed_button_label += " (with missing essentials)"
                
                if st.button(proceed_button_label, type="primary", key=f"run_etl_final_confirm_{selected_file_id}"):
                    # Map UI choice to internal entity type string for etl_runner
                    etl_entity_type_param = chosen_entity_type_ui.lower().replace(" ", "_").replace("(", "").replace(")", "")
                    # Make sure it matches one of the keys etl_runner expects
                    if etl_entity_type_param == "order_generic": etl_entity_type_param = "order" 
                                        
                    with st.spinner(f"Processing {selected_file_info['file_name']} as {chosen_entity_type_ui}..."):
                        success, message = run_etl_for_registered_file(selected_file_id, etl_entity_type_param)
                    
                    if success:
                        st.toast(f"✅ ETL for '{selected_file_info['file_name']}' completed successfully! {message}", icon="🎉")
                        st.success(f"ETL for '{selected_file_info['file_name']}' completed. Message: {message}")
                    else:
                        if "Pre-flight check failed:" in message: # This message is from etl_runner if internal check fails
                            st.toast(f"⚠️ {message}", icon="🛑") 
                            st.error(message) 
                        else:
                            st.toast(f"❌ ETL for '{selected_file_info['file_name']}' failed. {message}", icon="🚨")
                            st.error(f"ETL for '{selected_file_info['file_name']}' failed. Message: {message}")
                    
                    st.cache_data.clear() 
                    st.rerun()
    st.markdown("---")