# streamlit_app/pages/07_File_Upload.py
import streamlit as st
import os
from datetime import datetime
import sys
import pandas as pd # Required for pd.read_sql_query if used directly

# Add project root to sys.path to allow imports from src and other app modules
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(streamlit_app_dir)

if project_root not in sys.path:
    sys.path.append(project_root)
if streamlit_app_dir not in sys.path: 
    sys.path.append(streamlit_app_dir)

from src.config import logger
from src.db_utils import get_db_engine, register_uploaded_file_in_db # register_uploaded_file_in_db is now here
from src.file_utils import basic_profiler # basic_profiler is now here
# from app import fetch_data # Not strictly needed here unless displaying other DB data

# --- Configuration for Uploads ---
UPLOAD_DIR = os.path.join(project_root, "data", "uploads_new") # Changed name to avoid conflict if old exists
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    logger.info(f"Created upload directory: {UPLOAD_DIR}")

# --- Streamlit Page UI ---
st.set_page_config(page_title="Upload New Data Files", layout="wide") # Called only once per app, usually in main app.py
                                                                    # If this is a page, it might conflict.
                                                                    # Let's assume main app.py handles set_page_config.
st.title("📂 Upload New Data Files")
st.markdown("Upload one or more CSV or JSON files. These will be registered and available for analytics.")

# File Uploader
uploaded_files = st.file_uploader(
    "Choose files (CSV or JSON recommended)",
    type=["csv", "json", "txt"], # Allow txt as it might be CSV-like
    accept_multiple_files=True,
    key="multi_file_uploader_07" # Unique key for this uploader instance
)

if 'processed_files_info' not in st.session_state:
    st.session_state.processed_files_info = []


if uploaded_files:
    # Display files to be uploaded before hitting the button
    st.write(f"**{len(uploaded_files)} file(s) selected for upload:**")
    for up_file in uploaded_files:
        st.write(f"- {up_file.name} ({up_file.size / 1024:.2f} KB)")
    st.markdown("---")

    if st.button("Start Upload and Registration Process", type="primary", key="start_upload_button"):
        db_engine = get_db_engine()
        if not db_engine:
            st.error("Database connection failed. Cannot register files.")
        else:
            # Clear previous run info
            st.session_state.processed_files_info = [] 
            
            st.subheader("Upload Progress:")
            overall_progress_bar = st.progress(0)
            
            # Use columns for better layout of individual file progress
            # status_cols = st.columns(len(uploaded_files) if uploaded_files else 1)

            for i, uploaded_file_obj in enumerate(uploaded_files):
                original_file_name = uploaded_file_obj.name
                # Sanitize or make filename unique on server to prevent overwrites / path traversal
                # For now, using original name in a dedicated UPLOAD_DIR
                server_file_path = os.path.join(UPLOAD_DIR, original_file_name)
                file_size = uploaded_file_obj.size
                
                # Individual file status update
                with st.spinner(f"Processing {original_file_name}..."):
                    file_info = {"name": original_file_name, "status": "Processing..."}
                    
                    # 1. Save the file
                    try:
                        with open(server_file_path, "wb") as f:
                            f.write(uploaded_file_obj.getbuffer())
                        file_info["save_status"] = f"Saved to server at {server_file_path}"
                        
                        # 2. Basic Profiling
                        rows, cols = basic_profiler(server_file_path)
                        file_info["profile_status"] = f"Profiled: Rows={rows if rows is not None else 'N/A'}, Cols={cols if cols is not None else 'N/A'}"
                        
                        # 3. Register in Database
                        registered, message = register_uploaded_file_in_db(
                            db_engine,
                            original_file_name,
                            server_file_path, 
                            file_size,
                            entity_type_guess="unknown", # TODO: Implement better guessing
                            row_count=rows,
                            col_count=cols
                        )
                        file_info["db_status"] = message
                        if registered:
                            file_info["status"] = "Successfully processed and registered."
                        else:
                            file_info["status"] = f"Processed with issues: {message}"
                            
                    except Exception as e:
                        error_msg = f"Error processing '{original_file_name}': {str(e)}"
                        logger.error(f"Error handling uploaded file '{original_file_name}': {e}", exc_info=True)
                        file_info["status"] = "Error during processing."
                        file_info["error_details"] = error_msg
                
                st.session_state.processed_files_info.append(file_info)
                overall_progress_bar.progress((i + 1) / len(uploaded_files))
            
            # Clear the uploader's internal state so it doesn't show old files on rerun
            # This is a bit of a workaround for st.file_uploader's persistence.
            # A common way is to change its key or use st.experimental_rerun(),
            # or manage the list of uploaded_files in session_state and clear it.
            # For simplicity, we are not clearing `uploaded_files` here directly, user has to re-select for new batch.
            # The `processed_files_info` will show results of current batch.

    # Display results from st.session_state.processed_files_info
    if st.session_state.processed_files_info:
        st.markdown("---")
        st.subheader("Upload Summary:")
        for info in st.session_state.processed_files_info:
            if "Successfully" in info["status"]:
                st.success(f"**{info['name']}**: {info['status']}")
            elif "Error" in info["status"]:
                st.error(f"**{info['name']}**: {info['status']}")
                if "error_details" in info: st.caption(f"Details: {info['error_details']}")
            else: # Warnings or other messages
                st.warning(f"**{info['name']}**: {info['status']}")
            
            with st.expander("Show details", expanded=False):
                if "save_status" in info: st.write(f"- Save: {info['save_status']}")
                if "profile_status" in info: st.write(f"- Profile: {info['profile_status']}")
                if "db_status" in info: st.write(f"- Database: {info['db_status']}")
        
        if st.button("Clear Results and Upload New Batch", key="clear_results_button"):
            st.session_state.processed_files_info = []
            # This doesn't clear the file_uploader widget itself if files are still selected.
            # To truly reset the file_uploader, its key needs to change or the page needs a full rerun
            # in a way that resets its state.
            st.rerun()


st.markdown("---")
st.caption("Uploaded files, once registered, can be analyzed via the 'Source File Analytics' page (after it's updated to use the registry).")