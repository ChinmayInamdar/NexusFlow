# streamlit_app/pages/06_Source_File_Analytics.py
import streamlit as st
import pandas as pd
import os
import sys
import plotly.express as px

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir) # Goes to streamlit_app directory
project_root = os.path.dirname(streamlit_app_dir) # Goes to nexusflow_project directory

if project_root not in sys.path:
    sys.path.append(project_root)
if streamlit_app_dir not in sys.path: # To import app.py if needed by other pages
    sys.path.append(streamlit_app_dir)

from src.db_utils import get_db_engine
from src.config import logger
# from app import fetch_data # Using direct SQL for SourceFileRegistry

st.title("📄 Source File Analytics (Cleaned Data View)")
st.markdown("""
Select a **processed** source file to view its cleaned data as stored in the main database tables. 
This page shows the *result* of the ETL process for a specific file.
""")

db_engine = get_db_engine()

@st.cache_data(ttl=30) # Cache for 30 seconds to reflect recent processing
def get_files_from_registry_for_analytics():
    if not db_engine:
        st.error("Database connection not available.")
        return pd.DataFrame()
    try:
        # Fetch files, including those processed or with errors after processing attempt
        query = """
            SELECT file_id, file_name, upload_timestamp, processing_status, entity_type_guess 
            FROM SourceFileRegistry 
            ORDER BY upload_timestamp DESC
        """
        df_files = pd.read_sql_query(query, db_engine)
        return df_files
    except Exception as e:
        st.error(f"Error fetching file list from registry: {e}")
        logger.error(f"Error fetching file list from registry for analytics page: {e}", exc_info=True)
        return pd.DataFrame()

files_df = get_files_from_registry_for_analytics()

if files_df.empty:
    st.warning("No files found in the Source File Registry. Please upload and process files first.")
else:
    files_df["display_label"] = files_df["file_name"] + " (Status: " + \
                               files_df["processing_status"] + \
                               ", Guessed Entity: " + files_df["entity_type_guess"].fillna("N/A") + ")"
    
    # Make file_id the value for the selectbox options for direct use
    file_options_map = pd.Series(files_df.file_id.values, index=files_df.display_label).to_dict()
    
    selected_display_label = st.selectbox(
        "Select a Source File:",
        options=list(file_options_map.keys()), # Use display labels as options
        index=0 if not files_df.empty else None,
        key="source_file_analytics_select"
    )

    if selected_display_label:
        selected_file_id = file_options_map[selected_display_label]
        selected_file_info = files_df[files_df["file_id"] == selected_file_id].iloc[0]
        
        # This is the name stored in Customers.source_file_name, Products.source_file_name etc.
        source_file_name_to_filter_by = selected_file_info['file_name'] 
        db_guessed_entity_type = selected_file_info['entity_type_guess']
        processing_status = selected_file_info['processing_status']

        st.markdown("---")
        st.subheader(f"Displaying Cleaned Data for: {source_file_name_to_filter_by}")
        st.caption(f"File ID: `{selected_file_id}` | Processing Status: `{processing_status}` | DB Guessed Entity: `{db_guessed_entity_type if pd.notna(db_guessed_entity_type) else 'Unknown'}`")

        # Determine which entity type to try and view data for
        entity_type_for_viewing = db_guessed_entity_type
        if not entity_type_for_viewing or str(entity_type_for_viewing).lower() == 'unknown':
            st.info("The system's best guess for this file's entity type is 'Unknown' or not set. You can try selecting an entity type to view its data if it was processed as such.")
            temp_entity_choice = st.selectbox(
               "Attempt to view as entity type:",
               options=["(Auto-detect or select type)", "Customer", "Product", "Orders", "OrderItems"],
               key=f"temp_entity_view_select_{selected_file_id}"
            )
            if temp_entity_choice != "(Auto-detect or select type)":
                entity_type_for_viewing = temp_entity_choice
        
        table_to_query = None
        if entity_type_for_viewing and str(entity_type_for_viewing).lower() != 'unknown':
            entity_lower = str(entity_type_for_viewing).lower()
            if entity_lower == 'customer': table_to_query = 'Customers'
            elif entity_lower == 'product': table_to_query = 'Products'
            elif entity_lower == 'orders': table_to_query = 'Orders' # This will show aggregated Orders
            elif entity_lower == 'orderitems' or 'order_items' in entity_lower: table_to_query = 'OrderItems' # For item details
            # Add more mappings if your entity_type_guess uses different terms
        
        if not table_to_query:
            st.markdown("Please ensure the file has been processed with a known entity type, or select an entity type above to attempt viewing its data.")
        else:
            @st.cache_data(ttl=120) # Cache loaded data for 2 minutes
            def load_cleaned_data_by_source_from_table(target_table_name, source_file_name_filter):
                if not db_engine: return pd.DataFrame()
                try:
                    logger.info(f"Loading cleaned data for source '{source_file_name_filter}' from table '{target_table_name}'")
                    # Query the main table, filtering by source_file_name
                    query = f'SELECT * FROM "{target_table_name}" WHERE "source_file_name" = ?'
                    df = pd.read_sql_query(query, db_engine, params=(source_file_name_filter,))
                    if df.empty:
                        logger.warning(f"No data found in '{target_table_name}' for source_file_name '{source_file_name_filter}'.")
                    return df
                except Exception as e:
                    st.error(f"Error loading data from {target_table_name} for {source_file_name_filter}: {e}")
                    logger.error(f"Error loading data from {target_table_name} for {source_file_name_filter}: {e}", exc_info=True)
                    return pd.DataFrame()

            df_cleaned_data_view = load_cleaned_data_by_source_from_table(table_to_query, source_file_name_to_filter_by)

            if not df_cleaned_data_view.empty:
                st.markdown(f"#### Preview of Cleaned Data from `{table_to_query}` (Max 50 Rows from this source)")
                
                # Prepare for display: explicitly cast object columns to string for Arrow compatibility
                df_display_safe = df_cleaned_data_view.head(50).copy()
                for col in df_display_safe.select_dtypes(include=['object']).columns:
                    try: df_display_safe[col] = df_display_safe[col].astype(str)
                    except Exception: pass # Ignore if conversion fails for display
                st.dataframe(df_display_safe, use_container_width=True)

                st.markdown(f"#### Basic Statistics (from Cleaned `{table_to_query}` Data for this Source)")
                try:
                    # For pandas < 1.0, datetime_is_numeric is not valid.
                    # For pandas >= 1.5, it defaults to True.
                    # Let's try without it first for broader compatibility, then add try-except if specific versions need it.
                    described_df = df_cleaned_data_view.describe(include='all')
                    described_df_transposed = described_df.transpose()
                    
                    described_df_displayable = described_df_transposed.copy()
                    for col in described_df_displayable.select_dtypes(include=['object']).columns:
                        described_df_displayable[col] = described_df_displayable[col].astype(str)
                    if described_df_displayable.index.dtype == 'object':
                        described_df_displayable.index = described_df_displayable.index.astype(str)
                        
                    st.dataframe(described_df_displayable, use_container_width=True)
                except Exception as e_stat:
                    st.error(f"Could not display basic statistics: {e_stat}")
                    logger.error(f"Error displaying statistics for {source_file_name_to_filter_by} from {table_to_query}: {e_stat}", exc_info=True)

                st.markdown(f"#### Column Visualizations (Sample from Cleaned `{table_to_query}` Data for this Source)")
                if not df_cleaned_data_view.empty:
                    column_to_visualize = st.selectbox(
                        "Select a column to visualize:",
                        options=df_cleaned_data_view.columns.tolist(),
                        key=f"viz_select_{selected_file_id}_{table_to_query}" # Unique key
                    )
                    if column_to_visualize:
                        st.write(f"Value counts for **{column_to_visualize}** (Top 20):")
                        try:
                            counts = df_cleaned_data_view[column_to_visualize].astype(str).value_counts().nlargest(20)
                            if not counts.empty:
                                fig = px.bar(counts, x=counts.index, y=counts.values, labels={'x': column_to_visualize, 'y': 'Count'})
                                fig.update_layout(xaxis_title=column_to_visualize, yaxis_title="Frequency")
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.write("No data to visualize for this column (or all values are unique beyond top 20).")
                        except Exception as e_viz:
                            st.error(f"Could not generate visualization for {column_to_visualize}: {e_viz}")
                            logger.error(f"Viz error for {column_to_visualize} in {source_file_name_to_filter_by} from {table_to_query}: {e_viz}", exc_info=True)
                else:
                    st.write(f"Cleaned data from {table_to_query} (source: {source_file_name_to_filter_by}) is empty, no visualizations possible.")
            elif table_to_query : # table_to_query was set, but no data found
                st.info(f"No cleaned data found in table '{table_to_query}' for the source file '{source_file_name_to_filter_by}'. "
                        f"This could mean the file was not processed for this entity type, yielded no data after cleaning, "
                        f"or there was an issue during its ETL processing (Status: {processing_status}).")