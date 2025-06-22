# streamlit_app/pages/06_Source_File_Analytics.py
import streamlit as st
import pandas as pd
import os
import plotly.express as px
import numpy as np
import sys

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(streamlit_app_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.config import DATA_DIR_RAW, KNOWN_FILE_SOURCES_METADATA, logger
# fetch_data is not directly used here as we load raw files

st.header("📂 Source File Analytics")
st.markdown("Select a raw source file to see its basic statistics, data preview, and column visualizations.")

@st.cache_data(ttl=300) # Cache loaded raw data for 5 mins
def load_selected_source_file(file_name_key_from_select): # Use a different param name
    file_path = os.path.join(DATA_DIR_RAW, file_name_key_from_select) # Use the key which is the filename
    metadata = KNOWN_FILE_SOURCES_METADATA.get(file_name_key_from_select)

    if not metadata:
        logger.error(f"No metadata found for file key: {file_name_key_from_select}")
        st.error(f"Configuration error: No metadata for {file_name_key_from_select}")
        return None, None
    
    if not os.path.exists(file_path):
        st.error(f"File not found: {file_path}")
        return None, None

    file_type = metadata.get('type', os.path.splitext(file_name_key_from_select)[1].lower().replace('.', ''))
    parser_func_name = metadata.get('parser_func')
    
    if not parser_func_name: # Infer from extension if not in metadata
        if file_type == 'json':
            parser_func_name = 'read_json'
        elif file_type == 'csv':
            parser_func_name = 'read_csv'
        else:
            st.error(f"Unsupported file type '{file_type}' for {file_name_key_from_select} without explicit parser_func in metadata.")
            return None, None
            
    try:
        if hasattr(pd, parser_func_name):
            parser_func = getattr(pd, parser_func_name)
            df = parser_func(file_path, low_memory=False) if file_type == 'csv' else parser_func(file_path)
            logger.info(f"Successfully loaded {file_name_key_from_select} for preview.")
            return df, file_name_key_from_select # Return df and the original key
        else:
            st.error(f"Pandas has no parser '{parser_func_name}' for {file_name_key_from_select}")
            return None, None
    except Exception as e:
        st.error(f"Error loading {file_name_key_from_select}: {e}")
        logger.error(f"Error loading {file_name_key_from_select}: {e}", exc_info=True)
        return None, None

available_file_keys = list(KNOWN_FILE_SOURCES_METADATA.keys())

if not available_file_keys:
    st.warning("No source files configured in `src/config.py` under `KNOWN_FILE_SOURCES_METADATA`.")
else:
    selected_file_key = st.selectbox(
        "Choose a source file to analyze:",
        options=available_file_keys,
        index=0, 
        key="source_file_selector_page6" # Unique key for this selectbox
    )

    if selected_file_key:
        df_raw_preview, loaded_file_key = load_selected_source_file(selected_file_key)

        if df_raw_preview is not None and not df_raw_preview.empty and loaded_file_key == selected_file_key:
            st.subheader(f"Analytics for: {selected_file_key}")
            st.markdown(f"**Shape:** {df_raw_preview.shape[0]} rows, {df_raw_preview.shape[1]} columns")

            tab1, tab2, tab3, tab4 = st.tabs(["📄 Data Preview", "📊 Basic Stats", "🚫 Null Values", "📉 Column Plot"])

            with tab1:
                st.dataframe(df_raw_preview.head(20), height=400, use_container_width=True)

            with tab2:
                try:
                    st.dataframe(df_raw_preview.describe(include='all', datetime_is_numeric=True).transpose(), use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not generate full descriptive statistics: {e}. Displaying basic info.")
                    st.dataframe(df_raw_preview.head().transpose())


            with tab3:
                null_counts = df_raw_preview.isnull().sum().reset_index()
                null_counts.columns = ['Column', 'Null Count']
                null_counts_filtered = null_counts[null_counts['Null Count'] > 0].sort_values(by='Null Count', ascending=False)
                
                if not null_counts_filtered.empty:
                    st.write(f"Null values found in **{selected_file_key}**:")
                    fig_nulls = px.bar(null_counts_filtered, x='Column', y='Null Count', 
                                       title=f"Null Values in {selected_file_key}",
                                       color='Null Count', color_continuous_scale=px.colors.sequential.OrRd)
                    st.plotly_chart(fig_nulls, use_container_width=True)
                    st.dataframe(null_counts_filtered, use_container_width=True)
                else:
                    st.success("No null values found in this file.")
            
            with tab4:
                st.markdown("#### Dynamic Column Visualization")
                if not df_raw_preview.columns.empty:
                    plot_column = st.selectbox(
                        "Select a column to visualize:", 
                        options=[""] + list(df_raw_preview.columns), 
                        index=0,
                        key=f"plot_col_select_dynamic_{selected_file_key}" # More unique key
                    )
                    
                    if plot_column:
                        selected_series_original = df_raw_preview[plot_column]
                        selected_series_dropna = selected_series_original.dropna()

                        if selected_series_dropna.empty:
                            st.warning(f"Column '{plot_column}' has no non-null data to plot.")
                        
                        # Try to convert to numeric, but fall back if it doesn't make sense
                        try:
                            selected_series_numeric = pd.to_numeric(selected_series_dropna, errors='coerce')
                            # If most values became NaN after to_numeric, it's likely not truly numeric for plotting
                            if selected_series_numeric.notna().sum() / len(selected_series_dropna) < 0.5 and len(selected_series_dropna.unique()) > 15:
                                is_numeric_for_plot = False
                            else:
                                is_numeric_for_plot = pd.api.types.is_numeric_dtype(selected_series_numeric.dropna()) \
                                                      and len(selected_series_numeric.dropna().unique()) > 10 # Heuristic
                        except Exception:
                            is_numeric_for_plot = False

                        if is_numeric_for_plot:
                            st.write(f"Plotting distribution for numerical column: **{plot_column}**")
                            try:
                                fig_hist = px.histogram(selected_series_numeric.dropna(), nbins=30, title=f"Distribution of {plot_column}",
                                                        color_discrete_sequence=['#007bff'])
                                st.plotly_chart(fig_hist, use_container_width=True)
                            except Exception as e:
                                st.error(f"Could not plot histogram for {plot_column}: {e}")
                        else: 
                            st.write(f"Plotting value counts for column: **{plot_column}** (Top 20)")
                            try:
                                counts = selected_series_original.astype(str).value_counts().nlargest(20).reset_index()
                                counts.columns = [plot_column, 'count']
                                fig_bar = px.bar(counts, x=plot_column, y='count', 
                                                 title=f"Top 20 Values for {plot_column}", 
                                                 color=plot_column, color_discrete_sequence=px.colors.qualitative.Safe)
                                st.plotly_chart(fig_bar, use_container_width=True)
                            except Exception as e:
                                st.error(f"Could not plot bar chart for {plot_column}: {e}")
                    else:
                        st.info("Select a column to generate a plot.")
                else:
                    st.warning("No columns available in the selected file for plotting.")
        elif selected_file_key:
            st.error(f"Could not load or data is empty for {selected_file_key}. Check logs for details.")