# streamlit_app/pages/05_Data_Quality_Report.py
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
if streamlit_app_dir not in sys.path:
    sys.path.append(streamlit_app_dir)

from app import fetch_data, get_db_connection # Import get_db_connection if needed for direct table access

st.header("📋 Data Quality & ETL Summary")
st.markdown("""
This section provides an overview of the record counts in the unified database and a sample
null value analysis for key tables post-ETL.
""")

conn = get_db_connection()
if conn:
    try:
        customers_count_df = fetch_data("SELECT COUNT(*) as count FROM Customers")
        products_count_df = fetch_data("SELECT COUNT(*) as count FROM Products")
        orders_count_df = fetch_data("SELECT COUNT(*) as count FROM Orders")
        order_items_count_df = fetch_data("SELECT COUNT(*) as count FROM OrderItems")

        customers_count = customers_count_df['count'].iloc[0] if not customers_count_df.empty else "N/A"
        products_count = products_count_df['count'].iloc[0] if not products_count_df.empty else "N/A"
        orders_count = orders_count_df['count'].iloc[0] if not orders_count_df.empty else "N/A"
        order_items_count = order_items_count_df['count'].iloc[0] if not order_items_count_df.empty else "N/A"

        st.subheader("🔢 Record Counts in Unified Database")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Customers", customers_count)
            st.metric("Orders", orders_count)
        with col2:
            st.metric("Products", products_count)
            st.metric("Order Items", order_items_count)
        
        st.markdown("---")
        st.subheader("🚫 Null Value Analysis (Post-ETL)")

        tables_to_analyze = ["Customers", "Products", "Orders", "OrderItems"]
        selected_table_dq = st.selectbox("Select a table for Null Value Analysis:", tables_to_analyze)

        if selected_table_dq:
            df_dq_table = fetch_data(f"SELECT * FROM {selected_table_dq}")
            if df_dq_table is not None and not df_dq_table.empty:
                null_counts = df_dq_table.isnull().sum().reset_index()
                null_counts.columns = ['column', 'null_count']
                null_counts_filtered = null_counts[null_counts['null_count'] > 0].sort_values(by='null_count', ascending=False)
                
                if not null_counts_filtered.empty:
                    st.write(f"Null values found in **{selected_table_dq}**:")
                    fig_nulls = px.bar(null_counts_filtered, x='column', y='null_count', 
                                       title=f"Null Values in Cleaned {selected_table_dq} Table",
                                       color='null_count', color_continuous_scale=px.colors.sequential.OrRd)
                    st.plotly_chart(fig_nulls, use_container_width=True)
                    st.dataframe(null_counts_filtered, use_container_width=True)
                else:
                    st.success(f"No null values found in the cleaned {selected_table_dq} table (or all handled).")
            else:
                st.warning(f"Could not fetch data for {selected_table_dq} for DQ report.")

    except Exception as e:
        st.error(f"Error generating data quality report: {e}")
else:
    st.error("Database connection not available for Data Quality Report. Please ensure the ETL has run successfully.")