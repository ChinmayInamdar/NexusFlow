# streamlit_app/pages/00_Home.py
import streamlit as st
import os
import sys

# Ensure the main app's directory (streamlit_app) is in the path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
if streamlit_app_dir not in sys.path:
    sys.path.append(streamlit_app_dir)

from app import fetch_data # Import from the main app.py at streamlit_app/app.py

st.title("🚀 TechCorp Unified E-commerce Dashboard")
st.markdown("---")
st.markdown("""
    Welcome to the unified dashboard for TechCorp's e-commerce platforms.
    This platform integrates data from multiple acquired systems, providing a clean, 
    centralized view for analysis and business intelligence.
    
    **Navigate using the sidebar to explore:**
    - Customer demographics and segments.
    - Product performance and catalog details.
    - Order trends and fulfillment statuses.
    - Key Performance Indicators for sales.
    - Data quality metrics from the ETL process.
    - Analytics for individual source files.
""")
st.markdown("---")
st.subheader("📊 Data Sources Overview")
st.markdown("""
    - **Customer Data:** Processed from `customers_messy_data.json`.
    - **Product Data:** Processed from `products_inconsistent_data.json`.
    - **Order Item Data:** Unified from `reconciliation_challenge_data.csv` and `orders_unstructured_data.csv`.
""")

st.subheader("📈 Quick Stats from Unified Database")
try:
    customers_count_df = fetch_data("SELECT COUNT(*) as count FROM Customers")
    products_count_df = fetch_data("SELECT COUNT(*) as count FROM Products")
    orders_count_df = fetch_data("SELECT COUNT(*) as count FROM Orders")
    order_items_count_df = fetch_data("SELECT COUNT(*) as count FROM OrderItems")

    col1, col2, col3, col4 = st.columns(4)
    
    customers_count = customers_count_df['count'].iloc[0] if not customers_count_df.empty and 'count' in customers_count_df.columns else "N/A"
    products_count = products_count_df['count'].iloc[0] if not products_count_df.empty and 'count' in products_count_df.columns else "N/A"
    orders_count = orders_count_df['count'].iloc[0] if not orders_count_df.empty and 'count' in orders_count_df.columns else "N/A"
    order_items_count = order_items_count_df['count'].iloc[0] if not order_items_count_df.empty and 'count' in order_items_count_df.columns else "N/A"

    col1.metric("Total Customers", customers_count)
    col2.metric("Total Products", products_count)
    col3.metric("Total Orders", orders_count)
    col4.metric("Total Order Items", order_items_count)
except Exception as e:
    st.error(f"Could not load quick stats: {e}. Ensure the database is populated.")

st.markdown("---")
st.info("Use the sidebar to navigate to detailed analytics pages.")