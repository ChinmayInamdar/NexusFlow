# streamlit_app/pages/00_Home.py
import streamlit as st
import os
import sys

# Ensure the main app's directory (streamlit_app) is in the path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
if streamlit_app_dir not in sys.path:
    sys.path.append(streamlit_app_dir)

from app import fetch_data  # Import from the main app.py at streamlit_app/app.py

# Page Configuration
st.set_page_config(page_title="TechCorp Dashboard", page_icon="🚀", layout="wide")

# Title & Intro
st.title("🚀 TechCorp Unified E-commerce Dashboard")
st.markdown("##### One platform. All your commerce data. Centralized.")

st.markdown("""---""")

# Welcome Text
st.markdown(
    """
    Welcome to **TechCorp's Unified Dashboard**, your centralized platform to explore and analyze 
    e-commerce data integrated from multiple systems.

    **🔍 Use the sidebar to explore:**
    - 👥 Customer demographics and segments
    - 📦 Product performance and catalog details
    - 🛒 Order trends and fulfillment statuses
    - 📈 Sales KPIs and insights
    - ⚙️ ETL data quality metrics
    - 📂 Analytics by raw source files
    """
)

st.markdown("""---""")

# Data Sources Overview
st.subheader("📊 Data Sources Overview")
st.markdown(
    """
    - **Customer Data:** `customers_messy_data.json`
    - **Product Data:** `products_inconsistent_data.json`
    - **Order Item Data:** `reconciliation_challenge_data.csv`, `orders_unstructured_data.csv`
    """
)

st.markdown("""---""")

# Quick Stats
st.subheader("📈 Quick Stats from Unified Database")

try:
    customers_count_df = fetch_data("SELECT COUNT(*) as count FROM Customers")
    products_count_df = fetch_data("SELECT COUNT(*) as count FROM Products")
    orders_count_df = fetch_data("SELECT COUNT(*) as count FROM Orders")
    order_items_count_df = fetch_data("SELECT COUNT(*) as count FROM OrderItems")

    customers_count = customers_count_df['count'].iloc[0] if not customers_count_df.empty else "N/A"
    products_count = products_count_df['count'].iloc[0] if not products_count_df.empty else "N/A"
    orders_count = orders_count_df['count'].iloc[0] if not orders_count_df.empty else "N/A"
    order_items_count = order_items_count_df['count'].iloc[0] if not order_items_count_df.empty else "N/A"

    with st.container():
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(label="👥 Total Customers", value=customers_count)
        col2.metric(label="📦 Total Products", value=products_count)
        col3.metric(label="🛒 Total Orders", value=orders_count)
        col4.metric(label="📦 Total Order Items", value=order_items_count)

except Exception as e:
    st.error(f"⚠️ Could not load quick stats: {e}. Make sure the database is properly configured and populated.")

st.markdown("""---""")

# Navigation Info
st.info("📌 Tip: Use the navigation sidebar to access detailed dashboards and data insights.")
