# streamlit_app/pages/02_Product_Analytics.py
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

# Path setup
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
if streamlit_app_dir not in sys.path:
    sys.path.append(streamlit_app_dir)

from app import fetch_data

# Page setup
st.set_page_config(page_title="Product Analytics", layout="wide")
st.header("📦 Product Analytics")
st.markdown("### Analyze your product catalog, category spread, and stock availability.")

# Fetch data
df_products = fetch_data("SELECT * FROM Products")

if df_products is not None and not df_products.empty:

    # Preview
    st.subheader("🗂 Product Data Preview")
    st.dataframe(df_products.head(10), height=300, use_container_width=True)
    st.markdown("---")

    # Visuals
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📚 Top 10 Product Categories")
        if 'category' in df_products.columns:
            category_counts = df_products['category'].value_counts().nlargest(10).reset_index()
            category_counts.columns = ['category', 'count']
            fig_category = px.bar(
                category_counts,
                x='category',
                y='count',
                title="Top 10 Product Categories",
                color='count',
                text='count',
                color_continuous_scale='Tealgrn'
            )
            fig_category.update_layout(
                xaxis_title="Category",
                yaxis_title="Number of Products",
                height=420,
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(size=14),
                margin=dict(t=50, b=40),
                showlegend=False
            )
            fig_category.update_traces(marker_line_color='gray', marker_line_width=1)
            st.plotly_chart(fig_category, use_container_width=True)
        else:
            st.warning("⚠️ 'category' column not found.")

    with col2:
        st.subheader("📦 Product Stock Levels (Top 10)")
        if 'stock_quantity' in df_products.columns and 'product_name' in df_products.columns:
            top_stock = df_products.nlargest(10, 'stock_quantity')
            fig_stock = px.bar(
                top_stock,
                x='stock_quantity',
                y='product_name',
                orientation='h',
                title="Top 10 Products by Stock Quantity",
                color='stock_quantity',
                text='stock_quantity',
                color_continuous_scale='Blues'
            )
            fig_stock.update_layout(
                xaxis_title="Stock Quantity",
                yaxis_title="Product Name",
                height=420,
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(size=14),
                margin=dict(t=50, b=40),
                showlegend=False
            )
            fig_stock.update_traces(marker_line_color='gray', marker_line_width=1)
            st.plotly_chart(fig_stock, use_container_width=True)
        else:
            st.warning("⚠️ 'stock_quantity' or 'product_name' column missing.")

    st.markdown("---")

    # Filter Section
    st.subheader("🔎 Filter Products")

    search_term = st.text_input("Search by Product Name:", key="product_search")

    unique_categories = ['All']
    if 'category' in df_products.columns:
        unique_categories += sorted(df_products['category'].astype(str).unique().tolist())

    filter_category = st.selectbox("Filter by Category:", options=unique_categories, key="product_category_filter")

    filtered_df_prod = df_products.copy()

    if search_term and 'product_name' in filtered_df_prod.columns:
        filtered_df_prod = filtered_df_prod[
            filtered_df_prod['product_name'].astype(str).str.contains(search_term, case=False, na=False)
        ]

    if filter_category != 'All' and 'category' in filtered_df_prod.columns:
        filtered_df_prod = filtered_df_prod[filtered_df_prod['category'] == filter_category]

    st.markdown("#### 🎯 Filtered Product Results")
    st.dataframe(filtered_df_prod, height=400, use_container_width=True)

else:
    st.error("❌ No product data found or failed to load. Please ensure the ETL pipeline has run and the database is populated.")
