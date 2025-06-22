# streamlit_app/pages/02_Product_Analytics.py
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
if streamlit_app_dir not in sys.path:
    sys.path.append(streamlit_app_dir)

from app import fetch_data

st.header("📦 Product Analytics")
df_products = fetch_data("SELECT * FROM Products")

if df_products is not None and not df_products.empty:
    st.subheader("Product Data Overview")
    st.dataframe(df_products.head(10), height=300, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Products by Category")
        if 'category' in df_products.columns:
            category_counts = df_products['category'].value_counts().nlargest(10).reset_index()
            category_counts.columns = ['category', 'count'] # Adjusted
            fig_category = px.bar(category_counts, x='category', y='count', title="Top 10 Product Categories",
                                  color='category', color_discrete_sequence=px.colors.qualitative.Vivid)
            st.plotly_chart(fig_category, use_container_width=True)
        else:
            st.warning("Category column not found.")
    
    with col2:
        st.subheader("Product Stock Levels (Top 10 by Stock)")
        if 'stock_quantity' in df_products.columns and 'product_name' in df_products.columns:
            top_stock = df_products.nlargest(10, 'stock_quantity')
            fig_stock = px.bar(top_stock, x='product_name', y='stock_quantity', title="Top 10 Products by Stock Quantity",
                               color='product_name', color_discrete_sequence=px.colors.qualitative.Bold)
            fig_stock.update_layout(xaxis_title="Product Name", yaxis_title="Stock Quantity")
            st.plotly_chart(fig_stock, use_container_width=True)
        else:
            st.warning("Stock quantity or product name column not found.")

    st.subheader("Filter Products")
    if not df_products.empty:
        search_term = st.text_input("Search Product Name:", key="product_search")
        
        unique_categories = ['All']
        if 'category' in df_products.columns:
            unique_categories += sorted(df_products['category'].astype(str).unique().tolist())
        
        filter_category = st.selectbox("Filter by Category:", options=unique_categories, key="product_category_filter")
        
        filtered_df_prod = df_products.copy()
        if search_term and 'product_name' in filtered_df_prod.columns:
            filtered_df_prod = filtered_df_prod[filtered_df_prod['product_name'].astype(str).str.contains(search_term, case=False, na=False)]
        if filter_category != 'All' and 'category' in filtered_df_prod.columns:
            filtered_df_prod = filtered_df_prod[filtered_df_prod['category'] == filter_category]
        
        st.dataframe(filtered_df_prod, height=400, use_container_width=True)
    else:
        st.info("No product data to filter.")
else:
    st.warning("No product data found or error loading data. Please run the ETL pipeline.")