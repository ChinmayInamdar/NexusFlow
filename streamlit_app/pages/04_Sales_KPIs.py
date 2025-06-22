# streamlit_app/pages/04_Sales_KPIs.py
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

st.header("💰 Sales Key Performance Indicators")

# Adjusted query to handle potential NULLs in status before filtering
total_revenue_df = fetch_data("SELECT SUM(order_total_value_net) as TotalRevenue FROM Orders WHERE IFNULL(order_status, 'UNKNOWN') NOT IN ('CANCELLED', 'RETURNED')")
total_orders_count_df = fetch_data("SELECT COUNT(DISTINCT order_id) as TotalOrders FROM Orders WHERE IFNULL(order_status, 'UNKNOWN') NOT IN ('CANCELLED', 'RETURNED')")
avg_order_value_df = fetch_data("SELECT AVG(order_total_value_net) as AvgOrderValue FROM Orders WHERE IFNULL(order_status, 'UNKNOWN') NOT IN ('CANCELLED', 'RETURNED')")

revenue = 0
orders_count = 0
avg_val = 0

if total_revenue_df is not None and not total_revenue_df.empty and 'TotalRevenue' in total_revenue_df.columns:
    revenue_val = total_revenue_df['TotalRevenue'].iloc[0]
    revenue = revenue_val if pd.notna(revenue_val) else 0
if total_orders_count_df is not None and not total_orders_count_df.empty and 'TotalOrders' in total_orders_count_df.columns:
    orders_count_val = total_orders_count_df['TotalOrders'].iloc[0]
    orders_count = orders_count_val if pd.notna(orders_count_val) else 0
if avg_order_value_df is not None and not avg_order_value_df.empty and 'AvgOrderValue' in avg_order_value_df.columns:
    avg_val_val = avg_order_value_df['AvgOrderValue'].iloc[0]
    avg_val = avg_val_val if pd.notna(avg_val_val) else 0

col1, col2, col3 = st.columns(3)
col1.metric("Total Revenue (Valid Orders)", f"${revenue:,.2f}")
col2.metric("Total Valid Orders", f"{orders_count:,}")
col3.metric("Avg. Order Value (Valid)", f"${avg_val:,.2f}")
st.markdown("---")

st.subheader("📅 Monthly Sales Trend (Net Revenue)")
monthly_sales = fetch_data("""
    SELECT 
        strftime('%Y-%m', order_date) AS SaleMonth, 
        SUM(order_total_value_net) AS MonthlyRevenue
    FROM Orders
    WHERE IFNULL(order_status, 'UNKNOWN') NOT IN ('CANCELLED', 'RETURNED') AND order_date IS NOT NULL
    GROUP BY SaleMonth
    ORDER BY SaleMonth;
""")
if monthly_sales is not None and not monthly_sales.empty:
    fig_monthly_sales = px.line(monthly_sales, x='SaleMonth', y='MonthlyRevenue', title="Monthly Sales Revenue", markers=True)
    fig_monthly_sales.update_layout(xaxis_title="Month", yaxis_title="Revenue ($)")
    st.plotly_chart(fig_monthly_sales, use_container_width=True)
else:
    st.warning("Could not generate monthly sales trend. Ensure orders have valid dates and statuses.")
    
st.subheader("🏆 Top Selling Products (by Net Revenue)")
top_products_revenue = fetch_data("""
    SELECT p.product_name, SUM(oi.line_item_total_value - IFNULL(oi.line_item_discount, 0)) AS ProductRevenue
    FROM OrderItems oi
    JOIN Products p ON oi.product_id = p.product_id
    JOIN Orders o ON oi.order_id = o.order_id
    WHERE IFNULL(o.order_status, 'UNKNOWN') NOT IN ('CANCELLED', 'RETURNED')
    GROUP BY p.product_name
    ORDER BY ProductRevenue DESC
    LIMIT 10;
""")
if top_products_revenue is not None and not top_products_revenue.empty:
    fig_top_products = px.bar(top_products_revenue, x='ProductRevenue', y='product_name', orientation='h',
                              title="Top 10 Products by Net Revenue", color='ProductRevenue',
                              color_continuous_scale=px.colors.sequential.Viridis)
    fig_top_products.update_layout(yaxis_title="Product Name", xaxis_title="Net Revenue ($)")
    st.plotly_chart(fig_top_products, use_container_width=True)
else:
    st.warning("Could not generate top selling products chart.")