# streamlit_app/pages/03_Order_Overview.py
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

st.header("🚚 Order Overview")

# Fetch order items with product and customer details
query_order_items = """
SELECT 
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    p.product_name,
    oi.customer_id,
    c.customer_name,
    oi.quantity,
    oi.unit_price,
    oi.line_item_total_value,
    o.order_date,
    o.order_status,
    o.payment_method,
    o.payment_status,
    o.delivery_status
FROM OrderItems oi
LEFT JOIN Orders o ON oi.order_id = o.order_id
LEFT JOIN Products p ON oi.product_id = p.product_id
LEFT JOIN Customers c ON oi.customer_id = c.customer_id
ORDER BY o.order_date DESC
LIMIT 1000; 
""" # Limit for performance in dashboard
df_order_items_detailed = fetch_data(query_order_items)

if df_order_items_detailed is not None and not df_order_items_detailed.empty:
    st.subheader("Recent Order Items (Details)")
    st.dataframe(df_order_items_detailed.head(20), height=500, use_container_width=True)

    st.subheader("Orders by Status")
    if 'order_id' in df_order_items_detailed.columns and 'order_status' in df_order_items_detailed.columns:
        # Get unique orders first before counting statuses
        unique_orders_df = df_order_items_detailed.drop_duplicates(subset=['order_id'])
        order_status_counts = unique_orders_df['order_status'].value_counts().reset_index()
        order_status_counts.columns = ['status', 'count'] # Adjusted
        fig_order_status = px.pie(order_status_counts, names='status', values='count', 
                                  title="Order Status Distribution", hole=0.3,
                                  color_discrete_sequence=px.colors.qualitative.Plotly)
        fig_order_status.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_order_status, use_container_width=True)
    else:
        st.warning("Order ID or Order Status column not found for status distribution.")
else:
    st.warning("No order item data found or error loading data. Please run the ETL pipeline.")