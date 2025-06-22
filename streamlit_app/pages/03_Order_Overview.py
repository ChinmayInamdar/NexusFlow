# streamlit_app/pages/03_Order_Overview.py
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

# Add project root to sys.path to allow imports from src and other app modules
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir) # Goes to streamlit_app directory
project_root = os.path.dirname(streamlit_app_dir) # Goes to nexusflow_project directory

if project_root not in sys.path:
    sys.path.append(project_root)
if streamlit_app_dir not in sys.path: # To import app.py if needed by other pages
    sys.path.append(streamlit_app_dir)

from app import fetch_data # Assuming fetch_data is defined in streamlit_app/app.py
from src.config import logger # Assuming logger is defined in src/config.py

st.set_page_config(page_title="Order Overview", layout="wide") # Usually set in main app.py, but can be per page

st.header("🚚 Order Overview")
st.markdown("Explore recent order items and view order status distributions.")

# Fetch order items with product and customer details
# This query joins multiple tables to provide a comprehensive view of order items.
query_order_items_detailed = """
SELECT 
    oi.order_item_record_id, -- Corrected from oi.order_item_id
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
    o.delivery_status,
    oi.source_file_name AS item_source_file, -- Added to see the source of the item
    o.source_file_name AS order_source_file   -- Added to see the source of the order header
FROM OrderItems oi
LEFT JOIN Orders o ON oi.order_id = o.order_id AND oi.source_file_name = o.source_file_name -- Join on source_file_name if orders are also per source
LEFT JOIN Products p ON oi.product_id = p.product_id -- Assuming products are globally unique by product_id or use a more specific join
LEFT JOIN Customers c ON oi.customer_id = c.customer_id -- Assuming customers are globally unique by customer_id or use a more specific join
ORDER BY o.order_date DESC, oi.order_item_record_id DESC
LIMIT 1000; 
"""
# Note on Joins:
# - The join between OrderItems and Orders now includes source_file_name if an order_id can exist in multiple files
#   but refers to different conceptual orders. If order_id is globally unique, then joining on just order_id is fine.
#   Given our schema where Orders also has source_file_name, this join makes sense if order_id is not globally unique.
# - Joins to Products and Customers are on their business keys. If these tables also have multiple entries
#   per business key (due to different source_file_name), this join will potentially multiply rows if not careful.
#   For display, this usually picks one product/customer. For analytics, you'd use DISTINCT or GROUP BY.

logger.info("Fetching detailed order items for Order Overview page...")
df_order_items_detailed = fetch_data(query_order_items_detailed)

if df_order_items_detailed is not None and not df_order_items_detailed.empty:
    st.subheader("Recent Order Items (Details)")
    # Prepare for display: explicitly cast object columns to string for Arrow compatibility
    df_display_safe = df_order_items_detailed.head(20).copy() # Show first 20 for preview
    for col in df_display_safe.select_dtypes(include=['object']).columns:
        try: df_display_safe[col] = df_display_safe[col].astype(str)
        except Exception: pass # Ignore if conversion fails for display
    
    # Define columns to show, ensuring the renamed ID is used if needed for display logic
    # For st.dataframe, it will use the column names as returned by the query.
    st.dataframe(df_display_safe, height=500, use_container_width=True)

    st.markdown("---")
    st.subheader("Orders by Status")
    
    # To get accurate order status counts, we should look at unique orders.
    # The df_order_items_detailed might have multiple rows per order (one for each item).
    # We need a distinct list of orders and their statuses.
    if 'order_id' in df_order_items_detailed.columns and 'order_status' in df_order_items_detailed.columns:
        # Create a DataFrame with unique order_id and their corresponding order_status
        # If an order_id has items from different source_files but is conceptually the same order,
        # this drop_duplicates will pick the first encountered status.
        # If order_id + order_source_file defines unique orders, use that.
        unique_orders_status_df = df_order_items_detailed[['order_id', 'order_status', 'order_source_file']].drop_duplicates(
            subset=['order_id', 'order_source_file'] # Consider order_source_file for true uniqueness of order instance
        )
        
        if not unique_orders_status_df.empty:
            order_status_counts = unique_orders_status_df['order_status'].value_counts().reset_index()
            # Ensure columns are named correctly for plotly express
            order_status_counts.columns = ['status', 'count'] if len(order_status_counts.columns) == 2 else ['index', 'order_status'] # Fallback for older pandas
            if 'index' in order_status_counts.columns and 'status' not in order_status_counts.columns : order_status_counts.rename(columns={'index':'status', 'order_status':'count'}, inplace=True)


            if 'status' in order_status_counts.columns and 'count' in order_status_counts.columns:
                fig_order_status = px.pie(order_status_counts, names='status', values='count', 
                                          title="Order Status Distribution (Unique Orders)", hole=0.3,
                                          color_discrete_sequence=px.colors.qualitative.Pastel)
                fig_order_status.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_order_status, use_container_width=True)
            else:
                st.warning("Could not properly structure order status counts for chart.")
        else:
            st.info("No unique order statuses to display.")
            
    else:
        st.warning("Required columns ('order_id', 'order_status') not found in the detailed order items data.")

    # Further analytics can be added here, e.g., orders over time, payment method distribution.
    st.markdown("---")
    st.subheader("Payment Method Distribution")
    if 'order_id' in df_order_items_detailed.columns and 'payment_method' in df_order_items_detailed.columns:
        unique_orders_payment_df = df_order_items_detailed[['order_id', 'payment_method', 'order_source_file']].drop_duplicates(
            subset=['order_id', 'order_source_file']
        )
        if not unique_orders_payment_df.empty:
            payment_method_counts = unique_orders_payment_df['payment_method'].value_counts().nlargest(10).reset_index()
            payment_method_counts.columns = ['method', 'count'] if len(payment_method_counts.columns) == 2 else ['index', 'payment_method']
            if 'index' in payment_method_counts.columns and 'method' not in payment_method_counts.columns : payment_method_counts.rename(columns={'index':'method', 'payment_method':'count'}, inplace=True)

            if 'method' in payment_method_counts.columns and 'count' in payment_method_counts.columns:
                fig_payment_methods = px.bar(payment_method_counts, x='method', y='count',
                                             title="Top Payment Methods Used (Unique Orders)",
                                             color='method',
                                             labels={'method': 'Payment Method', 'count': 'Number of Orders'})
                st.plotly_chart(fig_payment_methods, use_container_width=True)
            else:
                st.warning("Could not properly structure payment method counts for chart.")
        else:
            st.info("No unique payment methods to display.")
    else:
        st.warning("Required columns for payment method analysis not found.")

else:
    st.warning("""
        No detailed order item data found. This could be due to:
        1. The database being empty or tables not populated. Please run the ETL pipeline (`python -m src.main_etl`).
        2. An error occurring during data fetching. Check terminal logs.
        3. The query returning no results (e.g., after schema changes if joins are no longer valid).
    """)
    st.caption(f"Attempted query: `{query_order_items_detailed}`")