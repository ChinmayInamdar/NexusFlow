# streamlit_app/pages/01_Customer_Insights.py
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

st.header("👥 Customer Insights")
df_customers = fetch_data("SELECT * FROM Customers")

if df_customers is not None and not df_customers.empty:
    st.subheader("Customer Data Overview")
    st.dataframe(df_customers.head(10), height=300, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Customers by Segment")
        if 'segment' in df_customers.columns:
            segment_counts = df_customers['segment'].value_counts().reset_index()
            segment_counts.columns = ['segment', 'count'] # Adjusted for potential new pandas versions
            fig_segment = px.pie(segment_counts, names='segment', values='count', title="Customer Segments",
                                 color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_segment.update_layout(legend_title_text='Segment')
            st.plotly_chart(fig_segment, use_container_width=True)
        else:
            st.warning("Segment column not found.")
    
    with col2:
        st.subheader("Customers by Status")
        if 'status' in df_customers.columns:
            status_counts = df_customers['status'].value_counts().reset_index()
            status_counts.columns = ['status', 'count'] # Adjusted
            fig_status = px.bar(status_counts, x='status', y='count', title="Customer Statuses",
                                color='status', color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.warning("Status column not found.")

    st.subheader("Filter Customers")
    if not df_customers.empty:
        unique_segments = sorted(df_customers['segment'].astype(str).unique().tolist()) if 'segment' in df_customers else []
        unique_statuses = sorted(df_customers['status'].astype(str).unique().tolist()) if 'status' in df_customers else []

        filter_segment = st.multiselect("Filter by Segment:", options=unique_segments, default=unique_segments if unique_segments else None)
        filter_status = st.multiselect("Filter by Status:", options=unique_statuses, default=unique_statuses if unique_statuses else None)
        
        filtered_df = df_customers.copy()
        if filter_segment and 'segment' in filtered_df:
            filtered_df = filtered_df[filtered_df['segment'].isin(filter_segment)]
        if filter_status and 'status' in filtered_df:
            filtered_df = filtered_df[filtered_df['status'].isin(filter_status)]
        
        st.dataframe(filtered_df, height=400, use_container_width=True)
    else:
        st.info("No customer data to filter.")
else:
    st.warning("No customer data found or error loading data. Please run the ETL pipeline.")