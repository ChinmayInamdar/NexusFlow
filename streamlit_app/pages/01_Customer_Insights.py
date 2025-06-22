# streamlit_app/pages/01_Customer_Insights.py
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

# Add root directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
streamlit_app_dir = os.path.dirname(current_dir)
if streamlit_app_dir not in sys.path:
    sys.path.append(streamlit_app_dir)

from app import fetch_data

# Page Config
st.set_page_config(page_title="Customer Insights", layout="wide")
st.header("👥 Customer Insights")
st.markdown("### Understand your customers by segment, status, and demographics.")

# Load Customer Data
df_customers = fetch_data("SELECT * FROM Customers")

if df_customers is not None and not df_customers.empty:

    # Preview Table
    st.subheader("🗂 Customer Data Preview")
    st.dataframe(df_customers.head(10), height=300, use_container_width=True)

    st.markdown("---")

    # Visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Customers by Segment")
        if 'segment' in df_customers.columns:
            segment_counts = df_customers['segment'].value_counts().reset_index()
            segment_counts.columns = ['segment', 'count']

            fig_segment = px.pie(
                segment_counts,
                names='segment',
                values='count',
                hole=0.5,
                title="Customer Segments Breakdown",
                color_discrete_sequence=px.colors.sequential.Tealgrn
            )
            fig_segment.update_traces(
                textinfo='percent+label',
                pull=[0.05] * len(segment_counts),
                marker=dict(line=dict(color='white', width=2))
            )
            fig_segment.update_layout(
                showlegend=True,
                height=400,
                annotations=[dict(text='Segments', x=0.5, y=0.5, font_size=16, showarrow=False)]
            )
            st.plotly_chart(fig_segment, use_container_width=True)
        else:
            st.warning("⚠️ 'segment' column not found in the dataset.")

    with col2:
        st.subheader("📶 Customers by Status")
        if 'status' in df_customers.columns:
            status_counts = df_customers['status'].value_counts().reset_index()
            status_counts.columns = ['status', 'count']

            fig_status = px.bar(
                status_counts,
                x='count',
                y='status',
                orientation='h',
                title="Customer Status Overview",
                color='status',
                text='count',
                color_discrete_sequence=px.colors.sequential.Plasma
            )
            fig_status.update_traces(marker_line_width=1.2, textposition='outside')
            fig_status.update_layout(
                xaxis_title="Count",
                yaxis_title="Status",
                height=400,
                bargap=0.3
            )
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.warning("⚠️ 'status' column not found in the dataset.")

    st.markdown("---")

    # Filters
    st.subheader("🔎 Filter Customers")
    unique_segments = sorted(df_customers['segment'].astype(str).unique().tolist()) if 'segment' in df_customers else []
    unique_statuses = sorted(df_customers['status'].astype(str).unique().tolist()) if 'status' in df_customers else []

    filter_segment = st.multiselect("Filter by Segment", options=unique_segments, default=unique_segments)
    filter_status = st.multiselect("Filter by Status", options=unique_statuses, default=unique_statuses)

    # Apply filters
    filtered_df = df_customers.copy()
    if filter_segment and 'segment' in filtered_df:
        filtered_df = filtered_df[filtered_df['segment'].isin(filter_segment)]
    if filter_status and 'status' in filtered_df:
        filtered_df = filtered_df[filtered_df['status'].isin(filter_status)]

    st.markdown("#### 🎯 Filtered Customer Results")
    st.dataframe(filtered_df, height=400, use_container_width=True)

else:
    st.error("❌ No customer data found or failed to load. Please ensure the ETL pipeline has run and the database is populated.")
