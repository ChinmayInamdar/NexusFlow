# streamlit_app/app.py
import streamlit as st
# from streamlit_option_menu import option_menu # Not needed for page navigation with 'pages' folder
import sqlite3
import pandas as pd
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.config import logger, DB_PATH
from streamlit_app.auth import authenticate_user_interface # Use the new UI handling function

st.set_page_config(
    page_title="NexusFlow E-commerce Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded" # Keep sidebar expanded initially
)

# --- Database Connection (can be shared across pages) ---
@st.cache_resource
def get_db_connection():
    try:
        abs_db_path = os.path.abspath(DB_PATH)
        if not os.path.exists(abs_db_path):
            # This error will be shown on the page where get_db_connection is first called if DB doesn't exist
            logger.error(f"Database file not found at {abs_db_path}. Please run the ETL pipeline first.")
            return None
        conn = sqlite3.connect(f"file:{abs_db_path}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        logger.info(f"Successfully connected to database: {abs_db_path}")
        return conn
    except sqlite3.OperationalError as e:
        logger.error(f"OperationalError connecting to the database: {e}. Path: '{abs_db_path}'")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while connecting to the database: {e}")
        return None

@st.cache_data(ttl=300)
def fetch_data(query, params=None):
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except pd.io.sql.DatabaseError as e:
            st.error(f"Database query error: {e}. Query: {query}") # Show error in app
            logger.error(f"Database query error: {e}. Query: {query}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error fetching data: {e}. Query: {query}") # Show error in app
            logger.error(f"Error fetching data: {e}. Query: {query}")
            return pd.DataFrame()
    else:
        logger.warning("fetch_data called but DB connection is not available.")
        # st.error("Database connection is not available. Please ensure the ETL pipeline has run successfully.")
        return pd.DataFrame()

# # --- Authentication ---
# # This will now display the login form in the main area if not authenticated.
# # The rest of the app (sidebar and page content) will only render if this returns True.
# if not authenticate_user_interface():
#     st.stop() # Stop execution if not authenticated, login form is already handled

# --- Main App Layout (Only if Authenticated) ---
with st.sidebar:
    st.image("https://i.imgur.com/3g8aq0q.png", width=100, use_column_width='auto') # 'auto' or 'always'
    st.success(f"Logged in as: **{st.session_state.get('username', 'User')}**")
    st.markdown("---")
    
    # Streamlit automatically creates navigation from files in the `pages` directory.
    # You can add other sidebar elements here if needed.
    
    if st.button("Logout", use_container_width=True, key="main_logout_button_sidebar"):
        st.session_state.authenticated = False
        st.session_state.username = None
        logger.info("User logged out.")
        st.query_params.clear() 
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.info("🛒 NexusFlow Dashboard v1.2")

# At this point, Streamlit's multi-page app feature will take over and render
# the content of the selected page from the `pages/` directory.
# Typically, `pages/00_Home.py` would be the default page loaded after authentication.
# If you are on localhost:8501 and logged in, it should try to load 00_Home.py.
# If you see a blank page (other than the sidebar), ensure 00_Home.py has content.