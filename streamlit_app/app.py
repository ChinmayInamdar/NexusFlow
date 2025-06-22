# streamlit_app/app.py
import streamlit as st
import sqlite3
import pandas as pd
import os
import sys

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root) # Insert at the beginning

from src.config import logger, DB_PATH # Assuming these exist and are configured

st.set_page_config(
    page_title="NexusFlow",
    page_icon="💠", # Diamond icon, can be changed
    layout="wide",
    initial_sidebar_state="auto" # Collapsed or auto, as screenshot implies a minimal sidebar initially
)

# --- Database Connection (can be shared across pages) ---
@st.cache_resource
def get_db_connection():
    try:
        # Ensure DB_PATH is defined in src.config
        if not DB_PATH: # Check if DB_PATH is empty or None
            logger.error("DB_PATH is not configured in src.config.")
            # Avoid direct st.error on landing page if DB is not critical for it
            # st.error("Database path is not configured. Please check src/config.py.")
            return None
            
        abs_db_path = os.path.abspath(DB_PATH)
        if not os.path.exists(abs_db_path):
            logger.warning(f"Database file not found at {abs_db_path}. Landing page will load; DB-dependent features may fail if this page relies on them.")
            return None # Allow page to load; other pages or parts might show specific errors if they need DB
        
        conn = sqlite3.connect(f"file:{abs_db_path}", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        logger.info(f"Successfully connected to database: {abs_db_path}")
        return conn
    except sqlite3.OperationalError as e:
        logger.error(f"OperationalError connecting to the database: {e}. Path: '{DB_PATH}'")
        logger.warning(f"Database connection error: {e}. Landing page will load; DB-dependent features may fail.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while connecting to the database: {e}")
        logger.warning(f"Unexpected error connecting to DB: {e}. Landing page will load; DB-dependent features may fail.")
        return None

@st.cache_data(ttl=300)
def fetch_data(query, params=None):
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except pd.io.sql.DatabaseError as e:
            st.error(f"Database query error: {e}. Query: {query}") 
            logger.error(f"Database query error: {e}. Query: {query}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error fetching data: {e}. Query: {query}")
            logger.error(f"Error fetching data: {e}. Query: {query}")
            return pd.DataFrame()
    else:
        logger.warning("fetch_data called but DB connection is not available.")
        # Optionally show an error if data fetching is absolutely critical for a page
        # st.error("Database connection is not available to fetch data.")
        return pd.DataFrame()

# --- Sidebar ---
with st.sidebar:
    # Example: you can put a smaller version of logo or navigation hints
    # st.image("https://i.imgur.com/3g8aq0q.png", width=40) # Gear logo if desired
    st.markdown("### 💠 NexusFlow")
    st.markdown("---")
    st.sidebar.info("Application v1.0") # Generic app info
    # Streamlit automatically adds page navigation from `pages/` directory here.

# --- Custom CSS for styling ---
# Main brand color (purple, adjust as needed)
brand_color = "#7B42BC" # A vibrant purple from screenshot button
# A slightly muted purple for general icons/accents
accent_purple = "#6a0dad" # or brand_color

st.markdown(f"""
<style>
    /* Main app container adjustments */
    .main .block-container {{
        padding-top: 1rem !important; 
        padding-bottom: 3rem !important;
        padding-left: 2rem !important;  /* Adjust side padding for wide layout */
        padding-right: 2rem !important; /* Adjust side padding for wide layout */
    }}

    /* Top logo text style */
    .top-logo-text {{
        font-size: 1.8em;
        font-weight: bold;
        color: #333; /* Dark gray for logo text */
        padding: 0.5rem 0rem 1.5rem 0rem; /* Padding: Top, Sides, Bottom */
        display: block;
    }}
    
    /* Hero section styling */
    .hero-section {{
        text-align: center;
        padding: 2rem 1rem;
    }}
    .hero-section .subtitle {{
        font-size: 1.1rem;
        color: {accent_purple}; /* Accent purple */
        font-weight: bold;
        margin-bottom: 0.5rem;
    }}
    .hero-section .subtitle-icon {{
        margin-right: 0.3em;
    }}
    .hero-section h1 {{
        font-size: 3.2rem;
        font-weight: 700; /* Bold */
        color: #2c3e50; /* Dark blue/charcoal */
        margin-bottom: 1rem;
        line-height: 1.2;
    }}
    .hero-section .description {{
        font-size: 1.15rem;
        color: #555;
        max-width: 750px;
        margin: 0 auto 1.5rem auto;
        line-height: 1.6;
    }}

    /* Section titles */
    .section-title {{
        text-align: center;
        font-size: 2.2rem;
        font-weight: 700; /* Bold */
        margin-top: 3rem;
        margin-bottom: 2rem;
        color: #2c3e50;
    }}

    /* Cards for "How It Works" and "Powerful Features" */
    .feature-card {{
        background-color: #ffffff;
        padding: 2rem 1.5rem; /* Increased padding */
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08); /* Slightly more prominent shadow */
        text-align: center;
        height: 100%;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: flex-start;
    }}
    .feature-card .icon {{
        font-size: 2.8rem; /* Larger icons */
        margin-bottom: 1rem;
        color: {brand_color}; /* Brand purple for icons */
    }}
    .feature-card h3 {{
        font-size: 1.4rem; /* Larger card titles */
        font-weight: 600; /* Semi-bold */
        color: #333;
        margin-bottom: 0.75rem;
    }}
    .feature-card p {{
        font-size: 0.95rem;
        color: #666;
        line-height: 1.5;
    }}

    /* Footer styling */
    .footer {{
        background-color: #161A1D; /* Very dark, near black */
        color: #e0e0e0;
        padding: 3rem 1rem;
        text-align: center;
        margin-top: 4rem;
    }}
    .footer .logo-text {{
        font-size: 1.8rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
        color: #ffffff;
    }}
    .footer .slogan {{
        font-size: 1rem;
        color: #a0a0b0; /* Lighter grey for slogan */
    }}

    /* Hide Streamlit's "Made with Streamlit" footer */
    footer[data-testid="stFooter"] {{
        visibility: hidden;
    }}
</style>
""", unsafe_allow_html=True)

# --- Landing Page Content ---

# Top Logo Text
st.markdown("<div class='top-logo-text'>💠 NexusFlow</div>", unsafe_allow_html=True)

# --- Hero Section ---
st.markdown("""
<div class="hero-section">
    <div class="subtitle"><span class="subtitle-icon">✨</span>AI-Powered Data Intelligence</div>
    <h1>From Data Chaos to<br>Business Clarity</h1>
    <p class="description">
        NexusFlow intelligently unifies, cleans, and analyzes your most complex
        datasets. Turn messy acquisitions into strategic assets with our AI-powered data
        reconciliation platform.
    </p>
    <!-- "Get Started Free" button (purple in screenshot) was here. Removed as requested. -->
    <!-- Original button: <button style="background-color: #7B42BC; color: white; border: none; padding: 1rem 2rem; font-size: 1rem; font-weight: bold; border-radius: 5px; cursor: pointer;">Get Started Free</button> -->
</div>
""", unsafe_allow_html=True)


# --- How It Works Section ---
st.markdown("<h2 class='section-title'>How It Works</h2>", unsafe_allow_html=True)

cols_how_it_works = st.columns(3, gap="large")
with cols_how_it_works[0]:
    st.markdown("""
    <div class="feature-card">
        <div class="icon">📤</div>
        <h3>Upload & Ingest</h3>
        <p>Securely upload any data format (CSV, JSON, Excel). Our engine automatically detects schemas and flags quality issues.</p>
    </div>
    """, unsafe_allow_html=True)

with cols_how_it_works[1]:
    st.markdown("""
    <div class="feature-card">
        <div class="icon">🔗</div>
        <h3>Reconcile & Unify</h3>
        <p>Visually map schemas, resolve conflicts, and let our AI-powered engine create a "golden record" for every customer and product.</p>
    </div>
    """, unsafe_allow_html=True)

with cols_how_it_works[2]:
    st.markdown("""
    <div class="feature-card">
        <div class="icon">📊</div>
        <h3>Analyze & Act</h3>
        <p>Explore your unified data through interactive dashboards and unlock critical business insights.</p>
    </div>
    """, unsafe_allow_html=True)


# --- Powerful Features Section ---
st.markdown("<h2 class='section-title'>Powerful Features</h2>", unsafe_allow_html=True)

cols_powerful_features = st.columns(4, gap="medium") # 4 columns might be tight, consider 2x2 for smaller screens (needs media queries or different layout)
with cols_powerful_features[0]:
    st.markdown("""
    <div class="feature-card">
        <div class="icon">✨</div>
        <h3>AI-Powered Matching</h3>
        <p>Intelligent fuzzy matching and entity resolution.</p>
    </div>
    """, unsafe_allow_html=True)

with cols_powerful_features[1]:
    st.markdown("""
    <div class="feature-card">
        <div class="icon">🧩</div>
        <h3>Schema Harmonization</h3>
        <p>Automatically align disparate data structures.</p>
    </div>
    """, unsafe_allow_html=True)

with cols_powerful_features[2]:
    st.markdown("""
    <div class="feature-card">
        <div class="icon">💡</div>
        <h3>AI Suggestions</h3>
        <p>Smart recommendations for data mapping.</p>
    </div>
    """, unsafe_allow_html=True)

with cols_powerful_features[3]:
    st.markdown("""
    <div class="feature-card">
        <div class="icon">📈</div>
        <h3>Interactive Analytics</h3>
        <p>Rich dashboards and business intelligence.</p>
    </div>
    """, unsafe_allow_html=True)


# --- Footer Section ---
st.markdown("""
<div class="footer">
    <div class="logo-text">💠 NexusFlow</div>
    <p class="slogan">Transform your data chaos into business clarity</p>
</div>
""", unsafe_allow_html=True)

logger.info(f"App.py: Displaying main landing page.")