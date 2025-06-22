# src/config.py
import os
import logging
import pandas as pd # For pd.set_option

# --- Project Root ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- Logging Setup ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# --- Pandas Display Options ---
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', 1000)

# --- File Paths ---
# Raw data directory
DATA_DIR_RAW = os.path.join(PROJECT_ROOT, 'data')

# Specific paths for initially known files (used in main_etl.py to load them)
CUSTOMERS_MESSY_JSON_ORIG_NAME = 'customers_messy_data.json'
PRODUCTS_INCONSISTENT_JSON_ORIG_NAME = 'products_inconsistent_data.json'
ORDERS_UNSTRUCTURED_CSV_ORIG_NAME = 'orders_unstructured_data.csv'
RECONCILIATION_DATA_CSV_ORIG_NAME = 'reconciliation_challenge_data.csv'

CUSTOMERS_MESSY_JSON_ORIG = os.path.join(DATA_DIR_RAW, CUSTOMERS_MESSY_JSON_ORIG_NAME)
PRODUCTS_INCONSISTENT_JSON_ORIG = os.path.join(DATA_DIR_RAW, PRODUCTS_INCONSISTENT_JSON_ORIG_NAME)
ORDERS_UNSTRUCTURED_CSV_ORIG = os.path.join(DATA_DIR_RAW, ORDERS_UNSTRUCTURED_CSV_ORIG_NAME)
RECONCILIATION_DATA_CSV_ORIG = os.path.join(DATA_DIR_RAW, RECONCILIATION_DATA_CSV_ORIG_NAME)


# --- Database ---
DB_NAME = 'unified_ecommerce.db'
DB_PATH = os.path.join(PROJECT_ROOT, DB_NAME) # Save DB in project root
DB_ENGINE_URL = f'sqlite:///{DB_PATH}'

# --- ETL Constants ---
DEFAULT_UNKNOWN_CATEGORICAL = 'UNKNOWN'
DEFAULT_UNKNOWN_NUMERIC_INT = 0 # Or use pd.NA for nullable integers
DEFAULT_UNKNOWN_NUMERIC_FLOAT = 0.0 # Or use np.nan
DEFAULT_STATUS_UNKNOWN = 'UNKNOWN'

# --- Standardization Maps (from your notebook) ---
GENDER_MAP = {
    'M': 'MALE', 'F': 'FEMALE', 'MALE': 'MALE', 'FEMALE': 'FEMALE',
    'OTHER': 'OTHER', '': DEFAULT_UNKNOWN_CATEGORICAL, ' ': DEFAULT_UNKNOWN_CATEGORICAL,
    'NONE': DEFAULT_UNKNOWN_CATEGORICAL # From exploration
}
CUSTOMER_STATUS_MAP = { # Standardized to UPPERCASE
    'ACTIVE': 'ACTIVE', 'INACTIVE': 'INACTIVE', 'PENDING': 'PENDING',
    'SUSPENDED': 'SUSPENDED', '': DEFAULT_STATUS_UNKNOWN, ' ': DEFAULT_STATUS_UNKNOWN,
    'NONE': DEFAULT_STATUS_UNKNOWN # From exploration
}
PAYMENT_STATUS_MAP = { # Standardized to UPPERCASE
    'COMPLETED': 'COMPLETED', 'PENDING': 'PENDING', 'FAILED': 'FAILED',
    '': DEFAULT_STATUS_UNKNOWN
}
ORDER_DELIVERY_STATUS_MAP = { # Standardized to UPPERCASE
    'DELIVERED': 'DELIVERED', 'PENDING': 'PENDING', 'IN_TRANSIT': 'IN_TRANSIT',
    'PROCESSING': 'PROCESSING', 'SHIPPED': 'SHIPPED', 'CANCELLED': 'CANCELLED',
    'RETURNED': 'RETURNED', '': DEFAULT_STATUS_UNKNOWN, ' ': DEFAULT_STATUS_UNKNOWN,
    'NONE': DEFAULT_STATUS_UNKNOWN # From exploration for order_status
}
STATE_ABBREVIATION_MAP = { # Keys should be uppercase for consistent lookup
    'CALIFORNIA': 'CA', 'NEW YORK': 'NY', 'ILLINOIS': 'IL', 'TEXAS': 'TX',
    'PENNSYLVANIA': 'PA', 'ARIZONA': 'AZ',
    'CA': 'CA', 'NY': 'NY', 'IL': 'IL', 'TX': 'TX', 'PA': 'PA', 'AZ': 'AZ'
    # Add more as needed
}
CITY_NORMALIZATION_MAP = { # Keys should be uppercase for consistent lookup
    'LA': 'Los Angeles', 'LOSANGELES': 'Los Angeles', 'LOS ANGELES': 'Los Angeles',
    'NYC': 'New York', 'NEW YORK CITY': 'New York', 'NEW_YORK': 'New York', 'NEW YORK': 'New York',
    'PHILA': 'Philadelphia', 'PHILADELPHIA': 'Philadelphia',
    'CHICAGO': 'Chicago', 'CHGO': 'Chicago',
    'PHOENIX': 'Phoenix',
    'HOUSTON': 'Houston'
    # Add more as needed
}

# For dynamic file processing (more advanced, used conceptually in new Streamlit page)
# This defines how known files are parsed and what entity they represent.
KNOWN_FILE_SOURCES_METADATA = {
    CUSTOMERS_MESSY_JSON_ORIG_NAME: {'entity': 'customer', 'type': 'json', 'parser_func': 'read_json'},
    PRODUCTS_INCONSISTENT_JSON_ORIG_NAME: {'entity': 'product', 'type': 'json', 'parser_func': 'read_json'},
    ORDERS_UNSTRUCTURED_CSV_ORIG_NAME: {'entity': 'order_items_unstructured', 'type': 'csv', 'parser_func': 'read_csv'},
    RECONCILIATION_DATA_CSV_ORIG_NAME: {'entity': 'order_items_reconciliation', 'type': 'csv', 'parser_func': 'read_csv'},
}

# Gemini API Key (will be fetched from env or input in ai_reconciliation.py)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

logger.info("Configuration loaded from src/config.py.")