# src/data_processing_utils.py
import pandas as pd
import numpy as np
import re
from datetime import datetime
from dateutil import parser
from .config import (
    logger, DEFAULT_UNKNOWN_CATEGORICAL, DEFAULT_UNKNOWN_NUMERIC_INT,
    DEFAULT_UNKNOWN_NUMERIC_FLOAT, DEFAULT_STATUS_UNKNOWN
    # GENDER_MAP, CUSTOMER_STATUS_MAP, etc. are used in etl_pipelines.py, not directly here unless a util func specifically needs one
)

def clean_string(value, case=None, default_if_empty=None):
    """Cleans a string: strips whitespace, optionally converts case, returns default if empty."""
    if pd.isna(value) or value is None: # Handle None explicitly
        return default_if_empty
    value_str = str(value).strip()
    if not value_str: # if after stripping it's empty
        return default_if_empty if default_if_empty is not None else None # Return None if no default
    
    if case == 'lower':
        return value_str.lower()
    elif case == 'upper':
        return value_str.upper()
    elif case == 'title':
        # A more robust title case for names like "McDonald" or "O'Malley"
        # This is a simple version; more complex regex might be needed for all edge cases.
        return ' '.join(word.capitalize() for word in value_str.split())
    return value_str

def standardize_categorical(value, mapping_dict, default_value=DEFAULT_UNKNOWN_CATEGORICAL, case_transform='upper'):
    """Standardizes categorical values using a mapping dictionary. Handles None input for mapping_dict keys."""
    cleaned_value_for_lookup = clean_string(value, case=case_transform) # Normalize the input value
    
    if cleaned_value_for_lookup is None: # if input was NaN or empty string became None
        # Check if None or an empty string (after transform) is a key in the map
        if None in mapping_dict:
            return mapping_dict[None]
        if "" in mapping_dict and case_transform is None: # Only if no case transform happened
             return mapping_dict[""]
        return default_value # Default if None/empty string is not explicitly mapped

    return mapping_dict.get(cleaned_value_for_lookup, default_value)


def parse_date_robustly(date_val, output_format='%Y-%m-%d', error_val=None):
    """Parses various date/datetime formats and returns a string in specified format or error_val."""
    if pd.isna(date_val) or str(date_val).strip() == '' or str(date_val).lower() == 'none':
        return error_val
    try:
        # Handle potential float inputs from CSVs that represent dates as numbers (e.g. Excel dates)
        # This is a very basic heuristic, real Excel date conversion is more complex.
        if isinstance(date_val, (float, np.floating)):
             if date_val > 20000 and date_val < 80000 : # Common range for Excel serial dates
                # Assuming date_val is an Excel serial number (days since 1899-12-30)
                dt_obj = pd.to_datetime('1899-12-30') + pd.to_timedelta(date_val, 'D')
                return dt_obj.strftime(output_format)
             else: # If it's a float but not in Excel date range, treat as error for date parsing
                logger.debug(f"Float value '{date_val}' not in typical Excel date range. Returning error_val.")
                return error_val

        dt_obj = parser.parse(str(date_val))
        return dt_obj.strftime(output_format)
    except (ValueError, TypeError, parser.ParserError) as e:
        logger.debug(f"Date parsing failed for '{date_val}': {e}. Returning error_val.")
        return error_val

def to_numeric_safe(value, target_type=float, default_value=None):
    """
    Safely converts a value to a specified numeric type (float or int).
    Removes common non-numeric characters like currency symbols.
    """
    if pd.isna(value) or str(value).strip().lower() in ['', 'none', 'null', 'na']: # More robust check for "empty"
        return default_value
    
    s_value = str(value).strip()
        
    # Remove currency symbols, commas (for thousands), and other non-essential chars.
    # Keep minus sign, decimal point, and 'e' for scientific notation.
    s_value = re.sub(r'[^\d\.\-eE]', '', s_value)
    
    # After stripping, if it's empty or just a leftover char, return default
    if not s_value or s_value == '.' or s_value == '-':
        return default_value
    
    try:
        num = float(s_value) # Always parse as float first for flexibility
        if target_type == int:
            return int(num)
        return num
    except ValueError:
        logger.debug(f"Could not convert '{value}' (cleaned: '{s_value}') to {target_type.__name__}.")
        return default_value

def standardize_boolean_strict(value,
                               true_values={'true', 'yes', '1', 'active', 'completed', 'delivered', 't'},
                               false_values={'false', 'no', '0', 'inactive', 'failed', 'pending', 'cancelled', 'returned', 'f'}):
    """Standardizes various inputs to Python boolean True/False, or None if ambiguous or truly null."""
    if pd.isna(value) or value is None: # Explicitly return None for pd.NA or np.nan or Python None
        return None
    
    s_value = str(value).strip().lower()
    if not s_value or s_value == 'none' or s_value == 'null' or s_value == 'na': # Empty string or common null strings
        return None

    if s_value in true_values:
        return True
    if s_value in false_values:
        return False
    
    # Handle direct boolean True/False input
    if isinstance(value, bool):
        return value

    # Handle numeric representations if they weren't in true/false_values
    try:
        # Use a stricter check for numbers that clearly mean true/false
        num_value = float(s_value)
        if np.isclose(num_value, 1.0): return True
        if np.isclose(num_value, 0.0): return False
    except ValueError:
        pass # Not a number, and not in string lists

    logger.debug(f"Ambiguous boolean value: '{value}'. Could not map to True/False. Returning None.")
    return None # Ambiguous

def standardize_phone_strict(phone_str):
    if pd.isna(phone_str) or str(phone_str).strip().lower() in ['', 'none', 'null']:
        return None
    digits = re.sub(r'\D', '', str(phone_str)) # Remove all non-digits
    
    if len(digits) == 10: # Standard US 10-digit
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    elif len(digits) == 11 and digits.startswith('1'): # US 11-digit with country code 1
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
    elif len(digits) >= 7 and len(digits) <= 15 : # Plausible length for other formats or international numbers
        logger.debug(f"Non-standard but plausible phone format for '{phone_str}', returning cleaned digits: {digits}")
        return digits 
    else: # Too short, too long, or no digits
        logger.debug(f"Invalid or unparseable phone number '{phone_str}', returning None.")
        return None

def standardize_postal_code(code_str):
    if pd.isna(code_str) or str(code_str).strip().lower() in ['', 'none', 'null']:
        return None
    
    cleaned_code = str(code_str).strip()
    if not cleaned_code: return None

    # Try to extract a 5-digit code, or 5-4 format.
    match_5_4 = re.match(r'^(\d{5})[-\s]?(\d{4})$', cleaned_code)
    if match_5_4:
        return match_5_4.group(1) # Return just the 5-digit part

    match_5 = re.match(r'^(\d{5})$', cleaned_code)
    if match_5:
        return match_5.group(1)
    
    # Handle cases where zip might be float string like "12345.0"
    if '.' in cleaned_code:
        cleaned_code_float_part = cleaned_code.split('.')[0]
        if re.match(r'^\d{5}$', cleaned_code_float_part):
            return cleaned_code_float_part
            
    logger.debug(f"Could not standardize postal code: '{code_str}'. Original value (cleaned): '{cleaned_code}' might be invalid or non-US.")
    # Return the cleaned string if it has some digits and plausible length, otherwise None
    if re.search(r'\d', cleaned_code) and len(cleaned_code) >= 3: # Basic check
        return cleaned_code
    return None


def get_current_timestamp_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

logger.info("Data processing utilities defined in src/data_processing_utils.py.")