# src/data_processing_utils.py
import pandas as pd
import numpy as np
import re
from datetime import datetime, date
from dateutil import parser
from .config import logger, DEFAULT_UNKNOWN_CATEGORICAL # Ensure this is imported or defined

# --- String Cleaning ---
def clean_string(text, case=None, default_if_empty=None):
    if pd.isna(text) or text is None:
        return default_if_empty if default_if_empty is not None else None
    text_str = str(text).strip()
    if not text_str: # Empty after strip
        return default_if_empty if default_if_empty is not None else None
    
    # Remove non-printable characters except common whitespace like \n, \r, \t
    text_str = re.sub(r'[^\x20-\x7E\n\r\t]', '', text_str)
    text_str = ' '.join(text_str.split()) # Normalize whitespace to single spaces

    if case == 'lower': text_str = text_str.lower()
    elif case == 'upper': text_str = text_str.upper()
    elif case == 'title': text_str = text_str.title()
    return text_str

# --- Customer Name Standardization (NEW ADVANCED VERSION) ---
def standardize_customer_name_advanced(name_str):
    if pd.isna(name_str) or not str(name_str).strip():
        return DEFAULT_UNKNOWN_CATEGORICAL

    name = str(name_str).strip()

    # Remove email-like parts (e.g., @domain.com)
    name = re.sub(r'@\S+', '', name).strip()
    
    # Remove trailing numbers if they seem part of a username and not a suffix like "Jr III"
    # This heuristic looks for a letter followed by numbers at the end, without a preceding space before the numbers.
    if re.search(r'[a-zA-Z][0-9]+$', name) and not re.search(r'\s[IVX0-9]+$', name, re.IGNORECASE):
        name = re.sub(r'[0-9]+$', '', name).strip()

    # Replace common separators ('.', '_', '-') with a space, but not if it's part of "Jr." or "Sr."
    # This is tricky. A simpler replacement first:
    name = re.sub(r'[_]+', ' ', name) # Underscores are almost always separators
    name = re.sub(r'-(?![sS][rR]$|[jJ][rR]$)', ' ', name) # Hyphens not part of Sr/Jr
    name = re.sub(r'\.(?![jJ][rR]$|[sS][rR]$|\s|$)', ' ', name) # Periods not part of Jr./Sr. or end of sentence

    # Remove extra spaces that might have been introduced
    name = ' '.join(name.split())

    # Capitalize each part of the name (Title Case)
    # Handle special cases like "McDonald", "O'Malley" if needed, but title() is a good start.
    name_parts = []
    for part in name.split():
        if part.lower() in ["jr", "sr", "ii", "iii", "iv", "md", "phd", "dds"]:
            name_parts.append(part.lower().capitalize() + ".") # Jr. Sr.
        elif re.match(r"^(mc|mac|o')", part.lower()): # Mc, Mac, O'
            if part.lower().startswith("mc") and len(part) > 2:
                name_parts.append("Mc" + part[2:].capitalize())
            elif part.lower().startswith("mac") and len(part) > 3:
                name_parts.append("Mac" + part[3:].capitalize())
            elif part.lower().startswith("o'") and len(part) > 2:
                 name_parts.append("O'" + part[2:].capitalize())
            else:
                 name_parts.append(part.capitalize()) # Default for short Mc/Mac
        else:
            name_parts.append(part.capitalize())
    final_name = ' '.join(name_parts)

    # If after all this, the name is empty, too short, or just punctuation, return UNKNOWN
    if not final_name or len(final_name) < 2 or final_name.count(' ') == len(final_name) -1 : 
        return DEFAULT_UNKNOWN_CATEGORICAL
        
    return final_name


# --- Categorical Standardization ---
def standardize_categorical(value, mapping_dict, default_value=DEFAULT_UNKNOWN_CATEGORICAL, case_transform=None):
    if pd.isna(value): return default_value
    cleaned_value = str(value).strip()
    if not cleaned_value: return default_value
    
    if case_transform == 'lower': cleaned_value = cleaned_value.lower()
    elif case_transform == 'upper': cleaned_value = cleaned_value.upper()
    
    return mapping_dict.get(cleaned_value, default_value)

# --- Date Parsing ---
def parse_date_robustly(date_str, output_format='%Y-%m-%d', errors='coerce'):
    if pd.isna(date_str) or str(date_str).strip() == '' or str(date_str).lower() in ['na', 'none', 'null', 'unknown']:
        return None if errors == 'coerce' else pd.NaT # Return None or NaT for consistency
    
    # Handle if it's already a datetime object (e.g., from pd.to_datetime)
    if isinstance(date_str, (datetime, date, pd.Timestamp)):
        try:
            return date_str.strftime(output_format)
        except ValueError: # If output_format is for datetime but date_str is only date
             if isinstance(date_str, date) and not isinstance(date_str, datetime) and '%H' in output_format:
                 # If it's a date object and output expects time, just format as date
                 return date_str.strftime('%Y-%m-%d')
             return None if errors == 'coerce' else pd.NaT


    date_str_cleaned = str(date_str).strip()
    # Common replacements for clarity before parsing
    date_str_cleaned = date_str_cleaned.replace('/', '-').replace('.', '-')
    
    # Try direct parsing with common formats first for speed
    common_formats = [
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d',
        '%m-%d-%Y %H:%M:%S', '%m-%d-%Y %H:%M', '%m-%d-%Y',
        '%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%d-%m-%Y',
        '%Y%m%d', '%Y%m%d%H%M%S'
    ]
    for fmt in common_formats:
        try:
            dt_obj = datetime.strptime(date_str_cleaned, fmt)
            return dt_obj.strftime(output_format)
        except ValueError:
            continue
            
    # Fallback to dateutil.parser for more flexibility
    try:
        # dayfirst=True can be ambiguous, try both if initial parse fails without it
        # Try inferring based on separators or common patterns
        dayfirst_heuristic = (date_str_cleaned.count('-') == 2 and int(date_str_cleaned.split('-')[0]) > 12) or \
                             (date_str_cleaned.count('/') == 2 and int(date_str_cleaned.split('/')[0]) > 12)

        dt_obj = parser.parse(date_str_cleaned, dayfirst=dayfirst_heuristic)
        return dt_obj.strftime(output_format)
    except (ValueError, TypeError, OverflowError):
        logger.debug(f"Robust date parsing failed for: '{date_str_cleaned}' (original: '{date_str}')")
        return None if errors == 'coerce' else pd.NaT


# --- Numeric Conversion ---
def to_numeric_safe(value, target_type=float, default_value=None, errors='coerce'):
    if pd.isna(value):
        return default_value if default_value is not None else (pd.NA if target_type == int else np.nan)

    if isinstance(value, (int, float)) and not isinstance(value, bool): # bool is subclass of int
        try:
            return target_type(value)
        except (ValueError, TypeError):
            return default_value if default_value is not None else (pd.NA if target_type == int else np.nan)

    s_val = str(value).strip()
    if not s_val or s_val.lower() in ['na', 'none', 'null', 'unknown', '#n/a', 'nan']:
        return default_value if default_value is not None else (pd.NA if target_type == int else np.nan)

    # Remove common currency symbols, commas, percentage signs
    s_val = re.sub(r'[$,]', '', s_val)
    is_percentage = '%' in s_val
    s_val = s_val.replace('%', '')
    
    try:
        num = float(s_val)
        if is_percentage:
            num /= 100.0
        
        if target_type == int:
            if np.isnan(num): # Handle if float conversion results in NaN
                 return default_value if default_value is not None else pd.NA
            return int(round(num)) # Round before int conversion
        return num # target_type is float or implicitly handled
    except (ValueError, TypeError):
        if errors == 'coerce':
            return default_value if default_value is not None else (pd.NA if target_type == int else np.nan)
        else:
            raise

# --- Boolean Standardization ---
def standardize_boolean_strict(value, true_values=None, false_values=None, default_if_unknown=None):
    if true_values is None:
        true_values = {'true', 'yes', '1', 't', 'y', 'on', 'active'}
    if false_values is None:
        false_values = {'false', 'no', '0', 'f', 'n', 'off', 'inactive'}

    if pd.isna(value): return default_if_unknown
    
    val_str = str(value).strip().lower()
    if not val_str: return default_if_unknown # Empty string

    if val_str in true_values: return True
    if val_str in false_values: return False
    
    # Try converting to numeric if it looks like a number but wasn't in true/false sets
    try:
        num_val = float(val_str)
        if num_val == 1.0: return True
        if num_val == 0.0: return False
    except ValueError:
        pass # Not a number

    return default_if_unknown

# --- Phone Number Standardization ---
def standardize_phone_strict(phone_str, default_if_invalid=None):
    if pd.isna(phone_str): return default_if_invalid
    cleaned = re.sub(r'\D', '', str(phone_str)) # Remove all non-digits
    
    if len(cleaned) == 10: # Standard US 10-digit
        return f"({cleaned[0:3]}) {cleaned[3:6]}-{cleaned[6:10]}"
    elif len(cleaned) == 11 and cleaned.startswith('1'): # US 11-digit with country code
        return f"+1 ({cleaned[1:4]}) {cleaned[4:7]}-{cleaned[7:11]}"
    # Add more rules for other formats or lengths if needed
    
    # If it doesn't match known valid formats after cleaning, return default
    logger.debug(f"Phone number '{phone_str}' (cleaned: '{cleaned}') did not match strict formats.")
    return default_if_invalid

# --- Postal Code Standardization ---
def standardize_postal_code(postal_code_str, country='US', default_if_invalid=None):
    if pd.isna(postal_code_str): return default_if_invalid
    pc_str = str(postal_code_str).strip()
    
    if country == 'US':
        pc_str = re.sub(r'[^0-9]', '', pc_str) # Remove non-digits
        if len(pc_str) == 5:
            return pc_str
        elif len(pc_str) == 9: # ZIP+4
            return f"{pc_str[:5]}-{pc_str[5:]}"
    # Add rules for other countries if needed
    # elif country == 'CA': ...

    logger.debug(f"Postal code '{postal_code_str}' (cleaned: '{pc_str}') did not match {country} format.")
    return default_if_invalid

# --- Timestamp ---
def get_current_timestamp_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

logger.info("Data processing utilities defined in src/data_processing_utils.py.")