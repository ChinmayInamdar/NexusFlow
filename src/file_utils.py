# src/file_utils.py
import pandas as pd
import os
from .config import logger # Assuming logger is defined in config

def basic_profiler(file_path):
    """
    Tries to read the file and get row/column count. Supports CSV and JSON.
    Returns (row_count, col_count).
    """
    row_count, col_count = None, None
    file_name = os.path.basename(file_path)
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            logger.warning(f"File {file_name} is empty or does not exist. Skipping profiling.")
            return None, None

        if file_path.lower().endswith('.csv'):
            # For CSV, try to infer delimiter and count rows/cols
            try:
                # Read first few lines for structure
                df_head = pd.read_csv(file_path, nrows=5, low_memory=False)
                if not df_head.empty:
                    col_count = len(df_head.columns)
                else: # File might be empty or just a header
                    df_full_check = pd.read_csv(file_path, low_memory=False)
                    col_count = len(df_full_check.columns) if not df_full_check.empty else 0

                # More robust row count for CSV
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    row_count = sum(1 for _ in f)
                if col_count is not None and row_count > 0: # If header exists
                    row_count -=1 # Exclude header row from data row count
                
            except pd.errors.EmptyDataError:
                logger.warning(f"CSV file {file_name} is empty or contains no data.")
                row_count, col_count = 0, 0
            except Exception as csv_e:
                logger.warning(f"Could not fully profile CSV {file_name}: {csv_e}. Attempting basic read.")
                # Fallback if complex CSV parsing fails
                df = pd.read_csv(file_path, low_memory=False)
                row_count = len(df)
                col_count = len(df.columns)


        elif file_path.lower().endswith('.json'):
            # For JSON, structure can vary.
            try: # Try line-delimited JSON first
                df_head = pd.read_json(file_path, lines=True, nrows=5)
                if not df_head.empty:
                    col_count = len(df_head.columns)
                    # Count lines for row_count
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        row_count = sum(1 for _ in f)
                else: # If lines=True gives empty, try as a single JSON object/array
                    data = pd.read_json(file_path) # Might be a list of records or a dict of lists
                    if isinstance(data, pd.DataFrame):
                        row_count = len(data)
                        col_count = len(data.columns) if row_count > 0 else 0
                    elif isinstance(data, pd.Series) and isinstance(data.iloc[0], list): # E.g. {"col": [1,2,3]}
                        row_count = len(data.iloc[0])
                        col_count = len(data)
                    elif isinstance(data, dict): # E.g. a single record as dict, or dict of columns
                         # This case is harder to generically profile into rows/cols without assumptions
                         df_from_dict = pd.DataFrame([data]) if not any(isinstance(v, list) for v in data.values()) else pd.DataFrame(data)
                         row_count = len(df_from_dict)
                         col_count = len(df_from_dict.columns)
                    else:
                         logger.warning(f"JSON file {file_name} has an unrecognized structure for profiling.")
            except ValueError: # If not lines=True, try normal read_json
                 data_val_err = pd.read_json(file_path)
                 if isinstance(data_val_err, pd.DataFrame):
                    row_count = len(data_val_err)
                    col_count = len(data_val_err.columns) if row_count > 0 else 0
                 else:
                    logger.warning(f"JSON file {file_name} (ValueError path) has an unrecognized structure for profiling.")
            except Exception as json_e:
                logger.warning(f"Could not profile JSON {file_name}: {json_e}")
        
        if row_count is not None or col_count is not None:
            logger.info(f"Basic profiling for {file_name}: Rows={row_count}, Cols={col_count}")
        else:
            logger.info(f"Could not determine row/column count for {file_name}")

    except Exception as e:
        logger.warning(f"Could not basic profile file {file_name}: {e}", exc_info=True)
    
    return row_count, col_count

logger.info("File utilities (basic_profiler) defined in src/file_utils.py.")