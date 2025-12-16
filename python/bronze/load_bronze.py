import json
import pandas as pd
import numpy as np
import os
import sys
from sqlalchemy import types
from typing import Any, cast
from sqlalchemy.dialects.mysql import JSON as MYSQL_JSON

# --- 1. System Path Setup (Ye sabse zaruri hai imports ke liye) ---
# Current script: python/bronze/load_bronze.py
# Hume 'python' folder ko sys.path mein add karna hai taaki 'utils' import ho sake
current_dir = os.path.dirname(os.path.abspath(__file__)) # .../python/bronze
python_folder = os.path.dirname(current_dir)             # .../python

if python_folder not in sys.path:
    sys.path.append(python_folder)

# Ab hum utils se safely import kar sakte hain
from utils.db_connection import get_engine
from utils.paths import get_raw_data_path

def load_cust_info():
    print("--- Starting Bronze Load: Customers ---")
    
    # --- 2. Connect to Database ---
    try:
        engine = get_engine("bronze")
        print("Database connected successfully.")
    except Exception as e:
        print(f"Connection Error: {e}")
        return

    # --- 3. Locate CSV File ---
    # Aapke tree.txt ke mutabik file 'source_crm' folder ke andar hai
    csv_filename = os.path.join("source_crm", "cust_info.csv")
    csv_path = get_raw_data_path(csv_filename)
    
    print(f"Reading CSV from: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"CRITICAL ERROR: File nahi mili at {csv_path}")
        print("Please check if file exists in 'data/raw/source_crm/'")
        return

    # --- 4. Read & Transform ---
    try:
        # Read all as string to preserve raw format (Phone numbers, dates, etc.)
        df = pd.read_csv(csv_path, dtype=str)
        
        # Normalize headers (remove spaces, lowercase)
        df.columns = df.columns.str.strip().str.lower()

        # Handle NaNs for JSON dump (NaN is not valid JSON)
        df_temp = df.where(pd.notnull(df), np.nan)
        
        # Create 'raw_row' - pure copy of source data in JSON format
        df['raw_row'] = df_temp.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
        
        # Optional: Add load timestamp
        df['load_timestamp'] = pd.Timestamp.now()

        # --- 5. Map Columns ---
        # .get() 
        df['cst_id'] = df.get('cst_id')
        df['cst_key'] = df.get('cst_key')
        df['cst_firstname'] = df.get('cst_firstname')
        df['cst_lastname'] = df.get('cst_lastname')
        df['cst_marital_status'] = df.get('cst_marital_status')
        df['cst_gndr'] = df.get('cst_gndr')
        df['cst_create_date_raw'] = df.get('cst_create_date')

        # Select columns to write
        final_cols = [
            'raw_row', 'cst_id', 'cst_key', 'cst_firstname', 
            'cst_lastname', 'cst_marital_status', 'cst_gndr', 
            'cst_create_date_raw', 'load_timestamp'
        ]
        
        # Filter dataframe to only include columns that actually exist now
        cols_to_write = [c for c in final_cols if c in df.columns]
        df_final = df[cols_to_write]

        # --- 6. Write to MySQL ---
        dtype_map = {
            "raw_row": MYSQL_JSON,
            "cst_id": types.VARCHAR(50),
            "cst_key": types.VARCHAR(100),
            "cst_firstname": types.VARCHAR(100)
        }

        print(f"Writing {len(df_final)} rows to table 'crm_customers_info'...")
        
        df_final.to_sql(
            name='crm_customers_info',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000,
            dtype=cast(Any, dtype_map)
        )
        
        print("--- Success: Data Loaded to Bronze Layer ---")

    except Exception as e:
        print(f"Processing Error: {e}")

def load_prd_info():
    print("--- Starting Bronze Load: Product ---")
    
    # --- 2. Connect to Database ---
    try:
        engine = get_engine("bronze")
        print("Database connected successfully.")
    except Exception as e:
        print(f"Connection Error: {e}")
        return
    csv_filename = os.path.join("source_crm", "prd_info.csv")
    csv_path = get_raw_data_path(csv_filename)
    print(f"Reading CSV from: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"CRITICAL ERROR: File nahi mili at {csv_path}")
        print("Please check if file exists in 'data/raw/source_crm/'")
        return

    # --- 4. Read & Transform ---
    try:
        # Read all as string to preserve raw format (Phone numbers, dates, etc.)
        df = pd.read_csv(csv_path, dtype=str)
        
        # Normalize headers (remove spaces, lowercase)
        df.columns = df.columns.str.strip().str.lower()

        # Handle NaNs for JSON dump (NaN is not valid JSON)
        df_temp = df.where(pd.notnull(df), np.nan)
        
        # Create 'raw_row' - pure copy of source data in JSON format
        df['raw_row'] = df_temp.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
        df['prd_id'] = df.get('prd_id')
        df['prd_key'] = df.get('prd_key')
        df['prd_name']  = df.get('prd_nm')
        df['prd_cost'] = df.get('prd_cost')
        df['prd_line'] = df.get('prd_line')
        df['prd_start_date_raw'] = df.get('prd_start_dt')
        df['prd_end_date_raw'] = df.get('prd_end_dt')

        final_cols = ['raw_row','prd_id','prd_key',
                      'prd_name','prd_cost','prd_line',
                      'prd_start_date_raw','prd_end_date_raw']
        cols_to_write = [c for c in final_cols if c in df.columns]
        df_final = df[cols_to_write]
        dtype_map = {
            "raw_row": MYSQL_JSON,
            "prd_id": types.VARCHAR(50),
            "prd_key": types.VARCHAR(100),
            "prd_name": types.VARCHAR(100)
        }
        print(f"Writing {len(df_final)} rows to table 'crm_prd_info'...")
        df_final.to_sql(
            name='crm_prd_info',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000,
            dtype=cast(Any, dtype_map)
        )
        
        print("--- Success: Data Loaded to Bronze Layer ---")
    except Exception as e:
        print(f"Processing Error: {e}")
if __name__ == "__main__":
    # load_cust_info()
    load_prd_info()