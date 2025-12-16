# load_crm_customers.py
import json
import pandas as pd
from sqlalchemy.dialects.mysql import JSON as MYSQL_JSON
from sqlalchemy import types
from db_connection import get_engine

# 1. engine
engine = get_engine("bronze")

# 2. read csv 
df = pd.read_csv("data/raw/cust_info.csv", dtype=str)   # read everything as string to preserve raw
# 3. normalize headers (metadata only)
df.columns = df.columns.str.strip().str.lower()

# 4. raw_row: keep original values exactly (no modification). Convert NaN->None
df = df.where(pd.notnull(df))
df['raw_row'] = df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)

# 5. create helper sanitized columns mapping to your bronze table (nullable)
# Ensure these names exist in table: cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date_raw
df['cst_id'] = df.get('cst_id')                       # will be None if missing
df['cst_key'] = df.get('cst_key')
df['cst_firstname'] = df.get('cst_firstname')
df['cst_lastname'] = df.get('cst_lastname')
df['cst_marital_status'] = df.get('cst_marital_status')
df['cst_gndr'] = df.get('cst_gndr')
df['cst_create_date_raw'] = df.get('cst_create_date')  # keep raw string

# 6. write to SQL
# Note: MySQL JSON column support — specify dtype mapping for raw_row
dtype_map = {
    "raw_row": MYSQL_JSON,   # or types.Text if JSON gives trouble
    "cst_id": types.VARCHAR(50),
    "cst_key": types.VARCHAR(100),
    # other columns omitted for brevity
}

# Use chunksize for safety
df[['raw_row','cst_id','cst_key','cst_firstname','cst_lastname','cst_marital_status','cst_gndr','cst_create_date_raw']].to_sql(
    name='crm_customers_info',
    con=engine,
    if_exists='append',
    index=False,
    chunksize=1000
)

print("done")
python/bronze/load_bronze.py
import pandas as pd
import numpy as np
from typing import Any, cast
import json
import os
import sys

# --- Path Setup to find 'utils' module ---
# Add the project root to sys.path so we can import from 'python.utils'
current_dir = os.path.dirname(os.path.abspath(__file__)) # python/bronze
python_folder = os.path.dirname(current_dir) # python
if python_folder not in sys.path:
    sys.path.append(python_folder)

# Now we can import cleanly
from utils.db_connection import get_engine
from utils.paths import get_raw_data_path # Import our new helper
from sqlalchemy import types
from sqlalchemy.dialects.mysql import JSON as MYSQL_JSON

def load_cust_info():
    print("--- Starting Bronze Load: Customers ---")
    
    # 1. Get Engine
    engine = get_engine("bronze")

    # 2. Get File Path using Helper (Magic happens here!)
    csv_path = get_raw_data_path("cust_info.csv")
    
    print(f"Reading file from: {csv_path}")
    
    if not os.path.exists(csv_path):
        print(f"ERROR: File not found at {csv_path}")
        return

    # 3. Read CSV
    df = pd.read_csv(csv_path, dtype=str)
    
    # 4. Normalize Headers
    df.columns = df.columns.str.strip().str.lower()
    
    # 5. Raw Row Logic (Bronze Layer Requirement)
    df = df.where(pd.notnull(df),np.nan)
    df['raw_row'] = df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)

    # 6. Select Columns
    df['cst_id'] = df.get('cst_id')
    df['cst_key'] = df.get('cst_key')
    # ... (Add other columns as needed)

    # 7. Write to SQL
    dtype_map = {
        "raw_row": MYSQL_JSON,
        "cst_id": types.VARCHAR(50),
        "cst_key": types.VARCHAR(100)
    }

    df[['raw_row', 'cst_id', 'cst_key']].to_sql(
        name='crm_customers_info',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000,
        dtype=cast(Any, dtype_map)
    )
    print("--- Success: Data Loaded to Bronze ---")

if __name__ == "__main__":
    load_cust_info()