import os
import sys
import logging
import numpy as np
import pandas as pd 

current_dir = os.path.dirname(os.path.abspath(__file__))
silver_folder = os.path.dirname(current_dir)
python_folder = os.path.dirname(silver_folder)

if python_folder not in sys.path:
    sys.path.append(python_folder)             # .../python


from utils.db_connection import get_engine     
from utils.paths import get_raw_data_path, get_logs_path

def extract_from_bronze(table_name: str) -> pd.DataFrame:
    allowed_tables = {
        "crm_customers_info", "crm_prd_info", "crm_sales_details",
        "erp_cust_az12", "erp_location_a101", "erp_px_cat_g1v2",
    }
    if table_name not in allowed_tables:
        raise ValueError(f"Table name not allowed: {table_name}")
    engine = get_engine("bronze")
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception as e:
        raise RuntimeError(f"Failed to extract from bronze table {table_name}") from e
    
_log_path = get_logs_path("pipeline.log")
os.makedirs(os.path.dirname(_log_path), exist_ok=True)
logging.basicConfig(
    filename=_log_path,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

schema ={
    "cid"                 : "string",
    "birth_date_raw"      : "datetime64[ns]",
    "gender_raw"          : "string"
    }

#! high level schema enforcement function 
def enforce_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    for column, dtype in schema.items():

        if column not in df.columns:
            #! log warning and skip missing columns
            logging.warning(f"[SCHEMA WARNING] Column missing: {column}")
            continue
        if dtype in ("Int64", "int64", "float64"):
            df[column] = pd.to_numeric(df[column], errors="coerce")
            if dtype == "Int64":
                df[column] = df[column].astype("Int64")
        elif dtype.startswith("datetime"):
            df[column] = pd.to_datetime(df[column], errors="coerce")
        elif dtype == "boolean":
            df[column] = df[column].astype("boolean")
        elif dtype == "string":
            df[column] = df[column].astype("string")
        else:
            # fallback (rare cases)
            df[column] = df[column].astype(dtype)

    return df
#! Customer ID Standardization 
def standardize_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    if "cid" in df.columns:
        df = df.loc[df["cid"].str.len() >= 10].copy()
        df.loc[:, "cid"] = df["cid"].astype(str).str[-10:]
    else:
        logging.warning("[CID WARNING] 'cid' column not found.")
    return df
#! Gender Normalization 
def normalize_gender(df: pd.DataFrame) -> pd.DataFrame:
    if "gender_raw" in df.columns:
        df.loc[:, "gender_raw"] = (
            df["gender_raw"]
            .str.strip()
            .replace({
                "M": "Male",
                "F": "Female",
                "": pd.NA
            })
        )
    else:
        logging.warning("[GENDER WARNING] 'gender_raw' column not found.")
    return df
#! Drop technical columns that are not needed in the silver layer
def drop_technical_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["raw_row"], errors="ignore")
    return df

