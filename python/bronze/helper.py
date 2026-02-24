import os
import sys
import json
import logging
import pandas as pd
import numpy as np

current_dir = os.path.dirname(os.path.abspath(__file__))
python_folder = os.path.dirname(current_dir)
if python_folder not in sys.path:
    sys.path.append(python_folder)

from utils.paths import get_logs_path

_log_path = get_logs_path("pipeline.log")
os.makedirs(os.path.dirname(_log_path), exist_ok=True)
logging.basicConfig(
    filename=_log_path,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def read_bronze_csv(csv_path: str) -> pd.DataFrame:
    """
    Bronze CSV reader:
    - checks file exists
    - reads all columns as string
    - normalizes headers
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found at: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str)
    logging.info(f"Loaded Bronze CSV from: {csv_path} | Shape: {df.shape}")
    df.columns = df.columns.str.strip().str.lower()
    return df


def add_raw_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds raw_row JSON column for Bronze layer
    """
    df_temp = df.where(pd.notnull(df), np.nan)
    df["raw_row"] = df_temp.apply(
        lambda r: json.dumps(r.to_dict(), default=str),
        axis=1
    )
    logging.debug("Added 'raw_row' column to DataFrame")
    return df
