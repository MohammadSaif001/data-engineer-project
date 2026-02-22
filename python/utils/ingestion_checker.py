from __future__ import annotations

from pathlib import Path

import pandas as pd

from .logger import setup_logger
from .paths import get_project_root

logger = setup_logger("ingestion_checker")
PROCESSED_FILE = Path(get_project_root()) / "data" / "processed" / "processed_files.csv"


def is_file_processed(source: str, file_name: str, bronze_table: str) -> bool:
    if not PROCESSED_FILE.exists() or PROCESSED_FILE.stat().st_size == 0:
        return False

    try:
        df = pd.read_csv(PROCESSED_FILE)
    except pd.errors.EmptyDataError:
        return False

    return (
        (df["source"].str.lower().str.strip() == source.lower().strip())
        & (df["file_name"].str.lower().str.strip() == file_name.lower().strip())
        & (df["bronze_table"].str.lower().str.strip() == bronze_table.lower().strip())
    ).any()


def mark_file_processed(source: str, file_name: str, bronze_table: str) -> None:
    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    row = pd.DataFrame([[source, file_name, bronze_table]], columns=["source", "file_name", "bronze_table"])

    if PROCESSED_FILE.exists() and PROCESSED_FILE.stat().st_size > 0:
        row.to_csv(PROCESSED_FILE, mode="a", header=False, index=False)
    else:
        row.to_csv(PROCESSED_FILE, index=False)

    logger.info("Marked file processed source=%s file=%s table=%s", source, file_name, bronze_table)
