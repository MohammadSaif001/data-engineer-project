from __future__ import annotations

import json
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Dict, Iterable

import pandas as pd
from sqlalchemy import types
from sqlalchemy.dialects.mysql import JSON as MYSQL_JSON

from utils.db_connection import get_engine
from utils.ingestion_checker import is_file_processed, mark_file_processed
from utils.logger import setup_logger
from utils.paths import get_raw_data_path

logger = setup_logger("bronze")


@dataclass(frozen=True)
class BronzeJob:
    source: str
    file_name: str
    table_name: str
    column_map: Dict[str, str]
    output_columns: list[str]
    dtype_map: Dict[str, Any]


def read_bronze_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    return df


def add_raw_row(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["raw_row"] = out.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
    return out


def apply_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    for dst, src in mapping.items():
        out[dst] = out.get(src)
    if "loaded_at" in out.columns:
        out["loaded_at"] = pd.Timestamp.now()
    return out


def run_job(job: BronzeJob) -> None:
    start = perf_counter()
    logger.info("[START] table=%s", job.table_name)

    if is_file_processed(job.source, job.file_name, job.table_name):
        logger.info("[SKIP] already processed table=%s", job.table_name)
        return

    engine = get_engine("bronze")
    csv_path = get_raw_data_path(f"{job.source}/{job.file_name}")

    df = read_bronze_csv(csv_path)
    df = add_raw_row(df)
    df = apply_mapping(df, job.column_map)

    cols = [c for c in job.output_columns if c in df.columns]
    df_final = df[cols]

    df_final.to_sql(
        name=job.table_name,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        dtype=job.dtype_map,
    )

    mark_file_processed(job.source, job.file_name, job.table_name)
    logger.info("[END] table=%s rows=%s elapsed=%.2fs", job.table_name, len(df_final), perf_counter() - start)


def build_jobs() -> Iterable[BronzeJob]:
    return [
        BronzeJob(
            source="source_crm",
            file_name="cust_info.csv",
            table_name="crm_customers_info",
            column_map={
                "cst_id": "cst_id",
                "cst_key": "cst_key",
                "cst_firstname": "cst_firstname",
                "cst_lastname": "cst_lastname",
                "cst_marital_status": "cst_marital_status",
                "cst_gndr": "cst_gndr",
                "cst_create_date_raw": "cst_create_date",
                "loaded_at": "loaded_at",
            },
            output_columns=[
                "raw_row",
                "cst_id",
                "cst_key",
                "cst_firstname",
                "cst_lastname",
                "cst_marital_status",
                "cst_gndr",
                "cst_create_date_raw",
                "loaded_at",
            ],
            dtype_map={
                "raw_row": MYSQL_JSON,
                "cst_id": types.VARCHAR(50),
                "cst_key": types.VARCHAR(100),
            },
        ),
        BronzeJob(
            source="source_crm",
            file_name="prd_info.csv",
            table_name="crm_prd_info",
            column_map={
                "prd_id": "prd_id",
                "prd_key": "prd_key",
                "prd_name": "prd_nm",
                "prd_cost": "prd_cost",
                "prd_line": "prd_line",
                "prd_start_date_raw": "prd_start_dt",
                "prd_end_date_raw": "prd_end_dt",
                "loaded_at": "loaded_at",
            },
            output_columns=[
                "raw_row",
                "prd_id",
                "prd_key",
                "prd_name",
                "prd_cost",
                "prd_line",
                "prd_start_date_raw",
                "prd_end_date_raw",
                "loaded_at",
            ],
            dtype_map={"raw_row": MYSQL_JSON, "prd_id": types.VARCHAR(50)},
        ),
        BronzeJob(
            source="source_crm",
            file_name="sales_details.csv",
            table_name="crm_sales_details",
            column_map={
                "sales_ord_num": "sls_ord_num",
                "sales_prd_key": "sls_prd_key",
                "sales_cust_id": "sls_cust_id",
                "sales_order_date_raw": "sls_order_dt",
                "sales_ship_date_raw": "sls_ship_dt",
                "sales_due_date_raw": "sls_due_dt",
                "sales_sales": "sls_sales",
                "sales_quantity": "sls_quantity",
                "sales_price": "sls_price",
                "loaded_at": "loaded_at",
            },
            output_columns=[
                "raw_row",
                "sales_ord_num",
                "sales_prd_key",
                "sales_cust_id",
                "sales_order_date_raw",
                "sales_ship_date_raw",
                "sales_due_date_raw",
                "sales_sales",
                "sales_quantity",
                "sales_price",
                "loaded_at",
            ],
            dtype_map={"raw_row": MYSQL_JSON, "sales_ord_num": types.VARCHAR(100)},
        ),
        BronzeJob(
            source="source_erp",
            file_name="CUST_AZ12.csv",
            table_name="erp_cust_az12",
            column_map={"cid": "cid", "birth_date_raw": "bdate", "gender_raw": "gen", "loaded_at": "loaded_at"},
            output_columns=["raw_row", "cid", "birth_date_raw", "gender_raw", "loaded_at"],
            dtype_map={"raw_row": MYSQL_JSON, "cid": types.VARCHAR(100)},
        ),
        BronzeJob(
            source="source_erp",
            file_name="LOC_A101.csv",
            table_name="erp_location_a101",
            column_map={"cid": "cid", "country_name": "cntry", "loaded_at": "loaded_at"},
            output_columns=["raw_row", "cid", "country_name", "loaded_at"],
            dtype_map={"raw_row": MYSQL_JSON, "cid": types.VARCHAR(100)},
        ),
        BronzeJob(
            source="source_erp",
            file_name="PX_CAT_G1V2.csv",
            table_name="erp_px_cat_g1v2",
            column_map={"id": "id", "cat": "cat", "subcat": "subcat", "maintenance_raw": "maintenance", "loaded_at": "loaded_at"},
            output_columns=["raw_row", "id", "cat", "subcat", "maintenance_raw", "loaded_at"],
            dtype_map={"raw_row": MYSQL_JSON, "id": types.VARCHAR(100)},
        ),
    ]


def run_bronze_pipeline() -> None:
    batch_start = perf_counter()
    logger.info("[BATCH START] Bronze pipeline")
    for job in build_jobs():
        try:
            run_job(job)
        except Exception:
            logger.exception("[ERROR] table=%s", job.table_name)
            raise
    logger.info("[BATCH END] elapsed=%.2fs", perf_counter() - batch_start)
