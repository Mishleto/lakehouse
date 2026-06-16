"""
excel_ingestor.py
-----------------
Framework module for ingesting one or more sheets from all Excel files in a
folder that match a given filename pattern into bronze Iceberg tables.

Public API
----------
run(source_system, source_dir, name_pattern, sheets, spark, log_table,
    mandatory_columns, on_missing_column, on_new_column)

Sequence (Template Method pattern)
-----------------------------------
Glob folder for files matching name_pattern; raise if none found.
For each matched file:
  For each (sheet_name, target_table) in sheets:
    1. Skip guard    - skip sheet if already successfully processed
    2. Read          - pandas read_excel -> NaN cleaned -> Spark DataFrame
    3. Enrich        - add _source_system, _ingestion_timestamp, _source_file
    4. Align schema  - validate mandatory columns; handle missing/new columns
                       per on_missing_column / on_new_column policy
    5. Write         - append_or_create via iceberg_writer; merge_schema driven
                       by on_new_column policy returned from align_schema
    6. Log success   - write to file_ingestion_log
       On sheet error - log failure, continue remaining sheets/files
After all files: raise if any sheet failed. 
"""

import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F

from pipelines.framework.iceberg_writer import append_or_create
from pipelines.framework.ingestion_logger import is_file_processed, log_file_run
from pipelines.framework.schema_utils import align_schema


def _make_source_file_key(excel_file: str, sheet_name: str) -> str:
    """Combines file path and sheet name into a single trackable key."""
    return f"{excel_file}::{sheet_name}"


def _ingest_sheet(
    spark: SparkSession,
    source_system: str,
    excel_file: str,
    sheet_name: str,
    target_table: str,
    log_table: str,
    mandatory_columns: list[str],
    on_missing_column: str,
    on_new_column: str,
) -> bool:
    """
    Process one sheet. Returns True on success (or skip), False on failure.
    Internal helper — not part of the public API.
    """
    source_file_key = _make_source_file_key(excel_file, sheet_name)
    print(f"\n[sheet] {sheet_name} -> {target_table}")

    # --- 1. Skip guard --------------------------------------------------------
    if is_file_processed(spark, source_file_key, log_table):
        print("[skip] Already successfully processed. Skipping.")
        return True

    try:
        # --- 2. Read ----------------------------------------------------------
        print(f"[read] Reading sheet '{sheet_name}' from {excel_file} ...")
        pandas_df = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl")
        print(f"[read] {len(pandas_df)} rows, {len(pandas_df.columns)} columns.")

        pandas_df = pandas_df.where(pd.notna(pandas_df), other=None)
        raw_df = spark.createDataFrame(pandas_df)

        # --- 3. Enrich --------------------------------------------------------
        ingestion_ts = datetime.now(timezone.utc)
        enriched_df = (
            raw_df
            .withColumn("_source_system",      F.lit(source_system))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
            .withColumn("_source_file",         F.lit(source_file_key))
        )

        # --- 4. Align schema --------------------------------------------------
        aligned_df, merge_schema = align_schema(
            enriched_df, target_table, spark,
            mandatory_columns=mandatory_columns,
            on_missing_column=on_missing_column,
            on_new_column=on_new_column,
        )

        # --- 5. Write ---------------------------------------------------------
        print(f"[write] Appending to {target_table} ...")
        append_or_create(aligned_df, target_table, merge_schema=merge_schema)
        print("[write] Done.")

        # --- 6. Log success ---------------------------------------------------
        log_file_run(
            spark, source_system, source_file_key, target_table,
            status="success", row_count=len(pandas_df), log_table=log_table,
        )
        return True

    except Exception as e:
        print(f"[error] Failed to process sheet '{sheet_name}':\n{traceback.format_exc()}")
        log_file_run(
            spark, source_system, source_file_key, target_table,
            status="failed", message=str(e), log_table=log_table,
        )
        return False


def run(
    source_system: str,
    source_dir: str,
    name_pattern: str,
    sheets: list[tuple[str, str]],
    spark: SparkSession,
    log_table: str,
    mandatory_columns: list[str] | None = None,
    on_missing_column: str = "fill_null",
    on_new_column: str = "add",
) -> None:
    """
    Ingest all sheets from every Excel file in source_dir whose name matches
    name_pattern into their respective Iceberg tables.

    Parameters
    ----------
    source_system     : e.g. 'ss2'
    source_dir        : directory to scan for Excel files
    name_pattern      : glob-style filename pattern, e.g. 'ss2_products_*.xlsx'
    sheets            : list of (sheet_name, target_table) tuples applied to
                        every matched file
    spark             : active SparkSession (caller creates and stops it)
    log_table         : ingestion log table
    mandatory_columns : columns that must exist in every sheet; fails if any
                        missing. Pass None or [] to skip validation.
    on_missing_column : "fill_null" (default) or "fail"
    on_new_column     : "add" (default) or "fail"

    Raises
    ------
    FileNotFoundError if no files match the pattern.
    RuntimeError      if one or more sheets failed, after processing all files.
    """
    matched_files = sorted(Path(source_dir).glob(name_pattern))

    if not matched_files:
        raise FileNotFoundError(
            f"excel_ingestor: no files matching '{name_pattern}' found in '{source_dir}'"
        )

    print(f"[scan] Found {len(matched_files)} file(s) matching '{name_pattern}' in '{source_dir}':")
    for f in matched_files:
        print(f"       {f}")

    mandatory_columns = mandatory_columns or []
    failed: list[str] = []

    for excel_file in matched_files:
        excel_file_str = excel_file.as_posix()
        print(f"\n[file] Processing {excel_file_str}")

        for sheet_name, target_table in sheets:
            success = _ingest_sheet(
                spark, source_system, excel_file_str,
                sheet_name, target_table, log_table,
                mandatory_columns, on_missing_column, on_new_column,
            )
            if not success:
                failed.append(_make_source_file_key(excel_file_str, sheet_name))

    if failed:
        raise RuntimeError(
            f"excel_ingestor: finished with failures in:\n" +
            "\n".join(f"  {key}" for key in failed)
        )

    print(f"\n[done] All files processed from '{source_dir}' (pattern: '{name_pattern}').")