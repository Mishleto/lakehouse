"""
csv_ingestor.py
---------------
Framework module for ingesting one or more CSV files into a single bronze
Iceberg table.

Public API
----------
run(source_system, source_dir, name_pattern, target_table, spark,
    log_table, mandatory_columns, on_missing_column, on_new_column)

The caller provides a directory and a glob pattern such as 'store_*.csv'.
All matching files are resolved at runtime with pathlib.Path.glob().
Each file is tracked and guarded independently in file_ingestion_log so
that re-runs only pick up files that have not yet been successfully
processed.

Sequence (Template Method pattern)
-----------------------------------
Glob phase:
  0. Discover      - list files matching source_dir / name_pattern, sorted
                     for deterministic ordering; warn and return if none found

Per-file loop:
  1. Skip guard    - skip file if already successfully processed
  2. Read          - spark.read.csv with header + inferSchema
  3. Enrich        - add _source_system, _ingestion_timestamp, _source_file
  4. Align schema  - validate mandatory columns; handle missing/new columns
                     per on_missing_column / on_new_column policy
  5. Write         - append_or_create via iceberg_writer; merge_schema driven
                     by on_new_column policy returned from align_schema
  6. Log success   - write to file_ingestion_log
     On error      - log failure, continue remaining files

After loop:
  Raise RuntimeError listing all failed files if any failed.
  Caller owns spark.stop().
"""

import traceback
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from pipelines.framework.iceberg_writer import append_or_create
from pipelines.framework.ingestion_logger import is_file_processed, log_file_run
from pipelines.framework.schema_utils import align_schema


def _ingest_file(
    spark: SparkSession,
    source_system: str,
    source_file: str,
    target_table: str,
    log_table: str,
    mandatory_columns: list[str],
    on_missing_column: str,
    on_new_column: str,
) -> bool:
    """
    Process one CSV file. Returns True on success (or skip), False on failure.
    Internal helper — not part of the public API.
    """
    print(f"\n[file] {source_file} -> {target_table}")

    # --- 1. Skip guard --------------------------------------------------------
    if is_file_processed(spark, source_file, log_table):
        print("[skip] Already successfully processed. Skipping.")
        return True

    try:
        # --- 2. Read ----------------------------------------------------------
        print(f"[read] Reading {source_file} ...")
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(source_file)
        )
        row_count = raw_df.count()
        print(f"[read] {row_count} rows, {len(raw_df.columns)} columns.")

        # --- 3. Enrich --------------------------------------------------------
        ingestion_ts = datetime.now(timezone.utc)
        enriched_df = (
            raw_df
            .withColumn("_source_system",      F.lit(source_system))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
            .withColumn("_source_file",         F.lit(source_file))
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
            spark, source_system, source_file, target_table,
            status="success", row_count=row_count, log_table=log_table,
        )
        return True

    except Exception as e:
        print(f"[error] Failed to process '{source_file}':\n{traceback.format_exc()}")
        log_file_run(
            spark, source_system, source_file, target_table,
            status="failed", message=str(e), log_table=log_table,
        )
        return False


def run(
    source_system: str,
    source_dir: str,
    name_pattern: str,
    target_table: str,
    spark: SparkSession,
    log_table: str,
    mandatory_columns: list[str] | None = None,
    on_missing_column: str = "fill_null",
    on_new_column: str = "add",
) -> None:
    """
    Discover and ingest all CSV files matching name_pattern in source_dir
    into a single target_table.

    Parameters
    ----------
    source_system     : e.g. 'ss1'
    source_dir        : absolute path to the folder containing the CSV files
    name_pattern      : glob expression, e.g. 'store_*.csv'
    target_table      : fully-qualified Iceberg table, e.g. nessie.bronze.ss1_stores
    spark             : active SparkSession (caller creates and stops it)
    log_table         : ingestion log table
    mandatory_columns : columns that must exist in source; fails if any missing.
                        Pass None or [] to skip validation.
    on_missing_column : "fill_null" (default) — fill columns present in bronze
                        table but absent from source with NULL.
                        "fail" — raise if any such columns exist.
    on_new_column     : "add" (default) — automatically add new source columns
                        to the bronze table via mergeSchema.
                        "fail" — raise if source has columns not in bronze table.

    Raises
    ------
    RuntimeError listing all failed files if any file failed.
    """
    # --- 0. Discover ----------------------------------------------------------
    matched = sorted(Path(source_dir).glob(name_pattern))

    if not matched:
        print(
            f"[warn] No files matched '{name_pattern}' in '{source_dir}'. "
            "Nothing to ingest."
        )
        return

    print(f"[glob] Found {len(matched)} file(s) matching '{name_pattern}' in '{source_dir}'.")

    mandatory_columns = mandatory_columns or []

    # --- Per-file loop --------------------------------------------------------
    failed_files = []

    for path in matched:
        source_file = path.as_posix()
        success = _ingest_file(
            spark, source_system, source_file,
            target_table, log_table,
            mandatory_columns, on_missing_column, on_new_column,
        )
        if not success:
            failed_files.append(source_file)

    # --- Final gate -----------------------------------------------------------
    if failed_files:
        raise RuntimeError(
            f"csv_ingestor: finished with failures in files: {failed_files}"
        )

    print(f"\n[done] All matched files processed into {target_table}.")