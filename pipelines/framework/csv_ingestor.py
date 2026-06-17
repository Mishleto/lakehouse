"""
csv_ingestor.py
---------------
Framework module for ingesting one or more CSV files into a single bronze
Iceberg table.

Public API
----------
run(source_system, source_dir, name_pattern, target_table, spark,
    log_table, mandatory_columns, mode, schema_evolution)

The caller provides a directory and a glob pattern such as 'store_*.csv'.
All matching files are resolved at runtime with pathlib.Path.glob().
Already-processed files are excluded upfront with a single log table query.

Modes and schema_evolution flag
---------------------
mode="sequential" (default)
    Files are processed one by one. Schema is inferred independently per
    file. One Iceberg write and one log entry per file.
    schema_evolution=True (default): permissive alignment — new columns are added to
        the target table; columns missing from source are filled with NULL.
    schema_evolution=False: schema must match the target exactly — new or missing
        columns raise immediately.

mode="bulk"
    All unprocessed files are read in a single Spark operation.
    One Iceberg write; one log entry per file (row counts via groupBy).
    schema_evolution=True (default): mergeSchema=True on read; permissive alignment.
    schema_evolution=False: mergeSchema=False on read (Spark errors on schema
        differences across files); strict alignment against target.

Sequence — sequential mode
---------------------------
  0. Discover + filter  glob source_dir; exclude already-processed files
                        with a single log table query.
  Per-file loop:
    1. Read          - spark.read.csv with inferSchema
    2. Enrich        - add _source_system, _ingestion_timestamp, _source_file
    3. Align schema  - validate mandatory columns; apply strict/permissive policy
    4. Write         - append_or_create via iceberg_writer
    5. Log success   - one entry per file
       On error      - log failure, continue remaining files
  Raise RuntimeError listing all failed files if any failed.

Sequence — bulk mode
--------------------
  0. Discover + filter  glob source_dir; exclude already-processed files.
  1. Read all      - spark.read.csv(all_files) with mergeSchema=True/False;
                     _metadata.file_path captured as _source_file
  2. Enrich        - add _source_system, _ingestion_timestamp
  3. Align schema  - validate mandatory columns; apply strict/permissive policy
  4. Cache         - cache aligned_df to avoid double scan
  5. Count         - groupBy(_source_file).count() for per-file log entries
  6. Write         - single append_or_create
  7. Log           - one entry per file from per-file counts
     On error      - log failure for every file in the batch, re-raise

  Caller owns spark.stop().
"""

import traceback
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from pipelines.framework.iceberg_writer import append_or_create
from pipelines.framework.ingestion_logger import get_processed_files, log_file_run, log_bulk_file_run
from pipelines.framework.schema_utils import align_schema


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _discover_and_filter(
    source_dir: str,
    name_pattern: str,
    spark: SparkSession,
    log_table: str,
) -> tuple[list[str], int]:
    """
    Glob source_dir for name_pattern, then exclude already-processed files
    using a single log table query.

    Returns (unprocessed_files, total_matched_count).
    """
    matched = sorted(Path(source_dir).glob(name_pattern))
    total = len(matched)

    if not matched:
        return [], 0

    norm_dir = Path(source_dir).as_posix().rstrip("/") + "/"
    processed = set(get_processed_files(spark, norm_dir, name_pattern, log_table))
    unprocessed = [p.as_posix() for p in matched if p.as_posix() not in processed]

    skipped = total - len(unprocessed)
    if skipped:
        print(f"[skip] {skipped} file(s) already processed. Excluding from this run.")

    return unprocessed, total


def _enrich(df, source_system: str):
    """Add _source_system and _ingestion_timestamp columns."""
    ingestion_ts = datetime.now(timezone.utc)
    return (
        df
        .withColumn("_source_system",      F.lit(source_system))
        .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )


def _align_policy(schema_evolution: bool) -> tuple[str, str]:
    """Return (on_missing_column, on_new_column) based on schema_evolution flag."""
    if schema_evolution:
        return "fill_null", "add"
    return "fail", "fail"


# ---------------------------------------------------------------------------
# Sequential mode
# ---------------------------------------------------------------------------

def _ingest_file_sequential(
    spark: SparkSession,
    source_system: str,
    source_file: str,
    target_table: str,
    log_table: str,
    mandatory_columns: list[str],
    schema_evolution: bool,
) -> bool:
    """Process one CSV file. Returns True on success, False on failure."""
    print(f"\n[file] {source_file} -> {target_table}")

    on_missing_column, on_new_column = _align_policy(schema_evolution)

    try:
        # --- 1. Read ----------------------------------------------------------
        print(f"[read] Reading {source_file} ...")
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(source_file)
        )
        row_count = raw_df.count()
        print(f"[read] {row_count} rows, {len(raw_df.columns)} columns.")

        # --- 2. Enrich --------------------------------------------------------
        enriched_df = (
            _enrich(raw_df, source_system)
            .withColumn("_source_file", F.lit(source_file))
        )

        # --- 3. Align schema --------------------------------------------------
        aligned_df, merge_schema = align_schema(
            enriched_df, target_table, spark,
            mandatory_columns=mandatory_columns,
            on_missing_column=on_missing_column,
            on_new_column=on_new_column,
        )

        # --- 4. Write ---------------------------------------------------------
        print(f"[write] Appending to {target_table} ...")
        append_or_create(aligned_df, target_table, merge_schema=merge_schema)
        print("[write] Done.")

        # --- 5. Log success ---------------------------------------------------
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


def _run_sequential(
    spark: SparkSession,
    source_system: str,
    unprocessed: list[str],
    target_table: str,
    log_table: str,
    mandatory_columns: list[str],
    schema_evolution: bool,
) -> None:
    failed_files = []

    for source_file in unprocessed:
        success = _ingest_file_sequential(
            spark, source_system, source_file,
            target_table, log_table,
            mandatory_columns, schema_evolution,
        )
        if not success:
            failed_files.append(source_file)

    if failed_files:
        raise RuntimeError(
            f"csv_ingestor: finished with failures in files: {failed_files}"
        )


# ---------------------------------------------------------------------------
# Bulk mode
# ---------------------------------------------------------------------------

def _run_bulk(
    spark: SparkSession,
    source_system: str,
    unprocessed: list[str],
    target_table: str,
    log_table: str,
    mandatory_columns: list[str],
    schema_evolution: bool,
) -> None:
    """
    Read all unprocessed files in one Spark operation.
    _source_file is captured per row via _metadata.file_path.
    One Iceberg write; one log entry per file.
    """
    print(f"\n[bulk] Reading {len(unprocessed)} file(s) (schema_evolution={schema_evolution}) ...")

    on_missing_column, on_new_column = _align_policy(schema_evolution)

    try:
        # --- 1. Read all files ------------------------------------------------
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mergeSchema", str(schema_evolution).lower())
            .csv(unprocessed)
        )

        # --- 2. Enrich --------------------------------------------------------
        enriched_df = (
            _enrich(raw_df, source_system)
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

        # --- 3. Align schema --------------------------------------------------
        aligned_df, merge_schema = align_schema(
            enriched_df, target_table, spark,
            mandatory_columns=mandatory_columns,
            on_missing_column=on_missing_column,
            on_new_column=on_new_column,
        )

        # --- 4. Cache ---------------------------------------------------------
        aligned_df.cache()

        # --- 5. Per-file row counts -------------------------------------------
        per_file_counts: dict[str, int] = {
            row["_source_file"]: row["count"]
            for row in aligned_df.groupBy("_source_file").count().collect()
        }
        total_rows = sum(per_file_counts.values())
        print(f"[bulk] {total_rows} total rows across {len(per_file_counts)} file(s).")

        # --- 6. Write ---------------------------------------------------------
        print(f"[write] Appending to {target_table} ...")
        append_or_create(aligned_df, target_table, merge_schema=merge_schema)
        print("[write] Done.")

        aligned_df.unpersist()

        # --- 7. Log one entry per file ----------------------------------------
        log_bulk_file_run(
            spark, source_system, target_table,
            status="success",
            files=[(f, per_file_counts.get(f, 0)) for f in unprocessed],
            log_table=log_table,
        )

    except Exception as e:
        print(f"[error] Bulk ingest failed:\n{traceback.format_exc()}")
        log_bulk_file_run(
            spark, source_system, target_table,
            status="failed",
            files=[(f, 0) for f in unprocessed],
            message=str(e),
            log_table=log_table,
        )
        raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run(
    source_system: str,
    source_dir: str,
    name_pattern: str,
    target_table: str,
    spark: SparkSession,
    log_table: str,
    mandatory_columns: list[str] | None = None,
    mode: str = "sequential",
    schema_evolution: bool = True,
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
    mandatory_columns : columns that must exist in every source file; raises
                        immediately if any are missing. Pass None or [] to skip.
    mode              : "sequential" (default) — one file at a time, infer
                        schema per file.
                        "bulk" — all files in one Spark read operation.
    schema_evolution  : True (default) — permissive: new columns are added to
                        the target table; columns missing from source are filled
                        with NULL. In bulk mode, mergeSchema=True on read.
                        False — strict: schema must match exactly; new or missing
                        columns raise immediately. In bulk mode, mergeSchema=False
                        on read (Spark errors on schema differences across files).

    Raises
    ------
    ValueError   if mode is not "sequential" or "bulk".
    RuntimeError listing all failed files if any file failed (sequential mode),
                 or re-raised exception from the bulk write (bulk mode).
    """
    if mode not in {"sequential", "bulk"}:
        raise ValueError(
            f"Invalid mode {mode!r}. Must be 'sequential' or 'bulk'."
        )

    mandatory_columns = mandatory_columns or []

    # --- 0. Discover and filter -----------------------------------------------
    unprocessed, total = _discover_and_filter(
        source_dir, name_pattern, spark, log_table
    )

    if total == 0:
        print(
            f"[warn] No files matched '{name_pattern}' in '{source_dir}'. "
            "Nothing to ingest."
        )
        return

    print(
        f"[glob] Found {total} file(s) matching '{name_pattern}' in '{source_dir}'. "
        f"{len(unprocessed)} unprocessed."
    )

    if not unprocessed:
        print("[done] All matched files already processed. Nothing to do.")
        return

    # --- Dispatch -------------------------------------------------------------
    if mode == "sequential":
        _run_sequential(
            spark, source_system, unprocessed,
            target_table, log_table,
            mandatory_columns, schema_evolution,
        )
    else:
        _run_bulk(
            spark, source_system, unprocessed,
            target_table, log_table,
            mandatory_columns, schema_evolution,
        )

    print(f"\n[done] Finished ingesting into {target_table}.")