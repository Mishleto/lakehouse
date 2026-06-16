"""
text_file_ingestor.py
---------------------
Framework module for ingesting text-based files (XML, JSON, CSV-as-raw,
or any other text format) as raw strings into a single bronze Iceberg table.

Bronze strategy
---------------
Text files with hierarchical or semi-structured content (XML, JSON) should
not be parsed in the bronze layer. Bronze stores the full file content as a
raw string — one row per file — preserving exactly what arrived. Silver dbt
models handle parsing and structuring.

Public API
----------
run(source_system, source_dir, name_pattern, target_table, spark,
    log_table)

The caller provides a directory and a glob pattern such as 'pos_*.xml' or
'events_*.json'. All matching files are resolved at runtime with
pathlib.Path.glob(). Each file is tracked and guarded independently in
file_ingestion_log so that re-runs only pick up files that have not yet
been successfully processed.

Target table schema
-------------------
  _source_system        STRING
  _ingestion_timestamp  TIMESTAMP
  _source_file          STRING    -- full file path (used as log key)
  file_name             STRING    -- filename only, for convenience
  raw_content           STRING    -- full file content as UTF-8 string

The column is named raw_content rather than raw_xml or raw_json because
bronze does not interpret the content. Silver models derive the format
from _source_system and _source_file context.

Sequence (mirrors csv_ingestor Template Method pattern)
-------------------------------------------------------
Glob phase:
  0. Discover    - list files matching source_dir / name_pattern, sorted
                   for deterministic ordering; warn and return if none found

Per-file loop:
  1. Skip guard  - skip file if already successfully processed
  2. Read        - read file as plain text (UTF-8)
  3. Enrich      - build single-row DataFrame with metadata columns
  4. Write       - append_or_create via iceberg_writer
  5. Log success - write to file_ingestion_log
     On error    - log failure, continue remaining files

After loop:
  Raise RuntimeError listing all failed files if any failed.
  Caller owns spark.stop().

Future extension
----------------
Queue-based sources (Kafka, etc.) will be handled by a separate
text_queue_ingestor module. The two modules will share a common
_write_raw_row() helper to avoid duplication. Queue ingestion has a
fundamentally different interface (poll + offset commit) and "already
processed" is managed by the queue offset, not by file_ingestion_log.
"""

import traceback
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from pipelines.framework.iceberg_writer import append_or_create
from pipelines.framework.ingestion_logger import is_file_processed, log_file_run

_TEXT_SCHEMA = StructType([
    StructField("_source_system",        StringType(),    False),
    StructField("_ingestion_timestamp",  TimestampType(), False),
    StructField("_source_file",          StringType(),    False),
    StructField("file_name",             StringType(),    False),
    StructField("raw_content",           StringType(),    False),
])


def _write_raw_row(
    spark: SparkSession,
    source_system: str,
    source_file: str,
    file_name: str,
    raw_content: str,
    ingestion_ts: datetime,
    target_table: str,
) -> None:
    """
    Build a single-row DataFrame and append it to the target Iceberg table.
    Extracted as a standalone helper for future reuse by text_queue_ingestor.
    """
    row = [(
        source_system,
        ingestion_ts,
        source_file,
        file_name,
        raw_content,
    )]
    df = spark.createDataFrame(row, schema=_TEXT_SCHEMA)

    append_or_create(df, target_table)


def _ingest_file(
    spark: SparkSession,
    source_system: str,
    source_file: str,
    target_table: str,
    log_table: str,
) -> bool:
    """
    Process one text file. Returns True on success (or skip), False on failure.
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
        raw_content = Path(source_file).read_text(encoding="utf-8")
        print(f"[read] {len(raw_content)} characters read.")

        # --- 3. Enrich + 4. Write ---------------------------------------------
        ingestion_ts = datetime.now(timezone.utc)
        print(f"[write] Appending to {target_table} ...")
        _write_raw_row(
            spark=spark,
            source_system=source_system,
            source_file=source_file,
            file_name=Path(source_file).name,
            raw_content=raw_content,
            ingestion_ts=ingestion_ts,
            target_table=target_table,
        )
        print("[write] Done.")

        # --- 5. Log success ---------------------------------------------------
        # row_count=1: one file always produces one bronze row regardless of
        # how many logical records (transactions, events) the file contains.
        log_file_run(
            spark, source_system, source_file, target_table,
            status="success", row_count=1, log_table=log_table,
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
    log_table: str = "nessie.meta.file_ingestion_log",
) -> None:
    """
    Discover and ingest all text files matching name_pattern in source_dir
    into a single target_table as raw string rows.

    Parameters
    ----------
    source_system : e.g. 'ss4'
    source_dir    : absolute path to the folder containing the files,
                    e.g. '/home/mihailcho/lakehouse/data/ss4/'
    name_pattern  : glob expression matched against filenames inside
                    source_dir, e.g. 'pos_*.xml', 'events_*.json', '*.xml'
    target_table  : fully-qualified Iceberg table all files are appended to,
                    e.g. 'nessie.bronze.ss4_pos_log'
    spark         : active SparkSession (caller creates and stops it)
    log_table     : ingestion log table (default: nessie.meta.file_ingestion_log)

    Raises
    ------
    RuntimeError listing all failed files if any file failed, after
    processing all discovered files.
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

    # --- Per-file loop --------------------------------------------------------
    failed_files = []

    for path in matched:
        source_file = path.as_posix()
        success = _ingest_file(
            spark, source_system, source_file,
            target_table, log_table,
        )
        if not success:
            failed_files.append(source_file)

    # --- Final gate -----------------------------------------------------------
    if failed_files:
        raise RuntimeError(
            f"text_file_ingestor: finished with failures in files: {failed_files}"
        )

    print(f"\n[done] All matched files processed into {target_table}.")
