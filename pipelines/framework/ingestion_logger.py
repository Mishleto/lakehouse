"""
ingestion_logger.py
-------------------
Stateless utility for reading from and writing to the two Iceberg ingestion
log tables:

    nessie.meta.file_ingestion_log   - for CSV and Excel sources
    nessie.meta.db_ingestion_log     - for JDBC / database sources

Public API
----------
get_processed_files(spark, source_dir, file_name_pattern, log_table) -> list[str]
is_file_processed(spark, source_file_key, log_table)                 -> bool
log_file_run(spark, ..., log_table)
log_bulk_file_run(spark, ..., files, log_table)
log_db_run(spark, ..., log_table)

Design notes
------------
- No session ownership: all functions receive spark as a parameter.
- watermark_min / watermark_max are stored as STRING for type-agnostic
  comparison across runs (format: 'YYYY-MM-DD HH:MM:SS.mmm').
- message is truncated to 1000 chars to match the existing column width
  convention in the log tables.
"""

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from pipelines.framework.sql_sanitizer import validate_object_identifier

_FILE_LOG_SCHEMA = (
    "source_system STRING, source_file STRING, target_table STRING, "
    "processed_at TIMESTAMP, row_count BIGINT, status STRING, message STRING"
)

_DB_LOG_SCHEMA = (
    "source_system STRING, source_table STRING, target_table STRING, "
    "processed_at TIMESTAMP, row_count BIGINT, watermark_column STRING, "
    "watermark_min STRING, watermark_max STRING, status STRING, message STRING"
)


# ---------------------------------------------------------------------------
# Skip-guard helpers
# ---------------------------------------------------------------------------

def get_processed_files(
        spark:  SparkSession,
        source_dir: str,
        file_name_pattern: str,
        log_table: str
) -> list[str]:
    """
    Returns list with the names of all successfully processed files that
    match this name pattern.

    Used by file ingestors (csv, excel) to exclude already processed
    files / sheets upfront with a single query.
    """

    validate_object_identifier(log_table)

    safe_name_pattern = file_name_pattern.replace("'", "''").replace("*", "%")  # escape single quotes, replace glob wildcard with SQL wildcard
    safe_dir_name = source_dir.replace("'", "''")  # escape single quotes

    query_sql = f"""
        SELECT source_file
        FROM {log_table}
        WHERE status = 'success'
            and source_file like '{safe_dir_name}{safe_name_pattern}'
    """

    # print(f"\n sql query:\n {query_sql} \n")

    source_file_keys = [row["source_file"] for row in spark.sql(query_sql).collect()]

    return source_file_keys


def is_file_processed(
    spark: SparkSession,
    source_file_key: str,
    log_table: str,
) -> bool:
    """
    Returns True if source_file_key has a successful entry in log_table.

    Used by csv_ingestor and excel_ingestor to skip already-processed
    files / sheets.
    """

    validate_object_identifier(log_table)

    safe_key = source_file_key.replace("'", "''")  # escape single quotes

    query_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM   {log_table}
        WHERE  source_file = '{safe_key}'
        AND    status      = 'success'
    """

    count = spark.sql(query_sql).collect()[0]["cnt"]

    # better way, safe against sql injection but does not work with this version of spark
    # count = spark.sql(f"""
    #     SELECT COUNT(*) AS cnt
    #     FROM   {log_table}
    #     WHERE  source_file = :source_file
    #     AND    status      = 'success'
    # """, args={"source_file": source_file_key}).collect()[0]["cnt"]

    return count > 0


def get_last_watermark(
    spark: SparkSession,
    source_system: str,
    source_table: str,
    log_table: str,
) -> str | None:
    """
    Returns the watermark_max STRING from the most recent successful run
    for the given source_system + source_table combination.
    Returns None on first run (no prior success exists).
    """

    validate_object_identifier(log_table)

    safe_source_system = source_system.replace("'", "''")  # escape single quotes
    safe_source_table = source_table.replace("'", "''")  # escape single quotes

    result = spark.sql(f"""
        SELECT watermark_max
        FROM   {log_table}
        WHERE  source_system = '{safe_source_system}'
        AND    source_table  = '{safe_source_table}'
        AND    status        = 'success'
        ORDER  BY processed_at DESC
        LIMIT  1
    """,).collect()

    # result = spark.sql(f"""
    #     SELECT watermark_max
    #     FROM   {log_table}
    #     WHERE  source_system = :source_system
    #     AND    source_table  = :source_table
    #     AND    status        = 'success'
    #     ORDER  BY processed_at DESC
    #     LIMIT  1
    # """, args={"source_system":source_system, "source_table":source_table} ).collect()

    return result[0]["watermark_max"] if result else None


# ---------------------------------------------------------------------------
# Log writers
# ---------------------------------------------------------------------------

def log_file_run(
    spark: SparkSession,
    source_system: str,
    source_file: str,
    target_table: str,
    status: str,
    row_count: int = 0,
    message: str = "",
    log_table: str = "",
) -> None:
    """
    Appends one row to file_ingestion_log.

    Parameters
    ----------
    source_file : file path for CSV; 'file_path::SheetName' for Excel
    status      : 'success' or 'failed'
    """

    validate_object_identifier(log_table)

    log_data = [(
        source_system,
        source_file,
        target_table,
        datetime.now(timezone.utc),
        row_count,
        status,
        message[:1000],
    )]
    (
        spark.createDataFrame(log_data, schema=_FILE_LOG_SCHEMA)
        .writeTo(log_table)
        .append()
    )
    print(f"[log] {log_table}: status={status}, rows={row_count}, source={source_file}")


def log_bulk_file_run(
    spark: SparkSession,
    source_system: str,
    target_table: str,
    status: str,
    files: list[tuple[str, int]],
    message: str = "",
    log_table: str = "",
) -> None:
    """
    Appends one row per file to file_ingestion_log in a single write.

    Parameters
    ----------
    files   : list of (source_file, row_count) tuples — one entry per file.
              row_count should be 0 for failed runs where counts are unknown.
    status  : 'success' or 'failed'
    """

    validate_object_identifier(log_table)

    processed_at = datetime.now(timezone.utc)
    log_data = [
        (source_system, source_file, target_table, processed_at, row_count, status, message[:1000])
        for source_file, row_count in files
    ]
    (
        spark.createDataFrame(log_data, schema=_FILE_LOG_SCHEMA)
        .writeTo(log_table)
        .append()
    )
    print(
        f"[log] {log_table}: status={status}, {len(files)} file(s), "
        f"total rows={sum(rc for _, rc in files)}"
    )


def log_db_run(
    spark: SparkSession,
    source_system: str,
    source_table: str,
    target_table: str,
    status: str,
    row_count: int = 0,
    watermark_column: str = "",
    watermark_min: str | None = None,
    watermark_max: str | None = None,
    message: str = "",
    log_table: str = "",
) -> None:
    """
    Appends one row to db_ingestion_log.

    Parameters
    ----------
    watermark_min / watermark_max : string representations of the batch
        watermark bounds, e.g. '2026-01-15 08:22:11.000'
    """

    validate_object_identifier(log_table)

    log_data = [(
        source_system,
        source_table,
        target_table,
        datetime.now(timezone.utc),
        row_count,
        watermark_column,
        watermark_min,
        watermark_max,
        status,
        message[:1000],
    )]
    (
        spark.createDataFrame(log_data, schema=_DB_LOG_SCHEMA)
        .writeTo(log_table)
        .append()
    )
    print(
        f"[log] {log_table}: status={status}, rows={row_count}, "
        f"wm={watermark_min} -> {watermark_max}"
    )