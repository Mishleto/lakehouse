"""
db_ingestor.py
--------------
Framework module for incrementally ingesting a JDBC database table into a
bronze Iceberg table using an UpdatedAt-style watermark.

Public API
----------
JdbcConfig      - dataclass holding JDBC connection parameters
run(source_system, jdbc_cfg, source_table, target_table,
    watermark_column, spark, log_table,
    mandatory_columns, on_missing_column, on_new_column)

Sequence (Template Method pattern)
-----------------------------------
1. Watermark     - read last successful watermark_max from db_ingestion_log
2. Read          - spark.read.jdbc with push-down WHERE clause (or full load
                   on first run)
3. Early exit    - if row_count == 0, log success with unchanged watermark
4. Capture wm    - compute min/max of watermark_column from this batch
5. Enrich        - add _source_system, _ingestion_timestamp, _source_table
6. Align schema  - validate mandatory columns; handle missing/new columns
                   per on_missing_column / on_new_column policy
7. Write         - append_or_create via iceberg_writer; merge_schema driven
                   by on_new_column policy returned from align_schema.
                   On type mismatch failure, retries with all non-metadata
                   columns cast to STRING and logs a warning.
8. Log success   - write to db_ingestion_log
   On any error  - log failure, re-raise (caller owns spark.stop())

Design notes
------------
- Watermark values are stored and compared as strings to avoid timestamp
  type mismatches across Spark / MSSQL.
- Type-change fallback: unsafe type changes will cause Iceberg to reject
  the write. The fallback casts all non-metadata columns to STRING and
  retries once. A warning is printed and included in the log message.
- No session ownership: the caller creates and stops SparkSession.
"""

import traceback
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

from pipelines.framework.iceberg_writer import append_or_create
from pipelines.framework.ingestion_logger import get_last_watermark, log_db_run
from pipelines.framework.schema_utils import align_schema
from pipelines.framework.sql_sanitizer import validate_object_identifier

_METADATA_COLUMNS = {"_source_system", "_ingestion_timestamp", "_source_table"}


@dataclass
class JdbcConfig:
    """
    JDBC connection parameters for a SQL Server (or compatible) source.

    Attributes
    ----------
    url      : JDBC connection string, e.g.
               'jdbc:sqlserver://localhost:1433;databaseName=MishoCorp;encrypt=false'
    user     : database username
    password : database password
    driver   : fully-qualified JDBC driver class name, e.g.
               'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    partition_predicates : list of where conditions used to force spark to perform multiple reads
                one for each partition.
                strings must be in format "country_cd='GB'"
    fetch_size : how many rows to be fetched by each parallel process, and then flushed before next batch 
    """
    url: str
    user: str
    password: str
    driver: str
    partition_predicates: list[str]
    fetch_size: str


def _cast_payload_to_string(df):
    """
    Cast all non-metadata columns to StringType.
    Used as a fallback when the source schema has an unsafe type change
    that Iceberg cannot auto-promote.
    """
    for col_name in df.columns:
        if col_name not in _METADATA_COLUMNS:
            df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
    return df


def run(
    source_system: str,
    jdbc_cfg: JdbcConfig,
    source_table: str,
    target_table: str,
    watermark_column: str,
    spark: SparkSession,
    log_table: str,
    mandatory_columns: list[str] | None = None,
    on_missing_column: str = "fill_null",
    on_new_column: str = "add",
) -> None:
    """
    Incrementally ingest source_table into target_table using watermark_column.

    Parameters
    ----------
    source_system     : e.g. 'ss3'
    jdbc_cfg          : JdbcConfig with connection details
    source_table      : qualified table name, e.g. 'Sales.Sale'
    target_table      : fully-qualified Iceberg table, e.g. nessie.bronze.ss3_sale
    watermark_column  : column used for incremental tracking, e.g. 'UpdatedAt'
    spark             : active SparkSession (caller creates and stops it)
    log_table         : ingestion log table
    mandatory_columns : columns that must exist in source; fails if any missing.
                        Pass None or [] to skip validation.
    on_missing_column : "fill_null" (default) or "fail"
    on_new_column     : "add" (default) or "fail" 
    """
    type_cast_fallback_used = False
    mandatory_columns = mandatory_columns or []

    try:
        validate_object_identifier(source_table)

        # --- 1. Determine watermark -------------------------------------------
        last_watermark = get_last_watermark(
            spark, source_system, source_table, log_table
        )

        if last_watermark is None:
            print("[watermark] No prior run found. Performing full load.")
        else:
            print(f"[watermark] Last watermark: {last_watermark}. Pulling incremental.")

        # --- 2. Read ----------------------------------------------------------
        print(f"[read] Reading {source_table} via JDBC ...")

        if last_watermark is not None:
            table_query = (
                f"(SELECT * FROM {source_table} "
                f"WHERE {watermark_column} > '{last_watermark}') AS t"
            )
        else:
            table_query = f"(SELECT * FROM {source_table}) AS t"

        connection_options = {
            "user": jdbc_cfg.user,
            "password": jdbc_cfg.password,
            "driver": jdbc_cfg.driver,
            "fetchsize": jdbc_cfg.fetch_size
        }

        if jdbc_cfg.partition_predicates:

            raw_df = spark.read.jdbc(
                url=jdbc_cfg.url,
                table=source_table,
                predicates=jdbc_cfg.partition_predicates,  # Must be passed as a direct argument
                properties=connection_options
            )
        else:
            raw_df = spark.read.jdbc(
                url=jdbc_cfg.url,
                table=source_table,
                properties=connection_options
            )

        if last_watermark is not None:
            raw_df = raw_df.filter(F.col(watermark_column) > last_watermark)

        print(f"[read stats] Getting min/max values for watermark columns, and row count. Current time: {datetime.now()}")

        # --- 3. Capture batch stats ------------------------------------------
        bounds = raw_df.agg(
            F.count(watermark_column).alias("cnt"),
            F.min(watermark_column).alias("wm_min"),
            F.max(watermark_column).alias("wm_max"),
        ).collect()[0]

        watermark_min = str(bounds["wm_min"])
        watermark_max = str(bounds["wm_max"])
        row_count =  int(bounds["cnt"])

        if row_count == 0:
            print("[skip] No new or modified rows. Nothing to write.")
            log_db_run(
                spark, source_system, source_table, target_table,
                status="success", row_count=0,
                watermark_column=watermark_column,
                watermark_min=last_watermark,
                watermark_max=last_watermark,
                log_table=log_table,
            )
            return
    
        print(f"[watermark] Row Count: {row_count}. Batch range: {watermark_min} -> {watermark_max}. Current time: {datetime.now()} ")

        # --- 4. Enrich --------------------------------------------------------
        ingestion_ts = datetime.now(timezone.utc)
        enriched_df = (
            raw_df
            .withColumn("_source_system",      F.lit(source_system))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
            .withColumn("_source_table",        F.lit(source_table))
        )

        # --- 5. Align schema --------------------------------------------------
        aligned_df, merge_schema = align_schema(
            enriched_df, target_table, spark,
            mandatory_columns=mandatory_columns,
            on_missing_column=on_missing_column,
            on_new_column=on_new_column,
        )
        
        # aligned_df.write.option("overwrite", True).parquet("C:/tmp/test_output")
        
        # --- 6. Write (with type-change fallback) -----------------------------
        print(f"[write] Appending to {target_table} ...")
        try:
            append_or_create(aligned_df, target_table, merge_schema=merge_schema)
        except Exception as write_err:
            print(
                f"[warn] Write failed — attempting type-cast fallback "
                f"(casting all payload columns to STRING).\n"
                f"       Original error: {write_err}"
            )
            fallback_df = _cast_payload_to_string(aligned_df)
            append_or_create(fallback_df, target_table, merge_schema=merge_schema)
            type_cast_fallback_used = True
            print("[write] Done (type-cast fallback used).")
        else:
            print("[write] Done.")

        # --- 7. Log success ---------------------------------------------------
        log_db_run(
            spark, source_system, source_table, target_table,
            status="success", row_count=row_count,
            watermark_column=watermark_column,
            watermark_min=watermark_min,
            watermark_max=watermark_max,
            message="type-cast fallback used — check source for type changes"
                    if type_cast_fallback_used else "",
            log_table=log_table,
        )

    except Exception as e:
        print(f"[error] Pipeline failed:\n{traceback.format_exc()}")
        log_db_run(
            spark, source_system, source_table, target_table,
            status="failed", watermark_column=watermark_column,
            message=str(e), log_table=log_table,
        )
        raise