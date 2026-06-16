"""
create_meta_tables.py
---------------------
One-time setup script. Creates the nessie.meta namespace and both
ingestion log tables if they do not already exist.

Run from project root (Anaconda Prompt):
    python pipelines/create_meta_tables.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from conf.spark_config import get_spark_session
from conf.config_loader import CONFIG

from pipelines.framework.sql_sanitizer import validate_object_identifier

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

spark = get_spark_session("create-meta-tables")

# ---------------------------------------------------------------------------
# Create namespace
# ---------------------------------------------------------------------------

ns_name = CONFIG['schemas']['meta']

validate_object_identifier(ns_name)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns_name}")
print(f"[meta] Namespace {ns_name} ready.")

# ---------------------------------------------------------------------------
# file_ingestion_log
# Tracks every file processed into the bronze layer.
# Used to avoid reprocessing the same file twice.
# ---------------------------------------------------------------------------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ns_name}.file_ingestion_log (
        source_system       STRING,
        source_file         STRING,
        target_table        STRING,
        processed_at        TIMESTAMP,
        row_count           BIGINT,
        status              STRING,
        message             STRING
    )
    USING iceberg
""")
print(f"[meta] Table {ns_name}.file_ingestion_log ready.")

# ---------------------------------------------------------------------------
# db_ingestion_log
# Tracks every incremental pull from a database source (Oracle, MSSQL, etc.).
# watermark_min / watermark_max store the range of the watermark column
# (e.g. change_timestamp or ID) that was pulled in that run.
# Both are stored as STRING so they work for timestamp, integer, or other types.
# The pipeline casts them to the correct type when building the next query.
# ---------------------------------------------------------------------------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ns_name}.db_ingestion_log (
        source_system       STRING,
        source_table        STRING,
        target_table        STRING,
        processed_at        TIMESTAMP,
        row_count           BIGINT,
        watermark_column    STRING,
        watermark_min       STRING,
        watermark_max       STRING,
        status              STRING,
        message             STRING
    )
    USING iceberg
""")
print(f"[meta] Table {ns_name}.db_ingestion_log ready.")

# ---------------------------------------------------------------------------

spark.stop()
print("[meta] Done.")
