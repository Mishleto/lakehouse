"""
ingest_ss4_pos_log.py
---------------------
Ingests POS log XML files from Source System 4 into nessie.bronze.ss4_pos_log.

Each XML file is stored as a single raw_content string row — no parsing,
no structure extraction. Silver dbt models handle XPath parsing into
relational tables.

Run from project root (Anaconda Prompt):
    python pipelines/bronze/ingest_ss4_pos_log.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from conf.spark_config import get_spark_session
from conf.config_loader import CONFIG
from pipelines.framework import text_file_ingestor

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "ss4"
SOURCE_DIR    = CONFIG['file_sources']['ss4_dir']
NAME_PATTERN  = "*.xml"
TARGET_TABLE  = f"{CONFIG['schemas']['bronze']}.ss4_pos_log"
LOG_TABLE     = CONFIG["logging"]["file_ingestion_log_table"]

# ---------------------------------------------------------------------------

spark = get_spark_session("ingest-ss4-pos-log")

try:
    text_file_ingestor.run(
        source_system=SOURCE_SYSTEM,
        source_dir=SOURCE_DIR,
        name_pattern=NAME_PATTERN,
        target_table=TARGET_TABLE,
        spark=spark,
        log_table=LOG_TABLE,
    )
finally:
    spark.stop()
