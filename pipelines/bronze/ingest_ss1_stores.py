"""
ingest_ss1_stores.py
-----------------------
Ingests files with name pattern stores_*.csv from Source System 1 into nessie.bronze.ss1_stores.

Run from project root (Anaconda Prompt):
    python pipelines/bronze/ingest_ss1_stores.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from conf.spark_config import get_spark_session
from conf.config_loader import CONFIG
from pipelines.framework import csv_ingestor

spark = get_spark_session("ingest-ss1-stores")

csv_ingestor.run(
    source_system = "ss1",
    source_dir    = CONFIG["file_sources"]["ss1_dir"],
    name_pattern  = "stores_*.csv",
    target_table  = f"{CONFIG['schemas']['bronze']}.ss1_stores",
    spark         = spark,
    log_table     = CONFIG["logging"]["file_ingestion_log_table"],
)

spark.stop()