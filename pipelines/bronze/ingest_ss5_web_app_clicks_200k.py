"""
ingest_ss3_sale.py
------------------
Incrementally ingests Sales.Sale from the MishoCorp MSSQL database
into nessie.bronze.ss3_sale.

Run from project root (Anaconda Prompt):
    python pipelines/bronze/ingest_ss3_sale.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from conf.spark_config import get_spark_session
from conf.config_loader import CONFIG
from pipelines.framework import db_ingestor

from datetime import datetime

spark = get_spark_session("ingest-ss5-web-app-clicks-200k")

_FETCH_SIZE_ = "50000"

cfg = db_ingestor.JdbcConfig(
    url     = CONFIG['db_sources']['ss5']['url'],
    user    = CONFIG['db_sources']['ss5']['user'],
    password= CONFIG['db_sources']['ss5']['password'],
    driver  = CONFIG['db_sources']['ss5']['driver'],
    fetch_size=_FETCH_SIZE_
)

print(datetime.now())

db_ingestor.run(
    source_system   ="ss5",
    jdbc_cfg        =cfg,
    source_table    =f"{CONFIG['db_sources']['ss5']['db_name']}.sales.web_app_clicks_200k",
    target_table    =f"{CONFIG['schemas']['bronze']}.ss5_web_app_clicks",
    watermark_column="click_id",
    spark           =spark,
    log_table       =CONFIG['logging']['db_ingestion_log_table'],
)

print(datetime.now())

spark.stop()
