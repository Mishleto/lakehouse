"""
ingest_ss2.py
-------------
Finds all excel files matching name pattern products_*.xlsx 
Then Ingests all four sheets from these files into the bronze layer.

    products.xlsx::Products       -> nessie.bronze.ss2_products
    products.xlsx::Suppliers      -> nessie.bronze.ss2_suppliers
    products.xlsx::DeliveryPrices -> nessie.bronze.ss2_delivery_prices
    products.xlsx::SalesPrices    -> nessie.bronze.ss2_sales_prices

Run from project root (Anaconda Prompt):
    python pipelines/bronze/ingest_ss2_products.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from conf.spark_config import get_spark_session
from conf.config_loader import CONFIG
from pipelines.framework import excel_ingestor


spark = get_spark_session("ingest-ss2-products")

SHEETS = [('Products', f"{CONFIG['schemas']['bronze']}.ss2_products"),
          ('Suppliers', f"{CONFIG['schemas']['bronze']}.ss2_suppliers"),
          ('DeliveryPrices', f"{CONFIG['schemas']['bronze']}.ss2_delivery_prices"),
          ('SalesPrices', f"{CONFIG['schemas']['bronze']}.ss2_sales_prices")]

excel_ingestor.run(
    source_system = "ss2",
    source_dir    = CONFIG["file_sources"]["ss2_dir"],
    name_pattern  = "products_*.xlsx",
    sheets        = SHEETS,
    spark         = spark,
    log_table     = CONFIG["logging"]["file_ingestion_log_table"],
)


spark.stop()
