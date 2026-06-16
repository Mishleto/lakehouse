import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from conf.config_loader import CONFIG

PROJECT_ROOT = Path(__file__).resolve().parents[1]

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.makedirs(CONFIG["s3a"]["buffer_dir"], exist_ok=True)

# Jar filenames are stable across environments — only the project root
# (and therefore the absolute path) changes.
JAR_FILES = [
    "iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
    "nessie-spark-extensions-3.5_2.13-0.99.0.jar",
    "hadoop-aws-3.4.1.jar",
    "bundle-2.29.51.jar",
]


def get_spark_session(app_name: str = "lakehouse") -> SparkSession:
    jar_paths = [str(PROJECT_ROOT / "jars" / name) for name in JAR_FILES]
    catalog = CONFIG["nessie"]["catalog_name"]

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(CONFIG["spark"]["master"])
        .config("spark.driver.memory", CONFIG["spark"]["driver_memory"])
        .config("spark.jars", ",".join(jar_paths))
        .config("spark.sql.shuffle.partitions", str(CONFIG["spark"]["shuffle_partitions"]))
        .config("spark.hadoop.fs.s3a.endpoint", CONFIG["s3a"]["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", CONFIG["s3a"]["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", CONFIG["s3a"]["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access",
                str(CONFIG["s3a"]["path_style_access"]).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.buffer.dir", CONFIG["s3a"]["buffer_dir"])
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{catalog}.uri", CONFIG["nessie"]["uri"])
        .config(f"spark.sql.catalog.{catalog}.ref", CONFIG["nessie"]["ref"])
        .config(f"spark.sql.catalog.{catalog}.warehouse", CONFIG["nessie"]["warehouse"])
    )

    extra_classpath = CONFIG["spark"].get("extra_classpath")
    if extra_classpath:
        builder = builder.config("spark.driver.extraClassPath", extra_classpath)

    return builder.getOrCreate()
