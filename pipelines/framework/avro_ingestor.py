"""
avro_ingestor.py
----------------
Framework module for ingesting one or more Avro files into a single bronze
Iceberg table.

Public API
----------
run(source_system, source_dir, name_pattern, target_table, spark,
    log_table, mandatory_columns, on_missing_column, on_new_column,
    schema_mode, explicit_schema)

The caller provides a directory and a glob pattern such as 'events_*.avro'.
All matching files are resolved at runtime with pathlib.Path.glob().
Each file is tracked and guarded independently in file_ingestion_log so
that re-runs only pick up files that have not yet been successfully
processed.

Schema Modes
------------
"default" (SchemaMode.DEFAULT)
    Every Avro file is opened natively via spark.read.format("avro").
    Spark infers the schema from each file's embedded Avro schema.
    Safest option; slowest for large numbers of small files because each
    file is opened twice (schema inference + read).

"external" (SchemaMode.EXTERNAL)
    The caller supplies an explicit Avro schema string (JSON) via the
    explicit_schema parameter. Every file is read as binary (format="avro"
    with the provided schema applied), which avoids per-file schema
    negotiation and is the fastest mode.

"first_file" (SchemaMode.FIRST_FILE)
    The first matched file is opened natively to extract its embedded Avro
    schema. All subsequent files (including the first, in the per-file loop)
    are read as binary with that schema applied. Good when all files share
    the same schema but you do not want to hard-code it.

"target" (SchemaMode.TARGET)
    The schema is derived from the existing target Iceberg table. Each file
    is read as binary with the target schema applied. Useful when the table
    schema is authoritative and source files must conform to it. Raises
    RuntimeError if the target table does not yet exist.

Sequence (Template Method pattern)
-----------------------------------
Schema resolution phase (before the per-file loop):
  0a. Discover     - list files matching source_dir / name_pattern, sorted;
                     warn and return if none found
  0b. Resolve schema
                   - DEFAULT   : no-op (schema resolved per file)
                   - EXTERNAL  : validate explicit_schema provided
                   - FIRST_FILE: open first file natively, extract schema
                   - TARGET    : read target table schema, convert to Avro JSON

Per-file loop:
  1. Skip guard    - skip file if already successfully processed
  2. Read          - read Avro file using resolved strategy (native or binary
                     with pre-resolved schema)
  3. Enrich        - add _source_system, _ingestion_timestamp, _source_file
  4. Align schema  - validate mandatory columns; handle missing/new columns
                     per on_missing_column / on_new_column policy
  5. Write         - append_or_create via iceberg_writer; merge_schema driven
                     by on_new_column policy returned from align_schema
  6. Log success   - write to file_ingestion_log
     On error      - log failure, continue remaining files

After loop:
  Raise RuntimeError listing all failed files if any failed.
  Caller owns spark.stop().
"""

import json
import traceback
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, functions as F

from pipelines.framework.iceberg_writer import append_or_create
from pipelines.framework.ingestion_logger import is_file_processed, log_file_run
from pipelines.framework.schema_utils import align_schema


class SchemaMode(str, Enum):
    DEFAULT = "default"
    EXTERNAL = "external"
    FIRST_FILE = "first_file"
    TARGET = "target"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_avro_native(spark: SparkSession, source_file: str) -> DataFrame:
    """Read a single Avro file using its embedded schema (native mode)."""
    return spark.read.format("avro").load(source_file)


def _read_avro_with_schema(
    spark: SparkSession, source_file: str, avro_schema_str: str
) -> DataFrame:
    """
    Read a single Avro file as binary with an explicit Avro schema string
    applied. Faster than native mode for collections that share a schema
    because Spark skips per-file schema negotiation.
    """
    return (
        spark.read
        .format("avro")
        .option("avroSchema", avro_schema_str)
        .load(source_file)
    )


def _extract_schema_from_file(spark: SparkSession, source_file: str) -> str:
    """
    Open one Avro file natively, extract its Spark schema, and convert it to
    an Avro schema JSON string suitable for use with the 'avroSchema' option.

    Raises
    ------
    RuntimeError if the file cannot be read or the schema cannot be serialised.
    """
    try:
        df = _read_avro_native(spark, source_file)
        # from_avro / to_avro helpers are not available in all distributions;
        # use SchemaConverters from spark-avro instead.
        from pyspark.sql.avro.functions import to_avro  # noqa: F401  (import check)
        from sparkavro import SchemaConverters  # type: ignore
        avro_schema = SchemaConverters.toAvroType(df.schema)
        return json.dumps(avro_schema)
    except ImportError:
        # Fallback: rely on the Java-side schema stored in the file header via
        # Hadoop InputFormat — read the schema from the file directly.
        sc = spark.sparkContext
        hadoop_conf = sc._jvm.org.apache.hadoop.conf.Configuration()
        path = sc._jvm.org.apache.hadoop.fs.Path(source_file)
        data_file_reader = (
            sc._jvm.org.apache.avro.file.DataFileReader
            .openReader(
                sc._jvm.org.apache.avro.mapred.FsInput(path, hadoop_conf),
                sc._jvm.org.apache.avro.generic.GenericDatumReader(),
            )
        )
        schema_str = str(data_file_reader.getSchema())
        data_file_reader.close()
        return schema_str


def _extract_schema_from_target(spark: SparkSession, target_table: str) -> str:
    """
    Read the existing target Iceberg table schema and convert it to an Avro
    schema JSON string.

    Raises
    ------
    RuntimeError if the target table does not exist.
    """
    try:
        target_df = spark.read.format("iceberg").load(target_table)
    except Exception as e:
        raise RuntimeError(
            f"avro_ingestor [target mode]: could not read target table "
            f"'{target_table}'. Ensure it exists before using schema_mode='target'.\n"
            f"Original error: {e}"
        ) from e

    # Strip metadata columns — they are added by the enrichment step, not
    # present in the source Avro files.
    metadata_cols = {"_source_system", "_ingestion_timestamp", "_source_file"}
    source_schema = target_df.drop(*metadata_cols).schema

    try:
        from sparkavro import SchemaConverters  # type: ignore
        avro_schema = SchemaConverters.toAvroType(source_schema)
        return json.dumps(avro_schema)
    except ImportError:
        # Build a minimal Avro record schema from the Spark schema manually.
        # This covers primitive types that are common in bronze tables.
        _SPARK_TO_AVRO = {
            "StringType": "string",
            "IntegerType": "int",
            "LongType": "long",
            "FloatType": "float",
            "DoubleType": "double",
            "BooleanType": "boolean",
            "BinaryType": "bytes",
            "TimestampType": {"type": "long", "logicalType": "timestamp-micros"},
            "DateType": {"type": "int", "logicalType": "date"},
        }
        fields = []
        for field in source_schema.fields:
            type_name = type(field.dataType).__name__
            avro_type = _SPARK_TO_AVRO.get(type_name, "string")
            # All fields nullable by default (union with null)
            fields.append({
                "name": field.name,
                "type": ["null", avro_type] if field.nullable else avro_type,
                "default": None if field.nullable else "__required__",
            })
        avro_schema = {
            "type": "record",
            "name": target_table.replace(".", "_"),
            "fields": fields,
        }
        return json.dumps(avro_schema)


# ---------------------------------------------------------------------------
# Per-file worker
# ---------------------------------------------------------------------------

def _ingest_file(
    spark: SparkSession,
    source_system: str,
    source_file: str,
    target_table: str,
    log_table: str,
    mandatory_columns: list[str],
    on_missing_column: str,
    on_new_column: str,
    schema_mode: SchemaMode,
    resolved_schema: Optional[str],
) -> bool:
    """
    Process one Avro file. Returns True on success (or skip), False on failure.
    Internal helper — not part of the public API.

    Parameters
    ----------
    resolved_schema : Avro schema JSON string; only used when schema_mode is
                      not DEFAULT. May be None for DEFAULT mode.
    """
    print(f"\n[file] {source_file} -> {target_table}  (schema_mode={schema_mode})")

    # --- 1. Skip guard --------------------------------------------------------
    if is_file_processed(spark, source_file, log_table):
        print("[skip] Already successfully processed. Skipping.")
        return True

    try:
        # --- 2. Read ----------------------------------------------------------
        print(f"[read] Reading {source_file} ...")

        if schema_mode == SchemaMode.DEFAULT:
            raw_df = _read_avro_native(spark, source_file)
        else:
            # EXTERNAL / FIRST_FILE / TARGET all arrive here with a
            # pre-resolved schema string.
            raw_df = _read_avro_with_schema(spark, source_file, resolved_schema)

        row_count = raw_df.count()
        print(f"[read] {row_count} rows, {len(raw_df.columns)} columns.")

        # --- 3. Enrich --------------------------------------------------------
        ingestion_ts = datetime.now(timezone.utc)
        enriched_df = (
            raw_df
            .withColumn("_source_system",      F.lit(source_system))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
            .withColumn("_source_file",         F.lit(source_file))
        )

        # --- 4. Align schema --------------------------------------------------
        aligned_df, merge_schema = align_schema(
            enriched_df, target_table, spark,
            mandatory_columns=mandatory_columns,
            on_missing_column=on_missing_column,
            on_new_column=on_new_column,
        )

        # --- 5. Write ---------------------------------------------------------
        print(f"[write] Appending to {target_table} ...")
        append_or_create(aligned_df, target_table, merge_schema=merge_schema)
        print("[write] Done.")

        # --- 6. Log success ---------------------------------------------------
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
    on_missing_column: str = "fill_null",
    on_new_column: str = "add",
    schema_mode: str | SchemaMode = SchemaMode.DEFAULT,
    explicit_schema: Optional[str] = None,
) -> None:
    """
    Discover and ingest all Avro files matching name_pattern in source_dir
    into a single target_table.

    Parameters
    ----------
    source_system     : e.g. 'ss4'
    source_dir        : absolute path to the folder containing the Avro files
    name_pattern      : glob expression, e.g. 'events_*.avro'
    target_table      : fully-qualified Iceberg table, e.g. nessie.bronze.ss4_events
    spark             : active SparkSession (caller creates and stops it)
    log_table         : ingestion log table
    mandatory_columns : columns that must exist in source; fails if any missing.
                        Pass None or [] to skip validation.
    on_missing_column : "fill_null" (default) — fill columns present in the
                        bronze table but absent from the source with NULL.
                        "fail" — raise if any such columns are found.
    on_new_column     : "add" (default) — automatically add new source columns
                        to the bronze table via mergeSchema.
                        "fail" — raise if the source has columns not in bronze.
    schema_mode       : one of "default", "external", "first_file", "target"
                        (or the SchemaMode enum). Controls how the Avro schema
                        is resolved before reading files. See module docstring
                        for a full description of each mode.
    explicit_schema   : Avro schema JSON string. Required when
                        schema_mode="external"; ignored otherwise.

    Raises
    ------
    ValueError      if schema_mode is invalid or explicit_schema is missing
                    when required.
    RuntimeError    if schema_mode="target" and the target table does not exist,
                    or if any files failed after the full run.
    """
    schema_mode = SchemaMode(schema_mode)

    # --- 0a. Discover ---------------------------------------------------------
    matched = sorted(Path(source_dir).glob(name_pattern))

    if not matched:
        print(
            f"[warn] No files matched '{name_pattern}' in '{source_dir}'. "
            "Nothing to ingest."
        )
        return

    print(
        f"[glob] Found {len(matched)} file(s) matching '{name_pattern}' "
        f"in '{source_dir}'. schema_mode={schema_mode}."
    )

    mandatory_columns = mandatory_columns or []

    # --- 0b. Resolve schema (once, before the per-file loop) ------------------
    resolved_schema: Optional[str] = None

    if schema_mode == SchemaMode.DEFAULT:
        print("[schema] DEFAULT — schema will be read from each file's embedded header.")

    elif schema_mode == SchemaMode.EXTERNAL:
        if not explicit_schema:
            raise ValueError(
                "avro_ingestor: schema_mode='external' requires explicit_schema "
                "to be a non-empty Avro schema JSON string."
            )
        resolved_schema = explicit_schema
        print("[schema] EXTERNAL — using caller-supplied schema.")

    elif schema_mode == SchemaMode.FIRST_FILE:
        first_file = matched[0].as_posix()
        print(f"[schema] FIRST_FILE — extracting schema from '{first_file}' ...")
        resolved_schema = _extract_schema_from_file(spark, first_file)
        print("[schema] Schema extracted. All files will be read as binary with this schema.")

    elif schema_mode == SchemaMode.TARGET:
        print(f"[schema] TARGET — deriving schema from target table '{target_table}' ...")
        resolved_schema = _extract_schema_from_target(spark, target_table)
        print("[schema] Schema derived. All files will be read as binary with target schema.")

    # --- Per-file loop --------------------------------------------------------
    failed_files: list[str] = []

    for path in matched:
        source_file = path.as_posix()
        success = _ingest_file(
            spark, source_system, source_file,
            target_table, log_table,
            mandatory_columns, on_missing_column, on_new_column,
            schema_mode, resolved_schema,
        )
        if not success:
            failed_files.append(source_file)

    # --- Final gate -----------------------------------------------------------
    if failed_files:
        raise RuntimeError(
            f"avro_ingestor: finished with failures in files: {failed_files}"
        )

    print(f"\n[done] All matched files processed into {target_table}.")
