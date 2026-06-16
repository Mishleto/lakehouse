"""
iceberg_writer.py
-----------------
Stateless utility for writing a Spark DataFrame to an Iceberg table via
the Nessie catalog.

Public API
----------
append_or_create(df, target_table, merge_schema)

Design notes
------------
- Always ensures the target namespace exists before writing.
- Uses the createOrAppend workaround required by PySpark 4.1.1:
    tableExists -> append()  /  writeTo(...).tableProperty(...).create()
  (createOrAppend() is not available in PySpark 4.1.1)
- All Iceberg tables are created as format-version 2.
- merge_schema=True (default) instructs Iceberg to automatically add new
  columns found in the incoming DataFrame that do not yet exist in the
  table schema. Existing rows receive NULL for those new columns.
  Safe type promotions (INT->LONG, FLOAT->DOUBLE) are also handled.
  Unsafe type changes (e.g. STRING->INT) will still raise — this is
  intentional since silent data corruption is worse than a failed pipeline.
- No session ownership: the caller creates and stops SparkSession.
"""

from pyspark.sql import DataFrame, SparkSession

from pipelines.framework.sql_sanitizer import validate_object_identifier


def append_or_create(
    df: DataFrame,
    target_table: str,
    merge_schema: bool = True,
) -> None:
    """
    Append df to target_table. Creates the table (format-version 2) if it
    does not exist. Ensures namespace exists first.

    Parameters
    ----------
    df           : enriched DataFrame ready to write
    target_table : fully-qualified Iceberg table name,
                   e.g. nessie.bronze.ss1_companies
    merge_schema : if True (default), instruct Iceberg to add new columns
                   from df that are not yet in the table schema.
                   Has no effect on first write (table is created from df).
    """
    name_parts = target_table.split('.')
    if len(name_parts) < 3:
        raise ValueError(f"Invalid fully qualified table name: {target_table!r}")

    namespace = '.'.join(name_parts[:-1])

    validate_object_identifier(namespace)
    validate_object_identifier(target_table)

    spark: SparkSession = df.sparkSession

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    if spark.catalog.tableExists(target_table):
        if merge_schema:
            # Get new columns by comparing schemas
            existing_cols = {f.name for f in spark.table(target_table).schema.fields}
            new_cols = [(f.name, f.dataType.simpleString()) 
                        for f in df.schema.fields 
                        if f.name not in existing_cols]
            for col_name, col_type in new_cols:
                print(f"[schema] ALTER TABLE adding column: {col_name} {col_type}")
                spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col_name} {col_type}")
        
        df.writeTo(target_table).append()
    else:
        (
            df.writeTo(target_table)
            .tableProperty("format-version", "2")
            .create()
        )
