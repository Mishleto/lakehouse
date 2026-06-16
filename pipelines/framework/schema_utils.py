"""
schema_utils.py
---------------
Utilities for handling schema evolution between incoming source data and
existing bronze Iceberg tables.

Public API
----------
align_schema(df, target_table, spark,
             mandatory_columns, on_missing_column, on_new_column)
-> tuple[DataFrame, bool]

Parameters
----------
mandatory_columns : list[str]
    Columns that MUST exist in the incoming source data.
    If any are missing the function raises immediately — no writing occurs.
    Pass an empty list to skip mandatory column validation entirely.

on_missing_column : "fill_null" | "fail"
    What to do when the existing bronze table has a column that is absent
    from the incoming source data (column dropped from source).
    "fill_null" — add the column as NULL (permissive, default).
    "fail"      — raise immediately with a clear message.

on_new_column : "add" | "fail"
    What to do when the incoming source data has a column that does not
    yet exist in the bronze table (new column in source).
    "add"  — allow iceberg_writer to add it via mergeSchema=True (default).
    "fail" — raise immediately with a clear message before any write.

Returns
-------
Tuple of (aligned_df, merge_schema) where:
    aligned_df   : DataFrame ready to write (may have NULL columns added)
    merge_schema : bool to pass to iceberg_writer.append_or_create()
                   True when on_new_column="add", False when "fail"

Design notes
------------
- If the target table does not yet exist (first run), mandatory_columns
  validation still runs against the incoming DataFrame columns.
  on_missing_column and on_new_column checks are skipped since there is
  no existing schema to compare against.
- All three checks produce clear, actionable error messages that name the
  specific columns involved.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_VALID_ON_MISSING = {"fill_null", "fail"}
_VALID_ON_NEW     = {"add", "fail"}


def align_schema(
    df: DataFrame,
    target_table: str,
    spark: SparkSession,
    mandatory_columns: list[str] | None = None,
    on_missing_column: str = "fill_null",
    on_new_column: str = "add",
) -> tuple[DataFrame, bool]:
    """
    Validate and align incoming DataFrame to the existing bronze table schema.

    Parameters
    ----------
    df                : incoming enriched DataFrame
    target_table      : fully-qualified Iceberg table, e.g. nessie.bronze.ss1_companies
    spark             : active SparkSession
    mandatory_columns : columns that must exist in df; raises if any are missing
    on_missing_column : "fill_null" or "fail" — behaviour when bronze table has
                        columns not present in df
    on_new_column     : "add" or "fail" — behaviour when df has columns not
                        present in the bronze table

    Returns
    -------
    (aligned_df, merge_schema)
        aligned_df   : DataFrame ready to write
        merge_schema : bool to pass to iceberg_writer.append_or_create()
    """
    if on_missing_column not in _VALID_ON_MISSING:
        raise ValueError(
            f"on_missing_column must be one of {_VALID_ON_MISSING}, "
            f"got {on_missing_column!r}"
        )
    if on_new_column not in _VALID_ON_NEW:
        raise ValueError(
            f"on_new_column must be one of {_VALID_ON_NEW}, "
            f"got {on_new_column!r}"
        )

    mandatory_columns = mandatory_columns or []
    incoming_columns  = set(df.columns)

    # --- Mandatory column check (always runs, even on first run) --------------
    if mandatory_columns:
        missing_mandatory = [c for c in mandatory_columns if c not in incoming_columns]
        if missing_mandatory:
            raise ValueError(
                f"[schema] Mandatory column(s) missing from source data: "
                f"{missing_mandatory}"
            )
        print(f"[schema] Mandatory columns present: {mandatory_columns}")

    # --- First run: table does not exist yet ----------------------------------
    # on_missing_column and on_new_column checks require an existing schema
    # to compare against. Skip them — table will be created from df's schema.
    if not spark.catalog.tableExists(target_table):
        merge_schema = (on_new_column == "add")
        return df, merge_schema

    # --- Build existing schema map --------------------------------------------
    existing_fields = {
        field.name: field.dataType
        for field in spark.table(target_table).schema.fields
    }
    existing_columns = set(existing_fields)

    # --- New columns: in source but not in bronze table -----------------------
    new_columns = incoming_columns - existing_columns
    if new_columns:
        if on_new_column == "fail":
            raise ValueError(
                f"[schema] New column(s) found in source that do not exist in "
                f"{target_table}: {sorted(new_columns)}. "
                f"Set on_new_column='add' to allow automatic schema evolution."
            )
        else:
            print(
                f"[schema] {len(new_columns)} new column(s) will be added to "
                f"{target_table}: {sorted(new_columns)}"
            )

    # --- Missing columns: in bronze table but not in source -------------------
    missing_columns = {
        name: dtype
        for name, dtype in existing_fields.items()
        if name not in incoming_columns
    }
    if missing_columns:
        if on_missing_column == "fail":
            raise ValueError(
                f"[schema] Column(s) present in {target_table} but missing from "
                f"source data: {sorted(missing_columns)}. "
                f"Set on_missing_column='fill_null' to fill them with NULL."
            )
        else:
            print(
                f"[schema] {len(missing_columns)} column(s) missing from source — "
                f"filling with NULL: {sorted(missing_columns)}"
            )
            for col_name, col_type in missing_columns.items():
                df = df.withColumn(col_name, F.lit(None).cast(col_type))

    merge_schema = (on_new_column == "add")
    return df, merge_schema