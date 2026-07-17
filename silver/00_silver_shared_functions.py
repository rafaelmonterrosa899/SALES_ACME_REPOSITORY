# =============================================================================
# NOTEBOOK: 00_silver_shared_functions
# Consolidated from Fabric notebook (.ipynb) into a single .py file for
# version control in GitHub. Cell boundaries are preserved as comment markers
# so the notebook structure remains traceable in source control.
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# <h1 style="color:#C00000; font-weight:bold; font-family:'EB Garamond', Garamond, serif; letter-spacing:1.5px; border-bottom: 2px solid #C00000; padding-bottom:6px;">00_silver_shared_functions</h1>

# =============================================================================
# CODE CELL 1 (notebook cell index 1)
# =============================================================================
# =============================================================================
# CELL 0 — Header & shared functions overview
# =============================================================================
# Purpose : Shared, reusable functions for the ACME Sales Silver layer.
#           Invoked via %run from 03a_silver_dimensions and 03b_silver_facts.
#
# Contents:
#   1. Imports (inherited pattern from Bronze shared functions)
#   2. read_bronze()            — generic reader from a Bronze Delta table
#   3. write_silver_merge()     — MERGE/UPSERT into Silver with hash-based
#                                  change detection (same pattern as Bronze)
#   4. validate_silver()        — post-write checks
#   5. add_unknown_member()     — generic "placeholder row" helper, used for
#                                  every case where a business decision is
#                                  ambiguous or a value is missing and CANNOT
#                                  be safely inferred (Orders.employee_id nulls,
#                                  orphan ShipperID 4/5, duplicate DivisionID)
#
# Design principle carried over from Bronze: never invent a business value.
# Where data is missing or ambiguous, add an explicit, clearly-labeled
# placeholder row instead of guessing — this keeps every fact row joinable
# in Gold without silently fabricating information or silently dropping
# real transactions via an INNER JOIN to an incomplete dimension.
#
# Author  : Rafael
# Date    : 2026-07-15
# =============================================================================

# =============================================================================
# CODE CELL 2 (notebook cell index 2)
# =============================================================================
# =============================================================================
# CELL 1 — Imports
# =============================================================================
# Same import pattern as 00_bronze_shared_functions. This notebook is
# independent (does NOT %run the Bronze shared functions) — Silver reads
# from Bronze Delta TABLES, not from raw files, so it doesn't need
# read_source(), the pandas-based reader, or mssparkutils file discovery.

import re
import uuid
import traceback
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    current_timestamp, lit, col, count, when, sha2, concat_ws, coalesce
)
from pyspark.sql.types import (
    StringType, StructType, StructField, TimestampType, LongType
)
from delta.tables import DeltaTable

print("Silver shared imports loaded successfully.")

# =============================================================================
# CODE CELL 3 (notebook cell index 3)
# =============================================================================
# =============================================================================
# CELL 2 — read_bronze()
# =============================================================================
# Purpose : Generic reader for any Bronze Delta table. Silver never reads
#           raw files directly — only Bronze tables, preserving the
#           one-directional flow (Bronze -> Silver -> Gold).
#
# Lineage design: only row_hash is dropped (it's a Bronze-specific mechanism
# for change detection against raw files — meaningless once we're
# transforming). created_at/updated_at/source_file_name/source_system are
# KEPT, but renamed with a bronze_ prefix, so lineage survives all the way
# through Silver into Gold (e.g. "which file did this row originate from")
# without colliding with Silver's own created_at/updated_at columns.
# =============================================================================

def read_bronze(table_name: str):
    """
    Read a Bronze Delta table, preserving lineage columns (renamed with a
    bronze_ prefix) and dropping only the Bronze-specific row_hash.

    Parameters:
        table_name : schema-qualified Bronze table, e.g. "dbo.bronze_orders"

    Returns:
        Spark DataFrame with bronze_created_at, bronze_updated_at,
        bronze_source_file_name, bronze_source_system in place of the
        original audit column names, and row_hash dropped.
    """
    df = spark.table(table_name)

    rename_map = {
        "created_at":       "bronze_created_at",
        "updated_at":       "bronze_updated_at",
        "source_file_name": "bronze_source_file_name",
        "source_system":    "bronze_source_system",
    }
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    if "row_hash" in df.columns:
        df = df.drop("row_hash")

    print(f"Read {table_name}: {df.count():,} rows, {len(df.columns)} columns")
    return df

# =============================================================================
# CODE CELL 4 (notebook cell index 4)
# =============================================================================
# =============================================================================
# CELL 3 — add_unknown_member()
# =============================================================================
# Purpose : Append one or more explicit "placeholder" rows to a dimension
#           DataFrame, for cases where a fact references a key that either
#           doesn't exist in the dimension (orphan FK, e.g. ShipperID 4/5)
#           or is ambiguous (e.g. a duplicate key needing business review).
#
# Design principle: NEVER invent business detail. The caller supplies only
# the columns we actually know (typically the key + a descriptive label
# making the placeholder's status obvious) — every other column is filled
# with NULL, honestly representing "we don't know this," rather than
# guessing at an address, phone number, or other attribute.
# =============================================================================

def add_unknown_member(df_dimension, placeholder_rows: list):
    """
    Append placeholder rows to a dimension DataFrame.

    Parameters:
        df_dimension     : the Silver dimension DataFrame to extend
        placeholder_rows : list of dicts, each mapping column_name -> value
                           for the columns we actually know. Any column in
                           df_dimension's schema NOT present in a given
                           dict is set to NULL for that row.

    Returns:
        df_dimension with the placeholder row(s) appended.

    Example:
        df_shippers = add_unknown_member(df_shippers, [
            {"shipper_id": 4, "company_name": "Unknown Shipper — Pending Confirmation"},
            {"shipper_id": 5, "company_name": "Unknown Shipper — Pending Confirmation"},
        ])
    """
    schema = df_dimension.schema
    rows = []
    for placeholder in placeholder_rows:
        row_values = {field.name: placeholder.get(field.name, None) for field in schema.fields}
        rows.append(row_values)

    df_placeholder = spark.createDataFrame(rows, schema=schema)

    print(f"Adding {len(placeholder_rows)} placeholder row(s):")
    display(df_placeholder)

    return df_dimension.unionByName(df_placeholder)

# =============================================================================
# CODE CELL 5 (notebook cell index 5)
# =============================================================================
# =============================================================================
# CELL 4 — write_silver_merge()
# =============================================================================
# Purpose : Write a transformed DataFrame to a Silver Delta table using
#           MERGE/UPSERT with SHA-256 row_hash change detection — same
#           pattern as Bronze's write_bronze_merge(), but Silver generates
#           its OWN created_at/updated_at (distinct from bronze_created_at/
#           bronze_updated_at, which came through via read_bronze()).
# =============================================================================

def write_silver_merge(df_transformed, target_table: str, merge_key):
    """
    Write a transformed DataFrame to Silver using MERGE/UPSERT with a
    SHA-256 row_hash for change detection. Adds created_at/updated_at
    (Silver's own audit columns) before writing.

    Parameters:
        df_transformed : DataFrame with Silver-level business columns
                         (plus any bronze_* lineage columns passed through)
        target_table   : schema-qualified table name, e.g. "silver.dim_customer"
        merge_key      : string or list of strings (composite key)

    Returns:
        dict with rows_inserted, rows_updated, rows_total_after_write
    """
    key_cols = [merge_key] if isinstance(merge_key, str) else list(merge_key)

    if "created_at" not in df_transformed.columns:
        df_transformed = df_transformed.withColumn("created_at", F.current_timestamp())
    if "updated_at" not in df_transformed.columns:
        df_transformed = df_transformed.withColumn("updated_at", F.current_timestamp())

    excluded_from_hash = set(key_cols) | {"created_at", "updated_at", "row_hash"}
    hash_cols = [c for c in df_transformed.columns if c not in excluded_from_hash]

    print(f"Merge key(s) : {key_cols}")
    print(f"Columns included in row_hash ({len(hash_cols)}): {hash_cols}")

    df_transformed = df_transformed.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in hash_cols]), 256)
    )

    table_exists = spark.catalog.tableExists(target_table)

    print(f"{'='*60}")
    print(f"  MERGE / UPSERT — {target_table}")
    print(f"{'='*60}")
    print(f"  Target table : {target_table}")
    print(f"  Table exists : {table_exists}")
    print(f"{'='*60}\n")

    if not table_exists:
        print("── First run detected — performing full INSERT ─────────────")
        df_transformed.write.format("delta").mode("overwrite").saveAsTable(target_table)

        rows_total = spark.table(target_table).count()
        print(f"  Table created : {target_table}")
        print(f"  Rows inserted : {rows_total:,}")

        return {
            "rows_inserted": rows_total,
            "rows_updated": 0,
            "rows_total_after_write": rows_total
        }

    else:
        print("── Existing table detected — performing MERGE ──────────────")

        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        target_columns = [f.name for f in spark.table(target_table).schema.fields]
        if "row_hash" not in target_columns:
            print(f"  row_hash column missing on {target_table} — adding it now")
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (row_hash STRING)")
            spark.catalog.refreshTable(target_table)

        delta_table = DeltaTable.forName(spark, target_table)

        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in key_cols]
        )

        update_cols = {
            c: f"source.{c}" for c in df_transformed.columns
            if c not in key_cols and c != "created_at"
        }

        delta_table.alias("target").merge(
            df_transformed.alias("source"),
            merge_condition
        ) \
        .whenMatchedUpdate(
            condition="target.row_hash != source.row_hash",
            set=update_cols
        ) \
        .whenNotMatchedInsertAll() \
        .execute()

        history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()[0]
        metrics = history["operationMetrics"]
        rows_inserted = int(metrics.get("numTargetRowsInserted", 0))
        rows_updated  = int(metrics.get("numTargetRowsUpdated", 0))
        rows_total    = spark.table(target_table).count()

        print(f"  MERGE complete")
        print(f"  Rows inserted : {rows_inserted:,}")
        print(f"  Rows updated  : {rows_updated:,}")
        print(f"  Total rows in Silver table : {rows_total:,}")

        return {
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_total_after_write": rows_total
        }

# =============================================================================
# CODE CELL 6 (notebook cell index 6)
# =============================================================================
# =============================================================================
# CELL 5 — validate_silver()
# =============================================================================
# Purpose : Post-write validation checks for a Silver Delta table.
#           Same structure as Bronze's validate_bronze(), adapted for
#           Silver's own created_at/updated_at audit columns.
# =============================================================================

def validate_silver(target_table: str, key_column):
    """
    Run post-write validation checks on a Silver Delta table.

    Parameters:
        target_table : schema-qualified table name, e.g. "silver.dim_customer"
        key_column    : string or list of strings (composite key)

    Returns:
        dict with total_rows, null_key_count, duplicate_key_count
    """
    key_cols = [key_column] if isinstance(key_column, str) else list(key_column)

    df_silver = spark.table(target_table)

    total_rows = df_silver.count()

    null_condition = None
    for k in key_cols:
        cond = F.col(k).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)
    null_key_count = df_silver.filter(null_condition).count()

    latest_update = df_silver.agg(F.max("updated_at")).collect()[0][0]
    earliest_creation = df_silver.agg(F.min("created_at")).collect()[0][0]

    duplicate_keys_df = df_silver.groupBy(*key_cols).count().filter("count > 1")
    duplicate_keys_count = duplicate_keys_df.count()

    print(f"{'='*60}")
    print(f"  POST-WRITE VALIDATION — {target_table}")
    print(f"{'='*60}")

    print(f"\n── Row count ───────────────────────────────────────────────")
    print(f"  Total rows in Silver table : {total_rows:,}")

    print(f"\n── Key integrity ({', '.join(key_cols)}) ─────────────────────")
    print(f"  Null key value(s) : {null_key_count:,}")
    print(f"  {'✅ No nulls on key' if null_key_count == 0 else '❌ NULLS FOUND — critical issue'}")

    print(f"\n── Duplicate check on key ({', '.join(key_cols)}) ──────────────")
    if duplicate_keys_count == 0:
        print(f"  ✅ No duplicate key values found in Silver table")
    else:
        print(f"  ⚠️  {duplicate_keys_count} duplicate key value(s) found — investigate")
        display(duplicate_keys_df.limit(10))

    print(f"\n── Audit timestamps ────────────────────────────────────────")
    print(f"  Earliest created_at : {earliest_creation}")
    print(f"  Latest updated_at   : {latest_update}")

    print(f"\n{'='*60}")
    print(f"  ✅ Validation complete — {target_table}")
    print(f"{'='*60}")

    return {
        "total_rows": total_rows,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_keys_count
    }

# =============================================================================
# CODE CELL 7 (notebook cell index 7)
# =============================================================================
# =============================================================================
# CELL 6 — write_audit_log()
# =============================================================================
# Purpose : Write a single row to governance.audit_ingestion_log describing
#           the outcome of processing one Silver source. Identical to the
#           Bronze version — both layers share the same governance table,
#           so the full pipeline's audit trail lives in one place.
# =============================================================================

AUDIT_TABLE = "governance.audit_ingestion_log"

def write_audit_log(run_id, table_name, source_file_name, run_timestamp,
                     rows_read, rows_inserted, rows_updated, rows_total,
                     status, error_message):

    audit_schema = StructType([
        StructField("run_id",                 StringType(),    False),
        StructField("table_name",              StringType(),    False),
        StructField("source_file_name",        StringType(),    True),
        StructField("run_timestamp",           TimestampType(), False),
        StructField("rows_read_from_source",   LongType(),      True),
        StructField("rows_inserted",           LongType(),      True),
        StructField("rows_updated",            LongType(),      True),
        StructField("rows_total_after_write",  LongType(),      True),
        StructField("status",                  StringType(),    False),
        StructField("error_message",           StringType(),    True),
    ])

    audit_row = [(
        run_id, table_name, source_file_name, run_timestamp,
        rows_read, rows_inserted, rows_updated, rows_total,
        status, error_message
    )]

    df_audit = spark.createDataFrame(audit_row, schema=audit_schema)

    df_audit.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)

    print(f"  📝 Audit log written : run_id={run_id} | table={table_name} | status={status}")
