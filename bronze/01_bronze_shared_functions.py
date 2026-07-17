# =============================================================================
# NOTEBOOK: 01_bronze_shared_functions
# Consolidated from Fabric notebook (.ipynb) into a single .py file for
# version control in GitHub. Cell boundaries are preserved as comment markers
# so the notebook structure remains traceable in source control.
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# <h1 style="color:#C00000; font-weight:bold; font-family:'EB Garamond', Garamond, serif; letter-spacing:1.5px; border-bottom: 2px solid #C00000; padding-bottom:6px;">00_bronze_shared_functions</h1>

# =============================================================================
# CODE CELL 1 (notebook cell index 1)
# =============================================================================
# =============================================================================
# CELL 0 — Header & shared functions overview
# =============================================================================
# Purpose : Shared, reusable functions for the ACME Sales Bronze layer.
#           This notebook is invoked via %run from both:
#             - 02a_bronze_dimensions  (overwrite strategy)
#             - 02b_bronze_facts       (merge/upsert strategy, hash-based)
#
# Contents:
#   1. Imports
#   2. to_snake_case() / normalize_schema()
#   3. run_eda()
#   4. write_bronze_overwrite()   — for dimension tables
#   5. write_bronze_merge()       — for fact tables (MERGE + row_hash)
#   6. validate_bronze()
#   7. write_audit_log()
#
# Design notes:
#   - No source files are ever deleted here (unlike Oracle Census) — ACME
#     source files remain in Files/bronze_sources/ as a permanent audit trail,
#     since this is a POC with a fixed, one-time file drop rather than a
#     recurring automated feed.
#   - Column NAMES are standardized to snake_case at Bronze; column VALUES
#     are never modified — consistent with the "no transformations on
#     source files" rule (the rule applies to the physical files, not to
#     the Delta table's schema representation).
#
# Author  : Rafael
# Updated : 2026-07-13
# =============================================================================

# =============================================================================
# CODE CELL 2 (notebook cell index 2)
# =============================================================================
# =============================================================================
# CELL 1 — Imports
# =============================================================================
# All imports needed by the shared functions below. Both child notebooks
# (02a_bronze_dimensions, 02b_bronze_facts) inherit these via %run, so
# neither needs to re-declare imports.

import re
import uuid
import traceback
from datetime import datetime, timezone
import pandas as pd
from notebookutils import mssparkutils

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    current_timestamp, lit, col, count, when, sha2, concat_ws, coalesce
)
from pyspark.sql.types import (
    StringType, StructType, StructField, TimestampType, LongType
)
from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    current_timestamp, lit, col, count, when, sha2, concat_ws, coalesce
)
from pyspark.sql.types import (
    StringType, StructType, StructField, TimestampType, LongType
)
from delta.tables import DeltaTable

print("Shared imports loaded successfully.")

# =============================================================================
# CODE CELL 3 (notebook cell index 3)
# =============================================================================
# =============================================================================
# CELL 2 — to_snake_case() and normalize_schema()
# =============================================================================
# Purpose : Normalize all column names to snake_case and append audit columns.
#           No data VALUES are modified — structural transformation only.
#           Applies to both dimension and fact sources.
# =============================================================================

def to_snake_case(col_name: str) -> str:
    """Convert a column name to snake_case format.

    Handles two distinct source patterns:
      - Space/hyphen-separated names (e.g. Oracle Census: "Employee ID")
      - PascalCase/camelCase names with no separators (e.g. ACME: "CategoryID")

    Steps:
        1. Strip leading/trailing whitespace
        2. Insert underscore at camelCase/PascalCase transitions
           (e.g. "CategoryID" -> "Category_ID")
        3. Replace spaces, hyphens, colons, slashes with underscores
        4. Remove remaining special characters (keep alphanumeric/underscore)
        5. Collapse multiple underscores into one
        6. Lowercase the result
    """
    col_name = col_name.strip()
    col_name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', col_name)
    col_name = re.sub(r'[\s\-\:\\/]+', '_', col_name)
    col_name = re.sub(r'[^\w]', '', col_name)
    col_name = re.sub(r'_+', '_', col_name)
    return col_name.lower()


def normalize_schema(df_raw, source_file_name: str, source_system: str):
    """
    Rename all columns to snake_case and append audit columns.

    Parameters:
        df_raw            : raw Spark DataFrame, as read from the source file
        source_file_name  : e.g. "Categories.csv" — for traceability
        source_system     : fixed value identifying the origin repository,
                             e.g. "ACME_FILE_REPO"

    Returns:
        df_normalized : DataFrame with snake_case columns + 3 audit columns
    """
    df_normalized = df_raw
    original_cols = df_raw.columns

    for original_col in original_cols:
        new_col = to_snake_case(original_col)
        if original_col != new_col:
            df_normalized = df_normalized.withColumnRenamed(original_col, new_col)

    df_normalized = df_normalized \
        .withColumn("created_at",         F.current_timestamp()) \
        .withColumn("updated_at",         F.current_timestamp()) \
        .withColumn("source_file_name",   F.lit(source_file_name)) \
        .withColumn("source_system",      F.lit(source_system))

    print(f"{'='*65}")
    print(f"  SCHEMA NORMALIZATION — Column Mapping ({source_file_name})")
    print(f"{'='*65}")
    print(f"  {'Original':<45} {'Normalized'}")
    print(f"  {'-'*45} {'-'*25}")
    for original_col, new_col in zip(original_cols, df_normalized.columns[:-3]):
        changed = " ✏️" if original_col != new_col else ""
        print(f"  {original_col:<45} {new_col}{changed}")

    print(f"\n── Summary ─────────────────────────────────────────────────")
    print(f"  Columns before normalization : {len(original_cols)}")
    print(f"  Audit columns added          : 3")
    print(f"  Columns after normalization  : {len(df_normalized.columns)}")
    print(f"  Rows                         : {df_normalized.count():,}")
    print(f"{'='*65}")

    return df_normalized

# =============================================================================
# CODE CELL 4 (notebook cell index 4)
# =============================================================================
# =============================================================================
# CELL 3 — run_eda()
# =============================================================================
# Purpose : Profile a raw source DataFrame before any transformation.
#           Identify key column candidates, null rates, duplicates, schema.
#           NO writes occur in this function — observe before acting.
#
# Difference vs. Oracle Census version: key candidates are discovered by a
# configurable list of keywords instead of a single hardcoded "employee id",
# since this function runs against 13 different sources with different
# natural key patterns.
# =============================================================================

def run_eda(df_raw, source_name: str, key_keywords: list = None):
    """
    Profile a raw DataFrame: shape, sample rows, null rates, duplicate check
    on key column candidates.

    Parameters:
        df_raw       : raw Spark DataFrame, as read from the source file
        source_name  : label used in printed output, e.g. "Categories"
        key_keywords : list of substrings used to detect key column
                       candidates (default covers common id patterns)
    """
    if key_keywords is None:
        key_keywords = ["id", "code", "number", "key"]

    row_count = df_raw.count()
    col_count = len(df_raw.columns)

    print(f"{'='*60}")
    print(f"  {source_name.upper()} — EDA PROFILE")
    print(f"{'='*60}")
    print(f"  Rows    : {row_count:,}")
    print(f"  Columns : {col_count}")
    print(f"{'='*60}\n")

    print("── Sample rows (first 5) ──────────────────────────────────")
    # display() renders an interactive table (sortable, filterable,
    # expandable to full screen) instead of plain text — much easier to
    # review than .show() output, especially for wide DataFrames.
    display(df_raw.limit(5))

    print("\n── Column names ───────────────────────────────────────────")
    for i, column_name in enumerate(df_raw.columns):
        print(f"  [{i:02d}] {column_name}")

    print("\n── Null counts per column (descending) ────────────────────")
    null_counts = df_raw.select([
        F.count(F.when(F.col(c).isNull() | (F.col(c) == ""), c)).alias(c)
        for c in df_raw.columns
    ])
    null_rows   = null_counts.collect()[0].asDict()
    null_sorted = sorted(null_rows.items(), key=lambda x: x[1], reverse=True)

    for col_name, null_val in null_sorted:
        pct  = (null_val / row_count) * 100 if row_count else 0
        flag = " ⚠️  100% NULL — review for Silver drop" if pct == 100 else ""
        print(f"  {col_name:<45} {null_val:>6,}  ({pct:5.1f}%){flag}")

    print("\n── Duplicate check on key column candidates ────────────────")
    candidates = [c for c in df_raw.columns if any(
        kw in c.lower() for kw in key_keywords
    )]

    if candidates:
        for candidate in candidates:
            total      = row_count
            distinct   = df_raw.select(candidate).distinct().count()
            duplicates = total - distinct
            print(f"  Column  : '{candidate}'")
            print(f"  Total   : {total:,}")
            print(f"  Distinct: {distinct:,}")
            print(f"  Dupes   : {duplicates:,}")
            print(f"  {'✅ Good key candidate' if duplicates == 0 else '⚠️  Duplicates found — expected for a fact/detail table, investigate if unexpected'}\n")
    else:
        print("  ⚠️  No obvious key column found by keyword scan.")
        print("  Review column list above and identify the key manually.")

    print(f"\n{'='*60}")
    print(f"  EDA complete — {source_name}")
    print(f"{'='*60}")

# =============================================================================
# CODE CELL 5 (notebook cell index 5)
# =============================================================================
# =============================================================================
# CELL 4 — write_bronze_overwrite()
# =============================================================================
# Purpose : Write a normalized DataFrame to a Bronze Delta table using a full
#           overwrite strategy. Used for DIMENSION sources only — each file
#           represents a complete extract, and there is no partial/delta
#           feed to reconcile, so overwrite is the correct and simplest
#           strategy (see design discussion: dimensions vs facts).
# =============================================================================

def write_bronze_overwrite(df_normalized, target_table: str):
    """
    Write a normalized DataFrame to Bronze using full overwrite.

    Parameters:
        df_normalized : DataFrame already processed by normalize_schema()
        target_table  : schema-qualified table name, e.g. "dbo.bronze_categories"

    Returns:
        dict with rows_inserted, rows_updated (always 0 for overwrite),
        rows_total_after_write — kept symmetric with write_bronze_merge()
        so both feed the audit log the same way.
    """
    print(f"{'='*60}")
    print(f"  BRONZE OVERWRITE — {target_table}")
    print(f"{'='*60}")

    (
        df_normalized.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")  # allow schema to evolve across runs
        .saveAsTable(target_table)
    )

    rows_total = spark.table(target_table).count()

    print(f"  ✅ Table written : {target_table}")
    print(f"  ✅ Rows total    : {rows_total:,}")
    print(f"{'='*60}")

    return {
        "rows_inserted": rows_total,
        "rows_updated": 0,
        "rows_total_after_write": rows_total
    }

# =============================================================================
# CODE CELL 6 (notebook cell index 6)
# =============================================================================
# =============================================================================
# CELL 5 — write_bronze_merge()
# =============================================================================
# Purpose : Incrementally load a normalized DataFrame into a Bronze Delta
#           table using MERGE/UPSERT with hash-based change detection.
#           Used for FACT sources (Orders, Order_Details, Shipments, Budget)
#           and, as of this version, DIMENSIONS as well — same pattern as
#           Oracle Census, generalized to accept any target table and
#           merge key(s).
#
# NOTE: intentionally does NOT deduplicate the incoming batch on merge_key.
# Bronze preserves the source as-is, including any duplicate rows a source
# file may legitimately contain. A duplicate found in Bronze is DETECTED
# and REPORTED via validate_bronze(), never silently removed here —
# deduplication is a Silver-layer decision.
#
# Difference vs. Oracle Census version: merge_key can be a LIST of columns
# (composite key). Also uses created_at/updated_at instead of a single
# ingestion_timestamp, so re-running the pipeline never resets the "when
# was this row first created" answer for unchanged rows (idempotent
# auditability, not just idempotent row counts).
# =============================================================================

def write_bronze_merge(df_normalized, target_table: str, merge_key):
    """
    Write a normalized DataFrame to Bronze using MERGE/UPSERT with a
    SHA-256 row_hash for change detection.

    Auditability: uses created_at (set once, on first insert, never
    touched again) and updated_at (refreshed only when row_hash changes).

    Parameters:
        df_normalized : DataFrame already processed by normalize_schema()
        target_table  : schema-qualified table name, e.g. "dbo.bronze_orders"
        merge_key     : string (single column) or list of strings
                        (composite key), e.g. "order_id" or
                        ["order_id", "product_id"]

    Returns:
        dict with rows_inserted, rows_updated, rows_total_after_write
    """
    key_cols = [merge_key] if isinstance(merge_key, str) else list(merge_key)

    excluded_from_hash = set(key_cols) | {"created_at", "updated_at", "row_hash"}
    hash_cols = [c for c in df_normalized.columns if c not in excluded_from_hash]

    print(f"Merge key(s) : {key_cols}")
    print(f"Columns included in row_hash ({len(hash_cols)}): {hash_cols}")

    df_normalized = df_normalized.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[coalesce(col(c), lit("")) for c in hash_cols]), 256)
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
        df_normalized.write.format("delta").mode("overwrite").saveAsTable(target_table)

        rows_total = spark.table(target_table).count()
        print(f"  ✅ Table created : {target_table}")
        print(f"  ✅ Rows inserted : {rows_total:,}")

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
            print(f"  ⚙️  row_hash column missing on {target_table} — adding it now (one-time migration)")
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (row_hash STRING)")
            spark.catalog.refreshTable(target_table)
            print(f"  ✅ row_hash column added — existing rows will have row_hash = NULL until next MERGE")

        delta_table = DeltaTable.forName(spark, target_table)

        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in key_cols]
        )

        # created_at is deliberately excluded here — on a matched row we
        # only refresh updated_at and the business columns. created_at
        # keeps whatever value it got when the row was first inserted.
        update_cols = {
            c: f"source.{c}" for c in df_normalized.columns
            if c not in key_cols and c != "created_at"
        }

        delta_table.alias("target").merge(
            df_normalized.alias("source"),
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

        print(f"  ✅ MERGE complete")
        print(f"  ✅ Rows inserted : {rows_inserted:,}")
        print(f"  ✅ Rows updated  : {rows_updated:,}")
        print(f"  ✅ Total rows in Bronze table : {rows_total:,}")

        return {
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_total_after_write": rows_total
        }

# =============================================================================
# CODE CELL 7 (notebook cell index 7)
# =============================================================================
# =============================================================================
# CELL 6 — validate_bronze()
# =============================================================================
# Purpose : Confirm a Bronze Delta table was loaded correctly by running a
#           set of lightweight quality checks after write_bronze_overwrite()
#           or write_bronze_merge() completes. Informational only — does not
#           raise, so a data quality flag does not block the pipeline.
#
# =============================================================================

def validate_bronze(target_table: str, key_column):
    """
    Run post-write validation checks on a Bronze Delta table.

    Parameters:
        target_table : schema-qualified table name, e.g. "dbo.bronze_orders"
        key_column    : string (single column) or list of strings
                        (composite key) — must match what was used in
                        write_bronze_merge()/write_bronze_overwrite()

    Returns:
        dict with total_rows, null_key_count, duplicate_key_count —
        useful for logging or asserting in the orchestrator.
    """
    key_cols = [key_column] if isinstance(key_column, str) else list(key_column)

    df_bronze = spark.table(target_table)

    total_rows = df_bronze.count()

    # Null check across all key columns combined (a row is "bad" if ANY
    # key column is null)
    null_condition = None
    for k in key_cols:
        cond = F.col(k).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)
    null_key_count = df_bronze.filter(null_condition).count()

    latest_update = df_bronze.agg(F.max("updated_at")).collect()[0][0]
    earliest_creation = df_bronze.agg(F.min("created_at")).collect()[0][0]

    # Duplicate check on the key column(s) — should never happen after a
    # correct overwrite or MERGE; if it does, it signals a real data
    # integrity issue worth investigating.
    duplicate_keys_df = df_bronze.groupBy(*key_cols).count().filter("count > 1")
    duplicate_keys_count = duplicate_keys_df.count()

    print(f"{'='*60}")
    print(f"  POST-WRITE VALIDATION — {target_table}")
    print(f"{'='*60}")

    print(f"\n── Row count ───────────────────────────────────────────────")
    print(f"  Total rows in Bronze table : {total_rows:,}")

    print(f"\n── Key integrity ({', '.join(key_cols)}) ─────────────────────")
    print(f"  Null key value(s) : {null_key_count:,}")
    print(f"  {'✅ No nulls on key' if null_key_count == 0 else '❌ Nulls found — critical issue'}")

    print(f"\n── Duplicate check on key ({', '.join(key_cols)}) ──────────────")
    if duplicate_keys_count == 0:
        print(f"  ✅ No duplicate key values found in Bronze table")
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
# CODE CELL 8 (notebook cell index 8)
# =============================================================================
# =============================================================================
# CELL 7 — write_audit_log()
# =============================================================================
# Purpose : Write a single row to governance.audit_ingestion_log describing
#           the outcome of processing one source (dimension or fact).
#
# Note: audit_ingestion_log enforces NOT NULL at the DDL level on run_id,
#       table_name, run_timestamp, and status — if any of those are missing,
#       this write will fail loudly, which is the desired behavior for a
#       tracking table (silent gaps in audit data are worse than a hard error).
#
# Identical structure to the Oracle Census version — reused as-is, since the
# audit table schema is shared across all ACME Bronze pipelines (dimensions
# and facts write to the same governance.audit_ingestion_log table).
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

# =============================================================================
# CODE CELL 9 (notebook cell index 9)
# =============================================================================
# =============================================================================
# CELL 8 — read_source()
# =============================================================================
# Purpose : Generic reader for CSV, XLSX, and XML source files. Originally
#           built and tested in 00_bronze_eda_profiling — moved here so both
#           the EDA notebook AND the Bronze ingestion notebooks
#           (02a_bronze_dimensions, 02b_bronze_facts) share the exact same
#           reading logic, instead of maintaining two copies.
# =============================================================================

BASE_SOURCE_PATH       = "Files/bronze_sources/"                      # relative path, used by spark.read
BASE_SOURCE_PATH_LOCAL = "/lakehouse/default/Files/bronze_sources/"   # local fs path, used by pandas

def read_source(file_name: str, file_type: str, sheet_name: str = None):
    """
    Read a single raw source file into a Spark DataFrame, with no transformations.

    Parameters:
        file_name : name of the file inside BASE_SOURCE_PATH
        file_type : one of "csv", "xlsx", "xml"
        sheet_name: required only when file_type == "xlsx"

    Returns:
        Spark DataFrame with the raw contents of the file
    """
    full_path       = BASE_SOURCE_PATH + file_name
    full_path_local = BASE_SOURCE_PATH_LOCAL + file_name

    if file_type == "csv":
        # multiLine=true: some sources (e.g. Customers.csv) have fields with
        # a literal line break inside quotes — without this, Spark treats
        # embedded newlines as row boundaries, inflating row counts.
        # encoding="Cp850": source files use the legacy DOS/IBM850 codepage,
        # confirmed via EDA (e.g. "M\x82xico" -> "México" only under CP850).
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("encoding", "Cp850")
            .csv(full_path)
        )

    elif file_type == "xlsx":
        # No native Spark Excel connector is installed in this Fabric
        # environment. Uses the same pandas -> spark.createDataFrame
        # pattern already used for CSV detection in Oracle Census.
        if sheet_name is None:
            raise ValueError(f"sheet_name is required to read XLSX file: {file_name}")

        df_pandas = pd.read_excel(full_path_local, sheet_name=sheet_name, engine="openpyxl")

        # Cast to string for Bronze (avoids pandas/Spark type mismatches on
        # mixed-type columns), but astype(str) turns real NaN into the
        # literal text "nan" — replace those back to proper None first.
        df_pandas = df_pandas.astype(str).where(df_pandas.notna(), None)

        df = spark.createDataFrame(df_pandas)

    elif file_type == "xml":
        df = (
            spark.read
            .format("xml")
            .option("rowTag", "_empty_")
            .load(full_path)
        )

    else:
        raise ValueError(f"Unsupported file_type '{file_type}' for file {file_name}")

    return df

# =============================================================================
# CODE CELL 10 (notebook cell index 10)
# =============================================================================
# =============================================================================
# CELL 9 — Independent row count validators
# =============================================================================
# Purpose : Count rows in a source file using a DIFFERENT method than
#           read_source() (which uses Spark). This catches bugs where Spark
#           misreads a file in a way that still "succeeds" without erroring
#           -- e.g. the multiLine bug found during EDA, where Customers.csv
#           silently inflated from 92 to 100 rows with no error raised.
#
# Design: pandas is used as the independent method. pandas' default CSV
# parser already handles embedded newlines inside quoted fields correctly
# (unlike Spark, which requires the explicit multiLine=true option) -- so
# using pandas here is a genuinely independent cross-check, not just the
# same logic written twice.
#
# Scope: covers single-file sources (CSV/XLSX/XML) directly. Shipments gets
# its own function since it spans multiple dynamically-discovered files.
# Budget is intentionally NOT auto-validated here -- its final row count
# (33, after unpivot) is not comparable to a raw file row count (8 real
# data rows) by design; that transformation is documented separately.
# =============================================================================

def count_source_rows_independent(file_name: str, file_type: str, sheet_name: str = None) -> int:
    """
    Count rows in a single source file using pandas, independent of the
    Spark-based read_source() function used by the actual pipeline.

    Parameters:
        file_name  : name of the file inside BASE_SOURCE_PATH
        file_type  : one of "csv", "xlsx", "xml"
        sheet_name : required only when file_type == "xlsx"

    Returns:
        int row count (excluding header)
    """
    full_path_local = BASE_SOURCE_PATH_LOCAL + file_name

    if file_type == "csv":
        df = pd.read_csv(full_path_local, encoding="cp850")
        return len(df)

    elif file_type == "xlsx":
        if sheet_name is None:
            raise ValueError(f"sheet_name is required to count XLSX file: {file_name}")
        df = pd.read_excel(full_path_local, sheet_name=sheet_name, engine="openpyxl")
        return len(df)

    elif file_type == "xml":
        import xml.etree.ElementTree as ET
        tree = ET.parse(full_path_local)
        root = tree.getroot()
        return len(list(root))

    else:
        raise ValueError(f"Unsupported file_type '{file_type}' for file {file_name}")


def count_shipments_rows_independent() -> int:
    """
    Independently count total rows across ALL Shipments*.csv files
    (dynamically discovered, same discovery logic as read_shipments_combined(),
    but counted here with pandas instead of Spark).

    Returns:
        int total row count summed across all discovered files
    """
    all_files = mssparkutils.fs.ls(BASE_SOURCE_PATH)
    shipment_files = sorted([
        f.name for f in all_files
        if f.name.startswith("Shipments") and f.name.endswith(".csv")
    ])

    total = 0
    for f in shipment_files:
        total += count_source_rows_independent(file_name=f, file_type="csv")

    return total
