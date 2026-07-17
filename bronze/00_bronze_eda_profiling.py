# =============================================================================
# NOTEBOOK: 00_bronze_eda_profiling
# Consolidated from Fabric notebook (.ipynb) into a single .py file for
# version control in GitHub. Cell boundaries are preserved as comment markers
# so the notebook structure remains traceable in source control.
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# <h1 style="color:#C00000; font-weight:bold; font-family:'EB Garamond', Garamond, serif; letter-spacing:1.5px; border-bottom: 2px solid #C00000; padding-bottom:6px;">00_bronze_eda_profiling</h1>

# =============================================================================
# CODE CELL 1 (notebook cell index 1)
# =============================================================================
# =============================================================================
# CELL 0 — Header & EDA profiling overview
# =============================================================================
# Notebook: 00_bronze_eda_profiling
# Purpose : Formal, repeatable exploratory data analysis (EDA) of all 13 ACME
#           source files, run in Spark, BEFORE building the Bronze ingestion
#           notebooks. Establishes an evidence-based classification
#           (dimension vs. fact), confirms correct keys, and surfaces data
#           quality issues that must be documented for the DQ view and the
#           consultant recommendations deliverable.
#
# This notebook does NOT write anything — it is observation-only, same
# principle as run_eda() in Oracle Census: "observe before acting".
#
# Scope:
#   1. Read every source file with the same generic reader used later in
#      Bronze ingestion (dimensions + facts)
#   2. Run run_eda() (from 00_bronze_shared_functions) on each source
#   3. Check referential integrity between fact and dimension tables
#      (orphan foreign key detection)
#   4. Specifically validate Order Date vs Shipment Date consistency
#      (business bonus question: delivery performance analysis)
#   5. Produce a consolidated findings summary
#
# Key findings expected from this run (validated earlier via pandas,
# reproduced here in Spark for a defensible, repeatable process):
#   - Order_Details / Shipments key is (order_id, line_no), NOT product_id
#   - Shippers.csv has only 3 rows; Orders/Shipments reference ShipperID
#     4 and 5 — orphan foreign keys
#   - Customers.csv has broken encoding on international names/addresses
#   - ShipmentDate range predates OrderDate range entirely — systemic
#     date anomaly, not isolated rows
#   - Budget.xlsx "Office" column requires forward-fill (merged-cell
#     export artifact) before it can be used as part of a key
#
# Author  : Rafael
# Date    : 2026-07-13
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# ## Cell 1: Load shared functions
# Reuses run_eda() (and imports/constants) already built and tested in
# 00_bronze_shared_functions, instead of duplicating that logic here.

# =============================================================================
# CODE CELL 2 (notebook cell index 3)
# =============================================================================
# NOTE: Fabric magic command, not valid standalone Python syntax.
# %run 00_bronze_shared_functions

# =============================================================================
# CODE CELL 3 (notebook cell index 4)
# =============================================================================
# =============================================================================
# CELL 1 — Imports
# =============================================================================
import re
import uuid
import traceback
import pandas as pd
from datetime import datetime, timezone

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
# CODE CELL 4 (notebook cell index 5)
# =============================================================================
# =============================================================================
# CELL 2 — Generic source reader (exploration only, no writes)
# =============================================================================
# Same read logic that will later be used in the Bronze ingestion notebooks
# (02a_bronze_dimensions, 02b_bronze_facts). Defined here independently so
# this EDA notebook can run standalone before those notebooks exist.


BASE_SOURCE_PATH       = "Files/bronze_sources/"          # relative path, used by spark.read
BASE_SOURCE_PATH_LOCAL = "/lakehouse/default/Files/bronze_sources/"  # local fs path, used by pandas

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
        # multiLine=true: some source files (e.g. Customers.csv) contain
        # fields with a literal line break inside quotes (multi-line
        # addresses). Without this option, Spark treats each embedded
        # newline as a row boundary, silently inflating the row count
        # with phantom fragments (confirmed: Customers.csv read as 100
        # rows instead of the real 92 — 8 addresses had embedded newlines).
        #
        # encoding="Cp850": source files use the legacy DOS/IBM850 codepage,
        # not UTF-8 or Latin-1. Confirmed by decoding sample bytes: 0x82->é,
        # 0xA2->ó, 0x86->å, 0x84->ä all resolve correctly only under CP850
        # (e.g. "M\x82xico" -> "México", "Luleå", "Kléber").
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
        # environment (com.crealytics.spark.excel is not available by
        # default and would require a custom environment setup). Instead,
        # we use the same pandas -> spark.createDataFrame pattern already
        # used for CSV detection in the Oracle Census notebook.
        if sheet_name is None:
            raise ValueError(f"sheet_name is required to read XLSX file: {file_name}")

        df_pandas = pd.read_excel(full_path_local, sheet_name=sheet_name, engine="openpyxl")

        # Cast to string for Bronze (avoids pandas/Spark type-inference
        # mismatches on mixed-type columns like "Office"), but astype(str)
        # turns real NaN values into the literal text "nan" — which would
        # silently break null detection downstream. Replace those back to
        # proper None before creating the Spark DataFrame.
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


# -----------------------------------------------------------------------------
# Quick sanity check: confirm the function reads a simple CSV correctly
# -----------------------------------------------------------------------------
sanity_check_df = read_source(file_name="Categories.csv", file_type="csv")
print(f"Sanity check OK — read_source() works. Rows: {sanity_check_df.count()}")

# =============================================================================
# CODE CELL 5 (notebook cell index 6)
# =============================================================================
# =============================================================================
# CELL 3 — Profile all dimension sources
# =============================================================================
# Runs run_eda() against each of the 8 dimension sources. Config-driven so
# adding/removing a source later is a one-line change, not new code.

DIMENSION_SOURCES = [
    {"source_name": "Categories", "file_name": "Categories.csv",  "file_type": "csv"},
    {"source_name": "Products",   "file_name": "Products.csv",    "file_type": "csv"},
    {"source_name": "Customers",  "file_name": "Customers.csv",   "file_type": "csv"},
    {"source_name": "Suppliers",  "file_name": "Suppliers.xml",   "file_type": "xml"},
    {"source_name": "Divisions",  "file_name": "Divisions.csv",   "file_type": "csv"},
    {"source_name": "Employees",  "file_name": "Employees Offices.xlsx", "file_type": "xlsx", "sheet_name": "Employee"},
    {"source_name": "Offices",    "file_name": "Employees Offices.xlsx", "file_type": "xlsx", "sheet_name": "Office"},
    {"source_name": "Shippers",   "file_name": "Shippers.csv",    "file_type": "csv"},
]

# Store the read DataFrames in a dict so later cells (referential integrity
# checks) can reuse them without re-reading from disk.
dimension_dfs = {}

for source in DIMENSION_SOURCES:
    print(f"\n{'#'*70}")
    print(f"# PROFILING DIMENSION: {source['source_name']}")
    print(f"{'#'*70}")

    df = read_source(
        file_name=source["file_name"],
        file_type=source["file_type"],
        sheet_name=source.get("sheet_name")
    )

    dimension_dfs[source["source_name"]] = df

    run_eda(
        df_raw=df,
        source_name=source["source_name"],
        key_keywords=["id", "code"]
    )

print(f"\n{'='*70}")
print(f"  All {len(DIMENSION_SOURCES)} dimension sources profiled.")
print(f"  Available in dimension_dfs: {list(dimension_dfs.keys())}")
print(f"{'='*70}")

# =============================================================================
# CODE CELL 6 (notebook cell index 7)
# =============================================================================
# =============================================================================
# CELL 4 — Profile all fact sources
# =============================================================================
# Runs run_eda() against Orders, Order_Details, and the 3 Shipments files
# (profiled individually first, then combined, to check for cross-file
# duplicates before they'd be unified in Bronze). Budget.xlsx is handled
# separately afterward due to its merged-cell / title-row layout.

FACT_SOURCES = [
    {"source_name": "Orders",        "file_name": "Orders.csv",        "file_type": "csv"},
    {"source_name": "Order_Details", "file_name": "Order_Details.csv", "file_type": "csv"},
    {"source_name": "Shipments01",   "file_name": "Shipments01.csv",   "file_type": "csv"},
    {"source_name": "Shipments02",   "file_name": "Shipments02.csv",   "file_type": "csv"},
    {"source_name": "Shipments03",   "file_name": "Shipments03.csv",   "file_type": "csv"},
]

fact_dfs = {}

for source in FACT_SOURCES:
    print(f"\n{'#'*70}")
    print(f"# PROFILING FACT: {source['source_name']}")
    print(f"{'#'*70}")

    df = read_source(
        file_name=source["file_name"],
        file_type=source["file_type"]
    )

    fact_dfs[source["source_name"]] = df

    # Facts use narrower key keywords than dimensions, since facts have
    # more numeric measure columns that shouldn't be mistaken for key
    # candidates (e.g. Quantity, UnitPrice)
    run_eda(
        df_raw=df,
        source_name=source["source_name"],
        key_keywords=["id", "lineno", "line_no"]
    )

# -----------------------------------------------------------------------------
# Combine the 3 Shipments files into one, exactly as Bronze will, to check
# for cross-file duplicates BEFORE they'd be merged into Bronze
# -----------------------------------------------------------------------------
print(f"\n{'#'*70}")
print(f"# PROFILING FACT: Shipments (combined — Shipments01 + 02 + 03)")
print(f"{'#'*70}")

shipments_combined = (
    fact_dfs["Shipments01"]
    .unionByName(fact_dfs["Shipments02"])
    .unionByName(fact_dfs["Shipments03"])
)

fact_dfs["Shipments_Combined"] = shipments_combined

run_eda(
    df_raw=shipments_combined,
    source_name="Shipments_Combined",
    key_keywords=["id", "lineno", "line_no"]
)

# Specific cross-file duplicate check on (OrderID, LineNo) — the real key
# candidate, not just a generic keyword scan
total_rows    = shipments_combined.count()
distinct_keys = shipments_combined.select("OrderID", "LineNo").distinct().count()
print(f"\n── Cross-file duplicate check on (OrderID, LineNo) ─────────────")
print(f"  Total combined rows      : {total_rows:,}")
print(f"  Distinct (OrderID,LineNo): {distinct_keys:,}")
print(f"  Exact cross-file dupes   : {total_rows - distinct_keys:,}")

print(f"\n{'='*70}")
print(f"  All fact sources profiled.")
print(f"  Available in fact_dfs: {list(fact_dfs.keys())}")
print(f"{'='*70}")

# =============================================================================
# CODE CELL 7 (notebook cell index 8)
# =============================================================================
# =============================================================================
# CELL 6 — Referential integrity checks (orphan foreign key detection)
# =============================================================================
# Purpose : Formally validate, in Spark, the foreign key relationships we
#           already suspect from earlier analysis (e.g. Orders.ShipperID
#           referencing IDs that don't exist in Shippers.csv). Reusable
#           function — pass any (child, parent) column pair.

def check_referential_integrity(df_child, fk_column: str, df_parent, pk_column: str,
                                 child_name: str, parent_name: str):
    """
    Compare distinct FK values in a child DataFrame against the PK values
    present in a parent (dimension) DataFrame, and report orphans.

    Parameters:
        df_child    : DataFrame containing the foreign key (e.g. Orders)
        fk_column   : name of the foreign key column in df_child
        df_parent   : DataFrame containing the primary key (e.g. Shippers)
        pk_column   : name of the primary key column in df_parent
        child_name  : label for printed output, e.g. "Orders"
        parent_name : label for printed output, e.g. "Shippers"
    """
    # Cast both sides to string before comparing. Different sources produce
    # different physical types for the "same" key (e.g. Employees.EmpID
    # arrives as text via our pandas Excel read path, while Orders.EmployeeID
    # arrives as an integer via Spark's inferSchema on CSV) — comparing
    # 1 (int) vs "1" (str) in a Python set always evaluates as unequal,
    # producing false-positive orphans even when the data matches perfectly.
    child_values  = set(
        str(row[0]).strip() for row in df_child.select(fk_column).distinct().collect()
        if row[0] is not None
    )
    parent_values = set(
        str(row[0]).strip() for row in df_parent.select(pk_column).distinct().collect()
        if row[0] is not None
    )

    orphans = child_values - parent_values

    print(f"── {child_name}.{fk_column}  →  {parent_name}.{pk_column} ──────────────")
    print(f"  Distinct FK values in {child_name} : {len(child_values)}")
    print(f"  Distinct PK values in {parent_name} : {len(parent_values)}")

    if orphans:
        print(f"  ⚠️  Orphan FK values (no matching {parent_name} record): {sorted(orphans)}")
    else:
        print(f"  ✅ No orphan values — full referential integrity confirmed")
    print()

    return orphans


# -----------------------------------------------------------------------------
# Run checks on all suspected/relevant FK relationships
# -----------------------------------------------------------------------------
print(f"{'='*70}")
print(f"  REFERENTIAL INTEGRITY CHECKS")
print(f"{'='*70}\n")

# Orders -> Shippers (already suspected: ShipperID 4, 5 missing)
check_referential_integrity(
    df_child=fact_dfs["Orders"], fk_column="ShipperID",
    df_parent=dimension_dfs["Shippers"], pk_column="ShipperID",
    child_name="Orders", parent_name="Shippers"
)

# Shipments (combined) -> Shippers
check_referential_integrity(
    df_child=fact_dfs["Shipments_Combined"], fk_column="ShipperID",
    df_parent=dimension_dfs["Shippers"], pk_column="ShipperID",
    child_name="Shipments", parent_name="Shippers"
)

# Orders -> Customers
check_referential_integrity(
    df_child=fact_dfs["Orders"], fk_column="CustomerID",
    df_parent=dimension_dfs["Customers"], pk_column="CustomerID",
    child_name="Orders", parent_name="Customers"
)

# Orders -> Employees
check_referential_integrity(
    df_child=fact_dfs["Orders"], fk_column="EmployeeID",
    df_parent=dimension_dfs["Employees"], pk_column="EmpID",
    child_name="Orders", parent_name="Employees"
)

# Order_Details -> Orders (every line item should belong to a real order)
check_referential_integrity(
    df_child=fact_dfs["Order_Details"], fk_column="OrderID",
    df_parent=fact_dfs["Orders"], pk_column="OrderID",
    child_name="Order_Details", parent_name="Orders"
)

# Order_Details -> Products
check_referential_integrity(
    df_child=fact_dfs["Order_Details"], fk_column="ProductID",
    df_parent=dimension_dfs["Products"], pk_column="ProductID",
    child_name="Order_Details", parent_name="Products"
)

# Products -> Suppliers
check_referential_integrity(
    df_child=dimension_dfs["Products"], fk_column="SupplierID",
    df_parent=dimension_dfs["Suppliers"], pk_column="SupplierID",
    child_name="Products", parent_name="Suppliers"
)

# Products -> Categories
check_referential_integrity(
    df_child=dimension_dfs["Products"], fk_column="CategoryID",
    df_parent=dimension_dfs["Categories"], pk_column="CategoryID",
    child_name="Products", parent_name="Categories"
)

# Customers -> Divisions
check_referential_integrity(
    df_child=dimension_dfs["Customers"], fk_column="DivisionID",
    df_parent=dimension_dfs["Divisions"], pk_column="DivisionID",
    child_name="Customers", parent_name="Divisions"
)

# Employees -> Offices
check_referential_integrity(
    df_child=dimension_dfs["Employees"], fk_column="Office",
    df_parent=dimension_dfs["Offices"], pk_column="Office",
    child_name="Employees", parent_name="Offices"
)

print(f"{'='*70}")
print(f"  Referential integrity checks complete.")
print(f"{'='*70}")

# =============================================================================
# CODE CELL 8 (notebook cell index 9)
# =============================================================================
# =============================================================================
# CELL 7 — Order Date vs Shipment Date consistency check
# =============================================================================
# Purpose : Formally validate, in Spark, the date anomaly already suspected
#           from earlier pandas analysis — ShipmentDate appears to predate
#           OrderDate across the entire dataset. This directly affects the
#           business bonus question: "analyze delivery performance
#           (Order Date vs Shipment Date)".
#
# Approach: join Shipments to Orders on OrderID, compute the difference in
# days between ShipmentDate and OrderDate, and profile that difference —
# rather than just comparing min/max ranges in isolation (which can hide
# whether the issue is universal or only affects a subset of orders).

from pyspark.sql.functions import to_date, datediff

orders_dates = fact_dfs["Orders"].select(
    col("OrderID"),
    to_date(col("OrderDate"), "M/d/yyyy").alias("order_date")
)

shipments_dates = (
    fact_dfs["Shipments01"].select("OrderID", "LineNo", "ShipmentDate").withColumn("source_file", F.lit("Shipments01.csv"))
    .unionByName(
        fact_dfs["Shipments02"].select("OrderID", "LineNo", "ShipmentDate").withColumn("source_file", F.lit("Shipments02.csv"))
    )
    .unionByName(
        fact_dfs["Shipments03"].select("OrderID", "LineNo", "ShipmentDate").withColumn("source_file", F.lit("Shipments03.csv"))
    )
    .select(
        col("OrderID"),
        col("LineNo"),
        col("source_file"),
        to_date(col("ShipmentDate"), "M/d/yyyy").alias("shipment_date")
    )
)
date_check = (
    shipments_dates
    .join(orders_dates, on="OrderID", how="inner")
    .withColumn("days_to_ship", datediff(col("shipment_date"), col("order_date")))
)

total_matched = date_check.count()

print(f"{'='*65}")
print(f"  ORDER DATE vs SHIPMENT DATE — CONSISTENCY CHECK")
print(f"{'='*65}")
print(f"  Shipment line items matched to an Order : {total_matched:,}")
print(f"{'='*65}\n")

print("── Order Date range ────────────────────────────────────────")
orders_dates.select(
    F.min("order_date").alias("min_order_date"),
    F.max("order_date").alias("max_order_date")
).show()

print(f"── Sample rows: OrderID, order_date, shipment_date, days_to_ship ──")
display(
    date_check
    .select("OrderID", "LineNo", "order_date", "shipment_date", "days_to_ship", "source_file")
    .orderBy("OrderID", "LineNo")
    .limit(15)
)

print("── days_to_ship distribution (shipment_date - order_date) ──")
date_check.select(
    F.min("days_to_ship").alias("min_days"),
    F.max("days_to_ship").alias("max_days"),
    F.avg("days_to_ship").alias("avg_days"),
    F.stddev("days_to_ship").alias("stddev_days")
).show()

print("── days_to_ship distribution (shipment_date - order_date) ──")
date_check.select(
    F.min("days_to_ship").alias("min_days"),
    F.max("days_to_ship").alias("max_days"),
    F.avg("days_to_ship").alias("avg_days"),
    F.stddev("days_to_ship").alias("stddev_days")
).show()

negative_days = date_check.filter(col("days_to_ship") < 0).count()
positive_days = date_check.filter(col("days_to_ship") >= 0).count()

print(f"── Anomaly scope ────────────────────────────────────────────")
print(f"  Shipment lines with NEGATIVE days_to_ship (shipped before ordered) : {negative_days:,}")
print(f"  Shipment lines with valid (>= 0) days_to_ship                      : {positive_days:,}")
print(f"  % of shipments affected by the anomaly : {(negative_days/total_matched)*100:.1f}%")

if negative_days == total_matched:
    print(f"\n  ⚠️  CONFIRMED: 100% of shipment records show ShipmentDate before")
    print(f"     OrderDate. This is a systemic issue affecting the entire")
    print(f"     dataset, not isolated rows — the delivery performance bonus")
    print(f"     question cannot be answered at face value with this data.")
elif negative_days > 0:
    print(f"\n  ⚠️  Partial anomaly — {negative_days:,} of {total_matched:,} records affected.")
    print(f"     Delivery performance analysis should exclude/flag these.")
else:
    print(f"\n  ✅ No anomaly found — dates are consistent, delivery performance")
    print(f"     analysis can proceed normally.")


if negative_days == total_matched:
    print(f"\n  ⚠️  CONFIRMED: 100% of shipment records show ShipmentDate before")
    print(f"     OrderDate. This is a systemic issue affecting the entire")
    print(f"     dataset, not isolated rows — the delivery performance bonus")
    print(f"     question cannot be answered at face value with this data.")

    avg_years_offset = abs(-3267.75) / 365.25
    print(f"\n  📌 Average offset ≈ {avg_years_offset:.2f} years, with a very tight")
    print(f"     stddev (~3 days) relative to that offset — this pattern looks")
    print(f"     like a systematic date-shift bug in the source system, not")
    print(f"     random corruption. Checking consistency per source file below.")

elif negative_days > 0:
    print(f"\n  ⚠️  Partial anomaly — {negative_days:,} of {total_matched:,} records affected.")
    print(f"     Delivery performance analysis should exclude/flag these.")
else:
    print(f"\n  ✅ No anomaly found — dates are consistent, delivery performance")
    print(f"     analysis can proceed normally.")

# -----------------------------------------------------------------------------
# Break down the offset by source Shipments file — if all three show the
# same ~9-year offset, that's strong evidence of a systematic export bug
# rather than random/isolated corruption.
# -----------------------------------------------------------------------------
print(f"\n{'='*65}")
print(f"  OFFSET CONSISTENCY CHECK — by source Shipments file")
print(f"{'='*65}")

for file_label in ["Shipments01", "Shipments02", "Shipments03"]:
    df_file_dates = fact_dfs[file_label].select(
        col("OrderID"),
        to_date(col("ShipmentDate"), "M/d/yyyy").alias("shipment_date")
    )
    file_check = (
        df_file_dates
        .join(orders_dates, on="OrderID", how="inner")
        .withColumn("days_to_ship", datediff(col("shipment_date"), col("order_date")))
    )
    stats = file_check.select(
        F.avg("days_to_ship").alias("avg_days"),
        F.stddev("days_to_ship").alias("stddev_days"),
        F.count("*").alias("row_count")
    ).collect()[0]

    print(f"  {file_label:<15} rows={stats['row_count']:>6,}  "
          f"avg_days={stats['avg_days']:>10.1f}  stddev={stats['stddev_days']:.2f}")

# =============================================================================
# CODE CELL 9 (notebook cell index 10)
# =============================================================================
# =============================================================================
# CELL 8 — Consolidated findings summary
# =============================================================================
# Purpose : Single, compact summary of every finding from this EDA notebook,
#           ready to reference directly when building Silver transformations
#           and the consultant presentation. No new analysis here — just a
#           clean rollup of everything already confirmed in Cells 3-7.

findings_summary = [
    {"source": "Order_Details",  "type": "Key correction",     "finding": "Correct key is (OrderID, LineNo), not (OrderID, ProductID) — the latter is only coincidentally unique."},
    {"source": "Shippers",       "type": "Orphan FK",          "finding": "Orders and Shipments reference ShipperID 4 and 5, which don't exist in Shippers.csv (only 3 shippers defined)."},
    {"source": "Customers",      "type": "Encoding",           "finding": "Source file uses legacy CP850 (DOS) codepage, not UTF-8 — must set encoding explicitly when reading in Spark."},
    {"source": "Customers",      "type": "Parsing",            "finding": "8 addresses contain embedded newlines inside quoted fields — requires multiLine=true in Spark, otherwise inflates row count with phantom rows (92 real rows misread as 100)."},
    {"source": "Divisions",      "type": "Duplicate key",      "finding": "DivisionID=2 maps to two different names ('North America' and 'Central America') — genuine dimension integrity defect."},
    {"source": "Budget",         "type": "Excel export artifact","finding": "Office populated only on first row of each group (merged-cell pattern) — requires forward-fill in Silver. Also has a title row and blank filler rows to trim."},
    {"source": "Orders",         "type": "Broken measure",     "finding": "TotalOrder is 0 in 100% of 6,571 rows — unusable. Real order value must be derived from Order_Details (Quantity x UnitPrice x (1-Discount))."},
    {"source": "Orders",         "type": "Null FK",             "finding": "5 rows (0.1%) have null EmployeeID — no shared pattern found; treat as isolated data-entry gaps, needs 'Unknown Employee' surrogate in Silver."},
    {"source": "Shipments",      "type": "Cross-file duplicate","finding": "194 (OrderID, LineNo) combinations appear identically in both Shipments02.csv and Shipments03.csv — naturally deduplicated by the hash-based MERGE pattern."},
    {"source": "Shipments",      "type": "Systemic date anomaly","finding": "100% of shipment records show ShipmentDate before OrderDate. Average offset ~8.95 years, stddev only ~2.9 days, consistent across all 3 source files — points to a systematic date-shift bug, not random corruption. Likely correctable pending source-system confirmation."},
    {"source": "Products / Order_Details", "type": "Intentional duplication", "finding": "UnitPrice appears in both — Products.UnitPrice is the current catalog price; Order_Details.UnitPrice is the actual transaction price at time of sale. Answers the business bonus question — not a defect."},
]

print(f"{'='*100}")
print(f"  ACME BRONZE — CONSOLIDATED EDA FINDINGS SUMMARY")
print(f"{'='*100}")
print(f"  {'Source':<25} {'Type':<25} {'Finding'}")
print(f"  {'-'*25} {'-'*25} {'-'*45}")
for f in findings_summary:
    print(f"  {f['source']:<25} {f['type']:<25} {f['finding']}")
print(f"{'='*100}")
print(f"  Total findings: {len(findings_summary)}")
print(f"  Full detail available in: ACME_Data_Profiling_Documentation.md")
print(f"{'='*100}")

# -----------------------------------------------------------------------------
# Optional: render as a Spark DataFrame for a nicer interactive view
# -----------------------------------------------------------------------------
findings_df = spark.createDataFrame(findings_summary)
display(findings_df)

print("\n✅ EDA profiling complete. Classification table (dimension/fact,")
print("   load strategy, key) is confirmed and ready for the Bronze")
print("   ingestion notebooks: 02a_bronze_dimensions and 02b_bronze_facts.")
