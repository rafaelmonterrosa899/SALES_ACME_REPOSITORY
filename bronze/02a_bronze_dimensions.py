# =============================================================================
# NOTEBOOK: 02a_bronze_dimensions
# Consolidated from Fabric notebook (.ipynb) into a single .py file for
# version control in GitHub. Cell boundaries are preserved as comment markers
# so the notebook structure remains traceable in source control.
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# <h1 style="color:#C00000; font-weight:bold; font-family:'EB Garamond', Garamond, serif; letter-spacing:1.5px; border-bottom: 2px solid #C00000; padding-bottom:6px;">02_bronze_dimensions</h1>

# =============================================================================
# CODE CELL 1 (notebook cell index 1)
# =============================================================================
# =============================================================================
# CELL 0 — Header & pipeline overview
# =============================================================================
# Notebook: 02a_bronze_dimensions
# Purpose : Bronze ingestion for all 8 ACME dimension sources. Each source
#           is read as-is (no transformations), column names standardized
#           to snake_case, audit columns added, and written using a full
#           OVERWRITE strategy — appropriate for dimensions, where each
#           file represents a complete, current extract with no partial/
#           delta feed to reconcile (see design discussion in prior sessions).
#
# Sources processed (8):
#   1. Categories.csv                          -> dbo.bronze_categories
#   2. Products.csv                             -> dbo.bronze_products
#   3. Customers.csv                             -> dbo.bronze_customers
#   4. Suppliers.xml                             -> dbo.bronze_suppliers
#   5. Divisions.csv                             -> dbo.bronze_divisions
#   6. Employees Offices.xlsx (sheet: Employee)  -> dbo.bronze_employees
#   7. Employees Offices.xlsx (sheet: Office)    -> dbo.bronze_offices
#   8. Shippers.csv                              -> dbo.bronze_shippers
#
# Error handling: each source runs inside its OWN try/except block — a
# failure on one source (e.g. a malformed file) does not stop the other 7
# from being processed. A final summary table reports status per source.
#
# Dependencies: requires 00_bronze_shared_functions (loaded via %run) for
# read_source(), normalize_schema(), write_bronze_overwrite(),
# validate_bronze(), write_audit_log().
#
# Known data quality findings from EDA (00_bronze_eda_profiling) relevant
# to these sources — NOT fixed here (Bronze = no transformations), only
# flagged for Silver:
#   - Customers.csv requires encoding="Cp850" and multiLine="true" when
#     read (handled inside read_source() itself, not a Silver fix — see
#     prior design discussion: correct file READING belongs in Bronze,
#     correct DATA VALUES/business logic belongs in Silver)
#   - Divisions.csv has a genuine duplicate key (DivisionID=2 maps to two
#     different names) — will surface in validate_bronze() as a duplicate
#     key warning, expected and already understood, not a bug in this code
#
# Author  : Rafael
# Date    : 2026-07-13
# =============================================================================

# =============================================================================
# CODE CELL 2 (notebook cell index 2)
# =============================================================================
# NOTE: Fabric magic command, not valid standalone Python syntax.
# %run 01_bronze_shared_functions

# =============================================================================
# CODE CELL 3 (notebook cell index 3)
# =============================================================================
# =============================================================================
# CELL 2 — Configuration & run identity
# =============================================================================
# Constants specific to THIS notebook (dimensions). Shared imports, spark
# session, and the read/write/validate functions already came in via %run
# in Cell 1 — only notebook-specific setup goes here.

SOURCE_SYSTEM = "ACME_FILE_REPO"   # fixed value: files were manually uploaded
                                     # to Files/bronze_sources/ for this POC.
                                     # Would change to e.g. "ACME_SHAREPOINT"
                                     # if/when the automated pipeline
                                     # discussed earlier gets built.

RUN_ID        = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.now(timezone.utc)

print(f"Run ID         : {RUN_ID}")
print(f"Run timestamp  : {RUN_TIMESTAMP}")
print(f"Source system  : {SOURCE_SYSTEM}")
print(f"Source path    : {BASE_SOURCE_PATH}")

# =============================================================================
# CODE CELL 4 (notebook cell index 4)
# =============================================================================
# =============================================================================
# CELL 3 — Dimension sources configuration
# =============================================================================
# Config-driven list of all 8 dimension sources. Adding/removing a dimension
# later is a one-line change here — no changes needed to the orchestrator
# (Cell 4). Values confirmed during formal EDA in 00_bronze_eda_profiling.

DIMENSION_SOURCES = [
    {
        "source_name":  "Categories",
        "file_name":    "Categories.csv",
        "file_type":    "csv",
        "target_table": "dbo.bronze_categories",
        "key_column":   "category_id",
    },
    {
        "source_name":  "Products",
        "file_name":    "Products.csv",
        "file_type":    "csv",
        "target_table": "dbo.bronze_products",
        "key_column":   "product_id",
    },
    {
        "source_name":  "Customers",
        "file_name":    "Customers.csv",
        "file_type":    "csv",
        "target_table": "dbo.bronze_customers",
        "key_column":   "customer_id",
    },
    {
        "source_name":  "Suppliers",
        "file_name":    "Suppliers.xml",
        "file_type":    "xml",
        "target_table": "dbo.bronze_suppliers",
        "key_column":   "supplier_id",
    },
    {
        "source_name":  "Divisions",
        "file_name":    "Divisions.csv",
        "file_type":    "csv",
        "target_table": "dbo.bronze_divisions",
        "key_column":   "division_id",
        # NOTE: DivisionID=2 is a confirmed genuine duplicate (maps to two
        # different names). validate_bronze() WILL report 1 duplicate key
        # here — expected, not a bug. Flagged for Silver resolution.
    },
    {
        "source_name":  "Employees",
        "file_name":    "Employees Offices.xlsx",
        "file_type":    "xlsx",
        "sheet_name":   "Employee",
        "target_table": "dbo.bronze_employees",
        "key_column":   "emp_id",
    },
    {
        "source_name":  "Offices",
        "file_name":    "Employees Offices.xlsx",
        "file_type":    "xlsx",
        "sheet_name":   "Office",
        "target_table": "dbo.bronze_offices",
        "key_column":   "office",
    },
    {
        "source_name":  "Shippers",
        "file_name":    "Shippers.csv",
        "file_type":    "csv",
        "target_table": "dbo.bronze_shippers",
        "key_column":   "shipper_id",
    },
]

print(f"Configured {len(DIMENSION_SOURCES)} dimension sources:")
for s in DIMENSION_SOURCES:
    print(f"  - {s['source_name']:<12} ({s['file_type']:<4}) -> {s['target_table']}")

# =============================================================================
# CODE CELL 5 (notebook cell index 5)
# =============================================================================
# =============================================================================
# CELL 4 — ORCHESTRATOR (per-source try/except)
# =============================================================================
# Purpose : Run the full pipeline (read -> normalize -> write -> validate ->
#           audit log) for each of the 8 dimension sources. Each source runs
#           inside its OWN try/except block — a failure on one source does
#           NOT stop the other 7 from being processed. A final summary
#           table reports status per source.
# =============================================================================

run_results = []

for source in DIMENSION_SOURCES:
    source_name  = source["source_name"]
    status       = "SUCCESS"
    error_message = None
    rows_read     = None
    write_metrics = None
    validation    = None
    row_count_mismatch = False

    print(f"\n{'#'*70}")
    print(f"# PROCESSING DIMENSION: {source_name}")
    print(f"{'#'*70}")

    try:
        # 1. Read
        df_raw = read_source(
            file_name=source["file_name"],
            file_type=source["file_type"],
            sheet_name=source.get("sheet_name")
        )
        rows_read = df_raw.count()

        # 1b. Independent row count cross-check (pandas, not Spark) — catches
        # bugs where Spark reads a file "successfully" but with the wrong
        # row count (e.g. the multiLine issue found during EDA)
        expected_rows = count_source_rows_independent(
            file_name=source["file_name"],
            file_type=source["file_type"],
            sheet_name=source.get("sheet_name")
        )
        if rows_read != expected_rows:
            row_count_mismatch = True
            print(f"  ⚠️  ROW COUNT MISMATCH: Spark read {rows_read:,} rows, "
                  f"independent pandas count expected {expected_rows:,}")
        else:
            row_count_mismatch = False
            print(f"  ✅ Row count cross-check passed: {rows_read:,} rows (Spark == pandas)")

        # 2. Normalize schema (snake_case + audit columns)
        df_normalized = normalize_schema(
            df_raw=df_raw,
            source_file_name=source["file_name"],
            source_system=SOURCE_SYSTEM
        )

        # 3. Write (MERGE strategy — same as facts, so created_at/updated_at
        #    are tracked correctly across re-runs, and dimensions get real
        #    idempotency instead of a full reset on every run)
        write_metrics = write_bronze_merge(
            df_normalized=df_normalized,
            target_table=source["target_table"],
            merge_key=source["key_column"]
        )

        # 4. Validate
        validation = validate_bronze(
            target_table=source["target_table"],
            key_column=source["key_column"]
        )

        status = "SUCCESS" if not row_count_mismatch else "WARNING"

    except Exception as e:
        status        = "FAILED"
        error_message = f"{type(e).__name__}: {e}"
        print(f"\n{'='*60}")
        print(f"  ❌ FAILED processing {source_name}")
        print(f"{'='*60}")
        traceback.print_exc()

    finally:
        # 5. Audit log — always written, success or failure
        write_audit_log(
            run_id=RUN_ID,
            table_name=source["target_table"],
            source_file_name=source["file_name"],
            run_timestamp=RUN_TIMESTAMP,
            rows_read=rows_read,
            rows_inserted=write_metrics["rows_inserted"] if write_metrics else None,
            rows_updated=write_metrics["rows_updated"] if write_metrics else None,
            rows_total=write_metrics["rows_total_after_write"] if write_metrics else None,
            status=status,
            error_message=error_message
        )

    run_results.append({
        "source_name":   source_name,
        "target_table":  source["target_table"],
        "status":        status,
        "rows_read":     rows_read,
        "rows_written":  write_metrics["rows_total_after_write"] if write_metrics else None,
        "duplicate_keys": validation["duplicate_key_count"] if validation else None,
        "row_count_mismatch": row_count_mismatch,
        "expected_rows": expected_rows,
        "error_message": error_message,
    })

# -----------------------------------------------------------------------------
# Final summary — one row per source, easy to scan for failures at a glance
# -----------------------------------------------------------------------------
print(f"\n{'='*100}")
print(f"  RUN SUMMARY — 02a_bronze_dimensions")
print(f"{'='*100}")
print(f"  Run ID : {RUN_ID}")
print(f"{'='*100}")
print(f"  {'Source':<12} {'Status':<10} {'Rows Read':>10} {'Rows Written':>13} {'Dup Keys':>9}  Error")
print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*13} {'-'*9}  {'-'*30}")
for r in run_results:
    error_display = r["error_message"] if r["error_message"] else ""
    print(f"  {r['source_name']:<12} {r['status']:<10} "
          f"{r['rows_read'] if r['rows_read'] is not None else '-':>10} "
          f"{r['rows_written'] if r['rows_written'] is not None else '-':>13} "
          f"{r['duplicate_keys'] if r['duplicate_keys'] is not None else '-':>9}  {error_display}")

failed_count = sum(1 for r in run_results if r["status"] == "FAILED")
warning_count = sum(1 for r in run_results if r["status"] == "WARNING")
print(f"\n{'='*100}")
if failed_count == 0 and warning_count == 0:
    print(f"  ✅ All {len(run_results)} dimension sources processed successfully.")
else:
    if warning_count > 0:
        print(f"  ⚠️  {warning_count} source(s) with row count mismatch warnings — review above.")
    if failed_count > 0:
        print(f"  ⚠️  {failed_count} of {len(run_results)} sources FAILED — review errors above.")
print(f"{'='*100}")

# -----------------------------------------------------------------------------
# Dedicated row count validation summary — quick scan, separate from the
# general run summary above
# -----------------------------------------------------------------------------
print(f"\n{'='*90}")
print(f"  ROW COUNT VALIDATION SUMMARY — Spark (read_source) vs pandas (independent)")
print(f"{'='*90}")
print(f"  {'Source':<15} {'Spark read':>12} {'Pandas expected':>16} {'Match':>8}")
print(f"  {'-'*15} {'-'*12} {'-'*16} {'-'*8}")
for r in run_results:
    spark_val = r["rows_read"] if r["rows_read"] is not None else "-"
    expected_val = r.get("expected_rows") if r.get("expected_rows") is not None else "-"
    match_icon = "✅" if not r["row_count_mismatch"] else "❌"
    print(f"  {r['source_name']:<15} {spark_val:>12} {expected_val:>16} {match_icon:>8}")

mismatches = sum(1 for r in run_results if r["row_count_mismatch"])
print(f"{'='*90}")
if mismatches == 0:
    print(f"  ✅ All {len(run_results)} sources: Spark and pandas row counts match exactly.")
else:
    print(f"  ⚠️  {mismatches} source(s) show a row count mismatch — investigate before trusting Bronze.")
print(f"{'='*90}")
