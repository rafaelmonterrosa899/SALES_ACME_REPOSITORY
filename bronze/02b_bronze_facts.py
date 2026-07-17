# =============================================================================
# NOTEBOOK: 02b_bronze_facts
# Consolidated from Fabric notebook (.ipynb) into a single .py file for
# version control in GitHub. Cell boundaries are preserved as comment markers
# so the notebook structure remains traceable in source control.
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# <h1 style="color:#C00000; font-weight:bold; font-family:'EB Garamond', Garamond, serif; letter-spacing:1.5px; border-bottom: 2px solid #C00000; padding-bottom:6px;">02b_bronze_facts</h1>

# =============================================================================
# CODE CELL 1 (notebook cell index 1)
# =============================================================================
# =============================================================================
# CELL 0 — Header & pipeline overview
# =============================================================================
# Notebook: 02b_bronze_facts
# Purpose : Bronze ingestion for all 4 ACME fact sources. Unlike dimensions
#           (02a_bronze_dimensions, full overwrite), facts use a MERGE/UPSERT
#           strategy with SHA-256 row-hash change detection — same pattern
#           already proven in Oracle Census — because facts represent
#           transactional history that should never be silently overwritten.
#
# Sources processed (4):
#   1. Orders.csv                                -> dbo.bronze_orders
#      key: order_id
#   2. Order_Details.csv                          -> dbo.bronze_order_details
#      key: (order_id, line_no) — NOT product_id, confirmed via EDA
#   3. Shipments01/02/03.csv (dynamically discovered, unified)
#                                                  -> dbo.bronze_shipments
#      key: (order_id, line_no)
#   4. Budget.xlsx (special structure: title row, merged-cell Office column,
#      year columns requiring unpivot)             -> dbo.bronze_budget
#      key: (office, employee_id, year)
#
# Special handling (isolated in dedicated functions, not forced into the
# generic read_source() path — same design principle used throughout this
# project: when a source breaks the generic pattern, give it its own
# function instead of adding special-case branches to shared code):
#   - read_shipments_combined() : dynamically discovers ALL files starting
#     with "Shipments" in Files/bronze_sources/ (not hardcoded to 3 files),
#     so a future Shipments04.csv is picked up automatically.
#   - read_budget()             : handles the title row, real header on
#     row 2, merged-cell Office forward-fill, and unpivots year columns
#     (2017-2021) into a proper fact grain (one row per office/employee/year).
#
# Error handling: each source runs inside its OWN try/except block — a
# failure on one source does not stop the other 3 from being processed.
# Same pattern as 02a_bronze_dimensions.
#
# Known data quality findings from EDA relevant to these sources — NOT
# fixed here (Bronze = no transformations to VALUES), only flagged:
#   - Orders.TotalOrder is 0 in 100% of rows — unusable, real order value
#     must be derived from Order_Details in Silver/Gold
#   - Orders has 5 null EmployeeID rows — isolated, no pattern found
#   - Shipments has a systemic ~8.95 year date offset vs Orders — flagged
#     for Silver/Gold, not corrected here
#   - 194 (order_id, line_no) combinations appear identically in both
#     Shipments02.csv and Shipments03.csv — naturally deduplicated by the
#     hash-based MERGE, no special handling needed in this notebook
#
# Author  : Rafael
# Date    : 2026-07-14
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
# Constants specific to THIS notebook (facts). Shared imports, spark session,
# and the read/write/validate functions already came in via %run in Cell 1.

SOURCE_SYSTEM = "ACME_FILE_REPO"   # same fixed value as 02a_bronze_dimensions —
                                     # both pipelines read from the same manually
                                     # uploaded file repository for this POC.

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
# CELL 3 — Special reader functions: Shipments (dynamic) and Budget
# =============================================================================
# These two sources break the generic read_source() pattern in different
# ways, so each gets its own dedicated function instead of forcing special
# cases into shared code — same principle used throughout this project.

# -----------------------------------------------------------------------------
# 3.1 — read_shipments_combined()
# -----------------------------------------------------------------------------
def read_shipments_combined():
    """
    Dynamically discover and read ALL files starting with "Shipments" in
    BASE_SOURCE_PATH, then unify them into a single DataFrame. New files
    (e.g. a future Shipments04.csv) are picked up automatically — no code
    change needed here.
    """
    all_files = mssparkutils.fs.ls(BASE_SOURCE_PATH)
    shipment_files = sorted([
        f.name for f in all_files
        if f.name.startswith("Shipments") and f.name.endswith(".csv")
    ])

    print(f"── Shipments files discovered in {BASE_SOURCE_PATH} ──────────")
    for f in shipment_files:
        print(f"  {f}")
    print(f"  Total files found: {len(shipment_files)}")

    if not shipment_files:
        raise FileNotFoundError(f"No Shipments*.csv files found in {BASE_SOURCE_PATH}")

    dfs = [read_source(file_name=f, file_type="csv") for f in shipment_files]

    df_combined = dfs[0]
    for df in dfs[1:]:
        df_combined = df_combined.unionByName(df)

    return df_combined


# -----------------------------------------------------------------------------
# 3.2 — read_budget()
# -----------------------------------------------------------------------------
def read_budget():
    """
    Read Budget.xlsx and fix its non-standard export layout, WITHOUT
    changing the grain of the data (Bronze principle: never change grain,
    only fix structural reading issues).

    Fixes applied here (structural reading issues — file is unreadable
    correctly without them, same category as CSV encoding/multiLine):
      - Skips row 1 (title "QWT Budget") — real header is row 2
      - Forward-fills "Office" — populated only on the first row of each
        group in the raw export (merged-cell pattern, confirmed via EDA).
        Without this, the Office/Employee relationship is structurally
        broken, not just "dirty" — this is a reading fix, not a business
        transformation.
      - Drops fully-blank trailing filler rows
      - Casts Office to int (avoids "1.0" vs "1" string mismatch bug found
        during testing)

    NOT done here (moved to Silver — changes the grain from one row per
    office/employee to one row per office/employee/year, which is a
    business modeling decision, not a reading fix):
      - Unpivoting year columns (2017..2021) into (year, budget_amount) rows

    Returns:
        Spark DataFrame with columns: Office, EmployeeID, 2017, 2018, 2019,
        2020, 2021 — same wide grain as the source file, just readable.
    """
    df_pandas = pd.read_excel(
        BASE_SOURCE_PATH_LOCAL + "Budget.xlsx",
        sheet_name="Budget",
        header=1  # row index 1 (0-based) = the second row = real header
    )

    # Drop fully-blank rows (trailing filler beyond the real data)
    df_pandas = df_pandas.dropna(how="all")

    # Forward-fill Office: populated only on the first row of each group
    df_pandas["Office"] = df_pandas["Office"].ffill()

    # Cast Office to int before it becomes a string, to avoid the
    # "1.0" vs "1" mismatch found during testing (see design notes)
    df_pandas["Office"] = df_pandas["Office"].astype(int)

    print(f"── Budget.xlsx — after title-row skip, blank-row drop, forward-fill ──")
    print(df_pandas.to_string())
    print(f"\n  Rows (wide grain, one per office/employee): {len(df_pandas)}")

    df_pandas = df_pandas.astype(str).where(df_pandas.notna(), None)

    return spark.createDataFrame(df_pandas)

# =============================================================================
# CODE CELL 5 (notebook cell index 5)
# =============================================================================

#Quick data check of previous step
test_shipments = read_shipments_combined()
print(f"Shipments combined rows: {test_shipments.count()}")

test_budget = read_budget()
display(test_budget.limit(10))

# =============================================================================
# CODE CELL 6 (notebook cell index 6)
# =============================================================================
# =============================================================================
# CELL 4 — Fact sources configuration
# =============================================================================
# Config-driven list of all 4 fact sources. Unlike dimensions, two of these
# (Shipments, Budget) use a custom_reader instead of the generic read_source()
# — the orchestrator (Cell 5) checks for that key and calls it when present.

FACT_SOURCES = [
    {
        "source_name":   "Orders",
        "file_name":     "Orders.csv",
        "file_type":     "csv",
        "target_table":  "dbo.bronze_orders",
        "key_column":    "order_id",
    },
    {
        "source_name":   "Order_Details",
        "file_name":     "Order_Details.csv",
        "file_type":     "csv",
        "target_table":  "dbo.bronze_order_details",
        "key_column":    ["order_id", "line_no"],
        # NOTE: key is (order_id, line_no), NOT product_id — confirmed via
        # EDA that (order_id, product_id) is only coincidentally unique.
    },
    {
        "source_name":   "Shipments",
        "custom_reader": read_shipments_combined,   # bypasses read_source() —
                                                       # dynamically discovers
                                                       # and unions Shipments*.csv
        "target_table":  "dbo.bronze_shipments",
        "key_column":    ["order_id", "line_no"],
        "source_file_label": "Shipments01-03.csv (dynamic)",
    },
    {
        "source_name":   "Budget",
        "custom_reader": read_budget,                # bypasses read_source() —
                                                       # handles title row and
                                                       # forward-fill only.
                                                       # Unpivot moved to Silver —
                                                       # Bronze never changes grain.
        "target_table":  "dbo.bronze_budget",
        "key_column":    ["office", "employee_id"],
        "source_file_label": "Budget.xlsx",
    },
]

print(f"Configured {len(FACT_SOURCES)} fact sources:")
for s in FACT_SOURCES:
    reader_type = "custom_reader" if "custom_reader" in s else s.get("file_type", "?")
    print(f"  - {s['source_name']:<15} ({reader_type:<14}) -> {s['target_table']}  key={s['key_column']}")

# =============================================================================
# CODE CELL 7 (notebook cell index 7)
# =============================================================================
# =============================================================================
# CELL 5 -- ORCHESTRATOR (per-source try/except, MERGE strategy)
# =============================================================================
# Purpose : Run the full pipeline (read -> normalize -> write -> validate ->
#           audit log) for each of the 4 fact sources.
#
# Row count cross-check: Orders and Order_Details use the generic
# count_source_rows_independent(). Shipments uses
# count_shipments_rows_independent() (sums all dynamically discovered
# Shipments*.csv files). Budget is intentionally excluded -- its final row
# count (33, post-unpivot) is not comparable to a raw file row count by
# design (see 00_bronze_shared_functions Cell 9 for the full explanation).
# =============================================================================

run_results = []

for source in FACT_SOURCES:
    source_name   = source["source_name"]
    status        = "SUCCESS"
    error_message = None
    rows_read     = None
    write_metrics = None
    validation    = None
    row_count_mismatch = False
    expected_rows = None
    row_count_check_applicable = True

    print(f"\n{'#'*70}")
    print(f"# PROCESSING FACT: {source_name}")
    print(f"{'#'*70}")

    try:
        if "custom_reader" in source:
            df_raw = source["custom_reader"]()
            source_file_label = source["source_file_label"]
        else:
            df_raw = read_source(
                file_name=source["file_name"],
                file_type=source["file_type"]
            )
            source_file_label = source["file_name"]

        rows_read = df_raw.count()

        if source_name == "Shipments":
            expected_rows = count_shipments_rows_independent()
        elif source_name == "Budget":
            row_count_check_applicable = False
        else:
            expected_rows = count_source_rows_independent(
                file_name=source["file_name"],
                file_type=source["file_type"]
            )

        if row_count_check_applicable:
            if rows_read != expected_rows:
                row_count_mismatch = True
                print(f"  WARNING: ROW COUNT MISMATCH -- Spark read {rows_read:,} rows, "
                      f"independent pandas count expected {expected_rows:,}")
            else:
                print(f"  Row count cross-check passed: {rows_read:,} rows (Spark == pandas)")
        else:
            print(f"  Row count cross-check skipped for {source_name} (post-transform grain)")

        df_normalized = normalize_schema(
            df_raw=df_raw,
            source_file_name=source_file_label,
            source_system=SOURCE_SYSTEM
        )

        write_metrics = write_bronze_merge(
            df_normalized=df_normalized,
            target_table=source["target_table"],
            merge_key=source["key_column"]
        )

        validation = validate_bronze(
            target_table=source["target_table"],
            key_column=source["key_column"]
        )

        status = "SUCCESS" if not row_count_mismatch else "WARNING"

    except Exception as e:
        status        = "FAILED"
        error_message = f"{type(e).__name__}: {e}"
        print(f"\n{'='*60}")
        print(f"  FAILED processing {source_name}")
        print(f"{'='*60}")
        traceback.print_exc()

    finally:
        write_audit_log(
            run_id=RUN_ID,
            table_name=source["target_table"],
            source_file_name=source.get("source_file_label", source.get("file_name")),
            run_timestamp=RUN_TIMESTAMP,
            rows_read=rows_read,
            rows_inserted=write_metrics["rows_inserted"] if write_metrics else None,
            rows_updated=write_metrics["rows_updated"] if write_metrics else None,
            rows_total=write_metrics["rows_total_after_write"] if write_metrics else None,
            status=status,
            error_message=error_message
        )

    run_results.append({
        "source_name":    source_name,
        "target_table":   source["target_table"],
        "status":         status,
        "rows_read":      rows_read,
        "rows_inserted":  write_metrics["rows_inserted"] if write_metrics else None,
        "rows_updated":   write_metrics["rows_updated"] if write_metrics else None,
        "rows_total":     write_metrics["rows_total_after_write"] if write_metrics else None,
        "duplicate_keys": validation["duplicate_key_count"] if validation else None,
        "row_count_mismatch": row_count_mismatch,
        "expected_rows": expected_rows,
        "row_count_check_applicable": row_count_check_applicable,
        "error_message":  error_message,
    })

print(f"\n{'='*110}")
print(f"  RUN SUMMARY -- 02b_bronze_facts")
print(f"{'='*110}")
print(f"  Run ID : {RUN_ID}")
print(f"{'='*110}")
print(f"  {'Source':<15} {'Status':<10} {'Read':>8} {'Inserted':>9} {'Updated':>8} {'Total':>8} {'DupKeys':>8}  Error")
print(f"  {'-'*15} {'-'*10} {'-'*8} {'-'*9} {'-'*8} {'-'*8} {'-'*8}  {'-'*30}")
for r in run_results:
    error_display = r["error_message"] if r["error_message"] else ""
    def fmt(v): return f"{v:,}" if isinstance(v, int) else (v if v is not None else "-")
    print(f"  {r['source_name']:<15} {r['status']:<10} "
          f"{fmt(r['rows_read']):>8} {fmt(r['rows_inserted']):>9} "
          f"{fmt(r['rows_updated']):>8} {fmt(r['rows_total']):>8} "
          f"{fmt(r['duplicate_keys']):>8}  {error_display}")

failed_count = sum(1 for r in run_results if r["status"] == "FAILED")
warning_count = sum(1 for r in run_results if r["status"] == "WARNING")
print(f"\n{'='*110}")
if failed_count == 0 and warning_count == 0:
    print(f"  All {len(run_results)} fact sources processed successfully.")
else:
    if warning_count > 0:
        print(f"  WARNING: {warning_count} source(s) with row count mismatch -- review above.")
    if failed_count > 0:
        print(f"  WARNING: {failed_count} of {len(run_results)} sources FAILED -- review errors above.")
print(f"{'='*110}")

print(f"\n{'='*95}")
print(f"  ROW COUNT VALIDATION SUMMARY -- Spark (read_source) vs pandas (independent)")
print(f"{'='*95}")
print(f"  {'Source':<15} {'Spark read':>12} {'Pandas expected':>16} {'Match':>10}")
print(f"  {'-'*15} {'-'*12} {'-'*16} {'-'*10}")
for r in run_results:
    spark_val = r["rows_read"] if r["rows_read"] is not None else "-"
    if not r["row_count_check_applicable"]:
        match_icon = "N/A"
        expected_val = "-"
    else:
        expected_val = r.get("expected_rows") if r.get("expected_rows") is not None else "-"
        match_icon = "✅" if not r["row_count_mismatch"] else "🚨"
    print(f"  {r['source_name']:<15} {spark_val:>12} {expected_val:>16} {match_icon:>10}")

applicable = [r for r in run_results if r["row_count_check_applicable"]]
mismatches = sum(1 for r in applicable if r["row_count_mismatch"])
print(f"{'='*95}")
if mismatches == 0:
    print(f"  All {len(applicable)} applicable sources: Spark and pandas row counts match exactly.")
    print(f"  (Budget excluded -- see Cell 9 notes on the unpivot transformation.)")
else:
    print(f"  WARNING: {mismatches} source(s) show a row count mismatch.")
print(f"{'='*95}")
