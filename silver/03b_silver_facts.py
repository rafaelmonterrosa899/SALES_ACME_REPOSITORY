# =============================================================================
# NOTEBOOK: 03b_silver_facts
# Consolidated from Fabric notebook (.ipynb) into a single .py file for
# version control in GitHub. Cell boundaries are preserved as comment markers
# so the notebook structure remains traceable in source control.
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# <h1 style="color:#C00000; font-weight:bold; font-family:'EB Garamond', Garamond, serif; letter-spacing:1.5px; border-bottom: 2px solid #C00000; padding-bottom:6px;">03b_silver_facts</h1>

# =============================================================================
# CODE CELL 1 (notebook cell index 1)
# =============================================================================

# =============================================================================
# CELL 0 — Header & pipeline overview
# =============================================================================
# Notebook: 03b_silver_facts
# Purpose : Transform all 4 Bronze fact tables into cleansed Silver facts.
#           Every transformation rule below is now CONFIRMED by the
#           business (David, 2026-07-15) — no longer documented
#           assumptions, actual verified rules.
#
# Sources processed (4):
#   1. dbo.bronze_orders          -> silver.fact_orders
#      - order_date cast to real date
#      - employee_id null -> -1 ("Unknown Employee", using the placeholder
#        already added to silver.dim_employee)
#      - total_order column DROPPED (confirmed unusable — 100% zero;
#        David confirmed: "calculate this from the Order detail" — the
#        real order value is computed in Gold, joining fact_orders to
#        fact_order_details, not stored redundantly in Silver)
#
#   2. dbo.bronze_order_details   -> silver.fact_order_details
#      - type casts only, no business logic changes needed
#
#   3. dbo.bronze_shipments       -> silver.fact_shipments
#      - CONFIRMED by David: "the year was supposed to be updated + 9
#        years... treat this as data that needs to be cleansed/corrected
#        before using it." shipment_date is corrected by +9 years
#        (108 months, to handle leap years consistently). The original
#        uncorrected value is preserved as shipment_date_original for
#        traceability — the correction is visible, not silently applied.
#      - CONFIRMED by David: "treat as duplicates to reduce to distinct
#        rows" — the 194 (order_id, line_no) pairs duplicated across
#        Shipments02.csv/Shipments03.csv are deduplicated here (same
#        row_hash -> keep one).
#
#   4. dbo.bronze_budget          -> silver.fact_budget
#      - Unpivot year columns (2017..2021) into (year, budget_amount) rows
#        — this is the grain-changing transformation intentionally NOT
#        done in Bronze (Bronze never changes grain — see Bronze design
#        notes on Budget).
#
# Author  : Rafael
# Date    : 2026-07-15
# =============================================================================

# =============================================================================
# CODE CELL 2 (notebook cell index 2)
# =============================================================================
# NOTE: Fabric magic command, not valid standalone Python syntax.
# %run 00_silver_shared_functions

# =============================================================================
# CODE CELL 3 (notebook cell index 3)
# =============================================================================
# =============================================================================
# CELL 2 — Configuration & run identity
# =============================================================================

RUN_ID        = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.now(timezone.utc)

print(f"Run ID         : {RUN_ID}")
print(f"Run timestamp  : {RUN_TIMESTAMP}")

# =============================================================================
# CODE CELL 4 (notebook cell index 4)
# =============================================================================
# =============================================================================
# CELL 3 — Transformation functions
# =============================================================================
# Order_Details needs no dedicated function (type casts only) — it uses the
# generic read_bronze() lambda directly in Cell 4's config, same pattern as
# the simple dimensions in 03a.

# -----------------------------------------------------------------------------
# 3.1 — transform_orders()
# -----------------------------------------------------------------------------
def transform_orders():
    """
    Cast order_date to a real date. Null employee_id -> -1 ("Unknown
    Employee", using the placeholder already in silver.dim_employee).
    Drops total_order entirely — CONFIRMED unusable (100% zero); Colette
    confirmed the real order value should be calculated from Order_Details,
    which happens in Gold (join fact_orders + fact_order_details), not
    stored redundantly here.
    """
    df = read_bronze("dbo.bronze_orders")

    df_fixed = df.withColumn(
        "order_date", F.to_date(col("order_date"), "M/d/yyyy")
    ).withColumn(
        "employee_id", F.coalesce(col("employee_id").cast("int"), F.lit(-1))
    ).drop("total_order")

    return df_fixed


# -----------------------------------------------------------------------------
# 3.2 — transform_shipments()
# -----------------------------------------------------------------------------
def transform_shipments():
    """
    CONFIRMED by Colette (2026-07-15):
      1. "treat as duplicates to reduce to distinct rows" -> deduplicate
         on (order_id, line_no) — the 194 cross-file duplicates from
         Shipments02.csv/Shipments03.csv collapse to one row each.
      2. "the year was supposed to be updated + 9 years... treat this as
         data that needs to be cleansed/corrected" -> shipment_date is
         corrected by adding 108 months (9 years, leap-year safe). The
         uncorrected value is preserved as shipment_date_original so the
         correction stays visible and auditable, not silently applied.
    """
    df = read_bronze("dbo.bronze_shipments")

    rows_before = df.count()
    df = df.dropDuplicates(["order_id", "line_no"])
    rows_after = df.count()
    print(f"Deduplicated {rows_before - rows_after:,} duplicate (order_id, line_no) rows "
          f"({rows_before:,} -> {rows_after:,})")

    df_fixed = df.withColumn(
        "shipment_date_original", F.to_date(col("shipment_date"), "M/d/yyyy")
    ).withColumn(
        "shipment_date", F.add_months(col("shipment_date_original"), 108)
    )

    return df_fixed


# -----------------------------------------------------------------------------
# 3.3 — transform_budget()
# -----------------------------------------------------------------------------
def transform_budget():
    """
    Unpivot year columns (2017..2021) into (year, budget_amount) rows —
    the grain-changing transformation deliberately NOT done in Bronze
    (Bronze never changes grain — see Bronze design notes on Budget).
    Rows where budget_amount is null are dropped (a year with no budgeted
    value for that office/employee is an absence of a fact, not a zero).
    """
    df = read_bronze("dbo.bronze_budget")

    year_columns = [c for c in df.columns if c.isdigit()]
    print(f"Year columns detected for unpivot: {year_columns}")

    lineage_cols = ["bronze_created_at", "bronze_updated_at",
                     "bronze_source_file_name", "bronze_source_system"]
    lineage_cols = [c for c in lineage_cols if c in df.columns]

    stack_expr = "stack({}, {}) as (year, budget_amount)".format(
        len(year_columns),
        ", ".join([f"'{y}', `{y}`" for y in year_columns])
    )

    df_unpivoted = df.select(
        "office", "employee_id", *lineage_cols, F.expr(stack_expr)
    ).filter(col("budget_amount").isNotNull())

    df_unpivoted = df_unpivoted.withColumn(
        "office", col("office").cast("int")
    ).withColumn(
        "employee_id", col("employee_id").cast("int")
    ).withColumn(
        "year", col("year").cast("int")
    ).withColumn(
        "budget_amount", col("budget_amount").cast("double")
    )

    # Reorder: business columns first, lineage columns last
    df_unpivoted = df_unpivoted.select(
        "office", "employee_id", "year", "budget_amount", *lineage_cols
    )

    print(f"Rows after unpivot: {df_unpivoted.count():,}")

    return df_unpivoted

# =============================================================================
# CODE CELL 5 (notebook cell index 5)
# =============================================================================
# =============================================================================
# CELL 4 — Silver fact sources configuration
# =============================================================================

SILVER_FACT_SOURCES = [
    {
        "source_name":   "Orders",
        "transform_fn":  transform_orders,
        "target_table":  "silver.fact_orders",
        "key_column":    "order_id",
    },
    {
        "source_name":   "Order_Details",
        "transform_fn":  lambda: read_bronze("dbo.bronze_order_details"),
        "target_table":  "silver.fact_order_details",
        "key_column":    ["order_id", "line_no"],
    },
    {
        "source_name":   "Shipments",
        "transform_fn":  transform_shipments,
        "target_table":  "silver.fact_shipments",
        "key_column":    ["order_id", "line_no"],
    },
    {
        "source_name":   "Budget",
        "transform_fn":  transform_budget,
        "target_table":  "silver.fact_budget",
        "key_column":    ["office", "employee_id", "year"],
    },
]

print(f"Configured {len(SILVER_FACT_SOURCES)} Silver fact sources:")
for s in SILVER_FACT_SOURCES:
    print(f"  - {s['source_name']:<15} -> {s['target_table']}  key={s['key_column']}")

# =============================================================================
# CODE CELL 6 (notebook cell index 6)
# =============================================================================
# =============================================================================
# CELL 5 -- ORCHESTRATOR (per-source try/except)
# =============================================================================
# Purpose : Run transform -> write -> validate -> audit log for each of the
#           4 Silver fact sources. Each source runs inside its OWN
#           try/except block -- a failure on one source does not stop the
#           other 3.
# =============================================================================

run_results = []

for source in SILVER_FACT_SOURCES:
    source_name   = source["source_name"]
    status        = "SUCCESS"
    error_message = None
    rows_read     = None
    write_metrics = None
    validation    = None

    print(f"\n{'#'*70}")
    print(f"# PROCESSING SILVER FACT: {source_name}")
    print(f"{'#'*70}")

    try:
        df_transformed = source["transform_fn"]()
        rows_read = df_transformed.count()

        write_metrics = write_silver_merge(
            df_transformed=df_transformed,
            target_table=source["target_table"],
            merge_key=source["key_column"]
        )

        validation = validate_silver(
            target_table=source["target_table"],
            key_column=source["key_column"]
        )

        status = "SUCCESS"

    except Exception as e:
        status        = "FAILED"
        error_message = f"{type(e).__name__}: {e}"
        print(f"\n{'='*60}")
        print(f"  ❌ FAILED processing {source_name}")
        print(f"{'='*60}")
        traceback.print_exc()

    finally:
        write_audit_log(
            run_id=RUN_ID,
            table_name=source["target_table"],
            source_file_name=f"transform_fn:{source['source_name']}",
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
        "rows_inserted":  write_metrics["rows_inserted"] if write_metrics else None,
        "rows_updated":   write_metrics["rows_updated"] if write_metrics else None,
        "rows_written":   write_metrics["rows_total_after_write"] if write_metrics else None,
        "duplicate_keys": validation["duplicate_key_count"] if validation else None,
        "error_message":  error_message,
    })

print(f"\n{'='*110}")
print(f"  RUN SUMMARY -- 03b_silver_facts")
print(f"{'='*110}")
print(f"  Run ID : {RUN_ID}")
print(f"{'='*110}")
print(f"  {'Source':<15} {'Status':<10} {'Inserted':>9} {'Updated':>9} {'Total':>9} {'DupKeys':>9}  Error")
print(f"  {'-'*15} {'-'*10} {'-'*9} {'-'*9} {'-'*9} {'-'*9}  {'-'*30}")
for r in run_results:
    error_display = r["error_message"] if r["error_message"] else ""
    def fmt(v): return f"{v:,}" if isinstance(v, int) else (v if v is not None else "-")
    print(f"  {r['source_name']:<15} {r['status']:<10} "
          f"{fmt(r['rows_inserted']):>9} {fmt(r['rows_updated']):>9} "
          f"{fmt(r['rows_written']):>9} {fmt(r['duplicate_keys']):>9}  {error_display}")

failed_count = sum(1 for r in run_results if r["status"] == "FAILED")
print(f"\n{'='*100}")
if failed_count == 0:
    print(f"  ✅ All {len(run_results)} Silver fact sources processed successfully.")
else:
    print(f"  ⚠️  {failed_count} of {len(run_results)} sources FAILED — review errors above.")
print(f"{'='*100}")
