# =============================================================================
# NOTEBOOK: 03a_silver_dimensions
# Consolidated from Fabric notebook (.ipynb) into a single .py file for
# version control in GitHub. Cell boundaries are preserved as comment markers
# so the notebook structure remains traceable in source control.
# =============================================================================

# -----------------------------------------------------------------------------
# MARKDOWN
# -----------------------------------------------------------------------------
# <h1 style="color:#C00000; font-weight:bold; font-family:'EB Garamond', Garamond, serif; letter-spacing:1.5px; border-bottom: 2px solid #C00000; padding-bottom:6px;">03a_silver_dimensions</h1>

# =============================================================================
# CODE CELL 1 (notebook cell index 1)
# =============================================================================
# =============================================================================
# CELL 0 — Header & pipeline overview
# =============================================================================
# Notebook: 03a_silver_dimensions
# Purpose : Transform all 8 Bronze dimension tables into cleansed, conformed
#           Silver dimensions. Every transformation is justified by a
#           specific EDA finding — nothing is cleaned "just in case."
#
# Sources processed (8):
#   1. dbo.bronze_categories  -> silver.dim_category   (type cast only)
#   2. dbo.bronze_products    -> silver.dim_product     (type cast only)
#   3. dbo.bronze_customers   -> silver.dim_customer    (encoding cleanup,
#                                                         state_province null
#                                                         -> "N/A (International)")
#   4. dbo.bronze_suppliers   -> silver.dim_supplier    (type cast only)
#   5. dbo.bronze_divisions   -> silver.dim_division    (resolve DivisionID=2
#                                                         duplicate — see below)
#   6. dbo.bronze_employees   -> silver.dim_employee    (add "Unknown Employee"
#                                                         placeholder, emp_id=-1)
#   7. dbo.bronze_offices     -> silver.dim_office      (type cast only)
#   8. dbo.bronze_shippers    -> silver.dim_shipper     (add placeholders for
#                                                         orphan ShipperID 4, 5)
#
# Divisions duplicate resolution (pending business confirmation from Colette):
#   DivisionID=2 originally maps to BOTH "North America" and "Central America"
#   in the source file. We keep DivisionID=2 = "North America" (the first
#   occurrence in the file) and reassign "Central America" to a new ID (6,
#   the next available), renamed to "Central America — Pending Confirmation"
#   so the ambiguity stays visible until the business confirms which mapping
#   is correct. This is a documented ASSUMPTION, not a verified fact — easy
#   to reverse once Colette responds.
#
# Unknown-member placeholders (never invent business detail — see
# add_unknown_member() in 00_silver_shared_functions):
#   - silver.dim_employee gets emp_id = -1, "Unknown Employee" — used later
#     when cleaning Orders.employee_id nulls in 03b_silver_facts.
#   - silver.dim_shipper gets shipper_id = 4 and 5, both labeled
#     "Unknown Shipper — Pending Confirmation" — these IDs are referenced by
#     real Orders/Shipments rows but have no master record in Shippers.csv.
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
# CELL 3 — Special transformation functions
# =============================================================================
# Each of these breaks the generic "just cast types" pattern for a specific,
# EDA-documented reason. Sources needing only type casts don't get a
# dedicated function — see the generic path in Cell 4's orchestrator.

# -----------------------------------------------------------------------------
# 3.1 — transform_divisions()
# -----------------------------------------------------------------------------

def transform_divisions():
    """
    Resolve the DivisionID=2 duplicate found during EDA (maps to both
    "North America" and "Central America" in the source file).

    CONFIRMED by Colette (2026-07-15): "Map 'North America' as the
    default." DivisionID=2 is kept as "North America" (now a verified
    business rule, not an assumption). "Central America" is reassigned to
    a new ID (6, the next available) — a real, permanent division, not a
    placeholder, since it's a legitimate distinct division that simply
    needs its own ID.
    """
    df = read_bronze("dbo.bronze_divisions")

    df_fixed = df.withColumn(
        "division_id",
        F.when(
            (col("division_id") == 2) & (col("division_name") == "Central America"),
            F.lit(6)
        ).otherwise(col("division_id"))
    )

    print("── Divisions after duplicate resolution (confirmed by Colette) ──")
    display(df_fixed.orderBy("division_id"))

    return df_fixed


# -----------------------------------------------------------------------------
# 3.2 — transform_customers()
# -----------------------------------------------------------------------------
def transform_customers():
    """
    Cast birth_date to a real date type, and replace NULL state_province
    with an explicit "N/A (International)" marker — per EDA finding,
    82.6% of customers are international and don't have a US/Canada-style
    state/province, so NULL here means "not applicable," not "missing data."
    """
    df = read_bronze("dbo.bronze_customers")

    df_fixed = df.withColumn(
        "birth_date", F.to_date(col("birth_date"), "M/d/yyyy")
    ).withColumn(
        "state_province",
        F.when(col("state_province").isNull(), F.lit("N/A (International)"))
         .otherwise(col("state_province"))
    )

    return df_fixed


# -----------------------------------------------------------------------------
# 3.3 — transform_shippers()
# -----------------------------------------------------------------------------
def transform_shippers():
    """
    Add placeholder rows for ShipperID 4 and 5 — referenced by real
    Orders/Shipments rows, but with no master record in Shippers.csv
    (confirmed orphan FK via EDA referential integrity check).
    """
    df = read_bronze("dbo.bronze_shippers")

    df_fixed = add_unknown_member(df, [
        {"shipper_id": 4, "company_name": "Unknown Shipper — Pending Confirmation"},
        {"shipper_id": 5, "company_name": "Unknown Shipper — Pending Confirmation"},
    ])

    return df_fixed


# -----------------------------------------------------------------------------
# 3.4 — transform_employees()
# -----------------------------------------------------------------------------
def transform_employees():
    """
    Cast types (bronze_employees has ALL-STRING columns — a side effect of
    the Excel ingestion path in Bronze, where pandas.astype(str) was used
    to avoid a NaN-as-string bug — see Bronze design notes). Also adds an
    "Unknown Employee" placeholder (emp_id = -1) — used later in
    03b_silver_facts when cleaning the 5 Orders rows with a null
    EmployeeID (isolated data-entry gaps, no shared pattern found in EDA).
    """
    df = read_bronze("dbo.bronze_employees")

    df_fixed = df.withColumn(
        "emp_id", col("emp_id").cast("int")
    ).withColumn(
        "hire_date", F.to_date(col("hire_date"), "yyyy-MM-dd")
    ).withColumn(
        "office", col("office").cast("int")
    ).withColumn(
        "extension", col("extension").cast("int")
    ).withColumn(
        "reports_to", col("reports_to").cast("int")
    ).withColumn(
        "year_salary", col("year_salary").cast("int")
    )

    df_fixed = add_unknown_member(df_fixed, [
        {"emp_id": -1, "last_name": "Unknown", "first_name": "Employee"},
    ])

    return df_fixed


# -----------------------------------------------------------------------------
# 3.5 — transform_offices()
# -----------------------------------------------------------------------------
def transform_offices():
    """
    Cast types (bronze_offices has ALL-STRING columns — same Excel
    ingestion side effect as Employees). No placeholder needed here —
    EDA confirmed Employees.office has no orphans against this dimension.
    """
    df = read_bronze("dbo.bronze_offices")

    df_fixed = df.withColumn(
        "office", col("office").cast("int")
    )

    return df_fixed

# =============================================================================
# CODE CELL 5 (notebook cell index 5)
# =============================================================================
# =============================================================================
# CELL 4 — Silver dimension sources configuration
# =============================================================================
# Each entry maps a Bronze source to its Silver target, key, and transform
# function. Sources needing only a straight read (no casts, no business
# logic) use the generic lambda; sources with EDA-documented issues use
# their dedicated transform_*() function from Cell 3.

SILVER_DIMENSION_SOURCES = [
    {
        "source_name":   "Category",
        "transform_fn":  lambda: read_bronze("dbo.bronze_categories"),
        "target_table":  "silver.dim_category",
        "key_column":    "category_id",
    },
    {
        "source_name":   "Product",
        "transform_fn":  lambda: read_bronze("dbo.bronze_products"),
        "target_table":  "silver.dim_product",
        "key_column":    "product_id",
    },
    {
        "source_name":   "Customer",
        "transform_fn":  transform_customers,
        "target_table":  "silver.dim_customer",
        "key_column":    "customer_id",
    },
    {
        "source_name":   "Supplier",
        "transform_fn":  lambda: read_bronze("dbo.bronze_suppliers"),
        "target_table":  "silver.dim_supplier",
        "key_column":    "supplier_id",
    },
    {
        "source_name":   "Division",
        "transform_fn":  transform_divisions,
        "target_table":  "silver.dim_division",
        "key_column":    "division_id",
    },
    {
        "source_name":   "Employee",
        "transform_fn":  transform_employees,
        "target_table":  "silver.dim_employee",
        "key_column":    "emp_id",
    },
    {
        "source_name":   "Office",
        "transform_fn":  transform_offices,
        "target_table":  "silver.dim_office",
        "key_column":    "office",
    },
    {
        "source_name":   "Shipper",
        "transform_fn":  transform_shippers,
        "target_table":  "silver.dim_shipper",
        "key_column":    "shipper_id",
    },
]

print(f"Configured {len(SILVER_DIMENSION_SOURCES)} Silver dimension sources:")
for s in SILVER_DIMENSION_SOURCES:
    print(f"  - {s['source_name']:<10} -> {s['target_table']}  key={s['key_column']}")

# =============================================================================
# CODE CELL 6 (notebook cell index 6)
# =============================================================================
# =============================================================================
# CELL 5 -- ORCHESTRATOR (per-source try/except)
# =============================================================================
# Purpose : Run transform -> write -> validate -> audit log for each of the
#           8 Silver dimensions. Each source runs inside its OWN try/except
#           block -- a failure on one source does not stop the other 7.
# =============================================================================

run_results = []

for source in SILVER_DIMENSION_SOURCES:
    source_name   = source["source_name"]
    status        = "SUCCESS"
    error_message = None
    rows_read     = None
    write_metrics = None
    validation    = None

    print(f"\n{'#'*70}")
    print(f"# PROCESSING SILVER DIMENSION: {source_name}")
    print(f"{'#'*70}")

    try:
        # 1. Transform (calls the appropriate transform_fn, which internally
        #    calls read_bronze() and applies any needed business logic)
        df_transformed = source["transform_fn"]()
        rows_read = df_transformed.count()

        # 2. Write (MERGE strategy, same idempotent pattern as Bronze)
        write_metrics = write_silver_merge(
            df_transformed=df_transformed,
            target_table=source["target_table"],
            merge_key=source["key_column"]
        )

        # 3. Validate
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
        # 4. Audit log -- always written, success or failure, same
        # governance.audit_ingestion_log table used by Bronze, so the
        # audit trail spans both layers in one place
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

# -----------------------------------------------------------------------------
# Final summary
# -----------------------------------------------------------------------------
print(f"\n{'='*110}")
print(f"  RUN SUMMARY -- 03a_silver_dimensions")
print(f"{'='*110}")
print(f"  Run ID : {RUN_ID}")
print(f"{'='*110}")
print(f"  {'Source':<12} {'Status':<10} {'Inserted':>9} {'Updated':>9} {'Total':>9} {'DupKeys':>9}  Error")
print(f"  {'-'*12} {'-'*10} {'-'*9} {'-'*9} {'-'*9} {'-'*9}  {'-'*30}")
for r in run_results:
    error_display = r["error_message"] if r["error_message"] else ""
    def fmt(v): return f"{v:,}" if isinstance(v, int) else (v if v is not None else "-")
    print(f"  {r['source_name']:<12} {r['status']:<10} "
          f"{fmt(r['rows_inserted']):>9} {fmt(r['rows_updated']):>9} "
          f"{fmt(r['rows_written']):>9} {fmt(r['duplicate_keys']):>9}  {error_display}")

failed_count = sum(1 for r in run_results if r["status"] == "FAILED")
print(f"\n{'='*100}")
if failed_count == 0:
    print(f"  ✅ All {len(run_results)} Silver dimension sources processed successfully.")
else:
    print(f"  ⚠️  {failed_count} of {len(run_results)} sources FAILED — review errors above.")
print(f"{'='*100}")
