# =============================================================================
# RECOMMENDATION 1 -- Shipment date source-system bug
# =============================================================================

print(f"{'='*90}")
print(f"  RECOMMENDATION #1 -- Shipment date generation bug in the source system")
print(f"{'='*90}")
print("""
  FINDING
  100% of shipment records (17,226 rows) showed a ShipmentDate exactly ~8.95
  years BEFORE the matching OrderDate -- with an unusually tight standard
  deviation (~2.9 days), pointing to a systematic date-shift bug rather than
  random data corruption. Colette confirmed this is a known issue: "the year
  was supposed to be updated + 9 years."

  BUSINESS IMPACT
  Delivery performance (Order Date vs Shipment Date) cannot be analyzed at
  face value without this correction -- every historical delivery-time
  report built on raw ShipmentDate would be wrong by ~9 years.

  ACTION TAKEN IN THIS POC
  1. Corrected +9 years (108 months) in Silver. Original value preserved as
     shipment_date_original for full auditability.
  2. Added an automated Data Quality check (governance.dq_shipments, rule
     SHIPMENT_BEFORE_ORDER) that alerts if ShipmentDate < OrderDate for ANY
     future load -- this exact pattern will never reach Gold silently again.
     The check is wired into the Fabric Data Pipeline: it runs after every
     Gold load and triggers an automatic email alert if it (or any other DQ
     rule) fires.

  RECOMMENDATION FOR ACME
  1. Escalate to the team/vendor responsible for the Shipments export --
     this is very likely a recurring bug affecting NEW shipments too, not
     just historical data.
  2. Until the source system is fixed, apply the same +9 year correction to
     any new incoming Shipments file before loading -- flag it as a known,
     temporary workaround in the pipeline documentation.
""")
print(f"{'='*90}")

# Note: governance.dq_shipments lives in the SQL Analytics Endpoint's T-SQL
# catalog, not the Spark/Lakehouse catalog -- it cannot be queried via
# spark.sql() from a notebook. The check logic is reproduced natively in
# PySpark below instead, reading directly from the Silver Delta tables.
print("-- Example: original vs. corrected shipment_date, real rows --")
display(
    spark.table("silver.fact_shipments")
    .select("order_id", "line_no", "shipment_date_original", "shipment_date")
    .orderBy("order_id")
    .limit(5)
)

print("-- Proof the DQ check logic finds 0 SHIPMENT_BEFORE_ORDER violations (native PySpark) --")
violation_count = (
    spark.table("silver.fact_shipments").alias("sh")
    .join(spark.table("silver.fact_orders").alias("o"), "order_id")
    .filter(col("sh.shipment_date") < col("o.order_date"))
    .count()
)
print(f"SHIPMENT_BEFORE_ORDER violations found: {violation_count}")


# =============================================================================
# RECOMMENDATION 2 -- Orphan Shipper master data
# =============================================================================

df_shippers_used = spark.table("gold.fact_sales").select("shipper_id").distinct()
orphan_check = (
    df_shippers_used
    .join(spark.table("gold.dim_shipper").select("shipper_id", "company_name"), "shipper_id", "left")
)

print(f"{'='*90}")
print(f"  RECOMMENDATION #2 -- Missing Shipper master data (ShipperID 4, 5)")
print(f"{'='*90}")
print("""
  FINDING
  Orders and Shipments reference ShipperID 4 and 5 in real transactions,
  but Shippers.csv only defines 3 shippers (IDs 1-3). Colette confirmed:
  "Some shippers may not be on the reference file. They can be flagged as
  'Unknown Shipper'."

  BUSINESS IMPACT
  Without resolution, any report joining sales to shipper (e.g. "which
  carrier moves the most volume") would either silently drop these orders
  (INNER JOIN) or show them as unexplained blanks -- in both cases, real
  shipping activity becomes invisible to analysis.

  ACTION TAKEN IN THIS POC
  1. Added explicit placeholder rows to gold.dim_shipper for IDs 4 and 5,
     labeled "Unknown Shipper - Pending Confirmation" -- every sales record
     remains joinable, and the gap stays visible rather than hidden.
  2. Added a referential integrity check to the Data Quality view
     (governance.dq_orders and governance.dq_shipments, rule
     ORPHAN_SHIPPER_ID) so any FUTURE orphan ShipperID is caught
     immediately, not discovered months later during a reporting exercise.
     Both checks currently return 0 rows, confirming the placeholder fix
     closed the gap.

  RECOMMENDATION FOR ACME
  1. Confirm with logistics/procurement whether shippers 4 and 5 are
     active carriers missing from the master file, or a data entry error
     (e.g. wrong ID typed on those orders).
  2. Once confirmed, update Shippers.csv (or its source system) with the
     real carrier names/details -- the pipeline will pick up the fix
     automatically on the next run (MERGE will update the placeholder rows).
""")
print(f"{'='*90}")

print("-- Shippers referenced in Gold, with dimension coverage --")
display(orphan_check.orderBy("shipper_id"))

print("-- Example: real orders using the placeholder shippers --")
display(
    spark.table("gold.fact_sales")
    .filter(col("shipper_id").isin([4, 5]))
    .join(spark.table("gold.dim_shipper"), "shipper_id")
    .select("order_id", "line_no", "shipper_id", "company_name", "extended_amount")
    .orderBy("order_id")
    .limit(5)
)


# =============================================================================
# RECOMMENDATION 3 -- Discount policy (25% tier)
# =============================================================================

print(f"{'='*90}")
print(f"  RECOMMENDATION #3 -- 25% discount tier is the only net-negative-margin tier")
print(f"{'='*90}")
print("""
  FINDING
  Full net-margin picture by discount tier (not just losing lines):

  0% discount tier:  10,429 lines, 457 losing money, net margin +$1,745,676.39
                      (losing lines are <0.5% of that tier's total margin --
                      statistical noise, not a policy problem)

  25% discount tier:  1,205 lines, 867 losing money (72% of transactions at
                      this discount), net margin -$55,889.10 -- the ONLY
                      discount tier with a NEGATIVE net margin out of 12
                      tiers analyzed.

  BUSINESS IMPACT
  Every order sold at 25% discount is, on aggregate, destroying value for
  ACME -- not an isolated set of unlucky products, but a structural problem
  with that specific discount level.

  ACTION TAKEN IN THIS POC
  Added a Data Quality check (governance.dq_order_details, rule
  NEGATIVE_MARGIN) that flags every order line with negative margin,
  independent of discount tier, so this pattern (and any future one like
  it) surfaces automatically after every load. Currently returns 2,772
  warnings across all discount tiers combined.

  RECOMMENDATION FOR ACME
  1. HIGH PRIORITY: Review/renegotiate the 25% discount policy -- it is the
     only real break-even point in the pricing structure. Identify which
     business contexts trigger this discount (e.g. bulk clearance, key
     accounts) that could be re-priced without losing the relationship.
  2. LOWER PRIORITY: Separately investigate base pricing/cost data for the
     small set of products losing money even at 0% discount -- likely a
     unit_cost or unit_price data issue rather than a discount policy issue.
  3. Do NOT recommend a blanket discount reduction across all tiers --
     0-20% discounts are net profitable and cutting them broadly would
     reduce revenue without addressing the actual problem.
""")
print(f"{'='*90}")

print("-- Example: real order lines losing money at 25% discount --")
display(
    spark.table("gold.fact_sales")
    .filter((col("discount") == 0.25) & (col("margin") < 0))
    .join(spark.table("gold.dim_product").select("product_id", "product_name"), "product_id")
    .select("order_id", "line_no", "product_name", "quantity", "unit_price", "discount", "margin")
    .orderBy("margin")
    .limit(5)
)

print("-- Full net-margin picture by discount tier (proof behind the recommendation) --")
display(
    spark.table("gold.fact_sales")
    .groupBy("discount")
    .agg(
        F.count("*").alias("total_lines"),
        F.sum(F.when(col("margin") < 0, 1).otherwise(0)).alias("losing_lines"),
        F.round(F.sum("margin"), 2).alias("net_margin"),
        F.round(F.sum("extended_amount"), 2).alias("net_revenue")
    )
    .orderBy("discount")
)


# =============================================================================
# RECOMMENDATION 4 -- employee_id default-value pattern (Shipments)
# =============================================================================

print(f"{'='*90}")
print(f"  RECOMMENDATION #4 -- Shipments.employee_id defaults to the company president")
print(f"{'='*90}")
print("""
  FINDING
  Shipments.employee_id disagrees with Orders.employee_id in 1,172 of
  17,032 order lines (6.9%). Of those 1,172 mismatches, 1,164 (99.3%) all
  point to the SAME employee_id (2 -- Erik Presley, President). That level
  of concentration is not consistent with a real business process (e.g. "a
  different employee handles shipping sometimes") -- it is the classic
  fingerprint of a source-system default/placeholder value used whenever
  the real shipping employee could not be captured.

  BUSINESS IMPACT
  Any report that uses Shipments.employee_id to measure sales performance,
  commissions, or workload by employee would silently attribute ~1,164
  transactions to the company president that he likely did not actually
  handle -- a material distortion for any performance or compensation
  analysis built on that field.

  ACTION TAKEN IN THIS POC
  1. gold.fact_sales.employee_id is sourced from Orders.employee_id (the
     actual salesperson), NOT Shipments.employee_id -- this was a
     deliberate design decision, not an oversight, made specifically to
     avoid this distortion.
  2. Added a Data Quality check (governance.dq_shipments, rule
     EMPLOYEE_ID_DEFAULT_PATTERN) that generically flags any employee_id
     responsible for a disproportionate share (>50%) of Orders/Shipments
     mismatches -- not hardcoded to emp_id=2, so it will catch any future
     occurrence of the same pattern with a different employee.
  3. Built gold.vw_shipment_employee_id_anomaly (SQL Endpoint) as a
     standalone, queryable view of the mismatch concentration for further
     investigation.

  RECOMMENDATION FOR ACME
  1. Confirm with the Shipments source-system owner whether employee_id is
     even meant to be populated at the line level, or whether it should be
     removed from that file entirely -- it appears to duplicate
     Orders.employee_id in 93% of cases and defaults incorrectly in the
     remaining 7%.
  2. Do NOT use Shipments.employee_id for any sales performance, margin,
     or commission reporting until the source system is fixed --
     gold.vw_margin_by_employee in this POC already avoids it for exactly
     this reason.
""")
print(f"{'='*90}")

# Note: governance.dq_shipments and gold.vw_shipment_employee_id_anomaly live
# in the SQL Analytics Endpoint's T-SQL catalog, not the Spark/Lakehouse
# catalog -- reproduced natively in PySpark below instead.
print("-- Example: real order lines where Shipments.employee_id differs from Orders.employee_id --")

df_mismatches = (
    spark.table("silver.fact_orders").alias("o")
    .join(spark.table("silver.fact_shipments").alias("sh"), "order_id")
    .filter(col("o.employee_id") != col("sh.employee_id"))
    .select(
        col("o.order_id"),
        col("sh.line_no"),
        col("o.employee_id").alias("order_employee_id"),
        col("sh.employee_id").alias("shipment_employee_id"),
    )
)

print(f"Total mismatches: {df_mismatches.count()}")
display(df_mismatches.orderBy("order_id").limit(5))

print("-- Concentration proof: mismatches grouped by shipment_employee_id (native PySpark) --")
display(
    df_mismatches
    .groupBy("shipment_employee_id")
    .count()
    .withColumnRenamed("count", "mismatch_count")
    .orderBy(col("mismatch_count").desc())
)


# =============================================================================
# RECOMMENDATION 5 -- Duplicate DivisionID in source master data
# =============================================================================

print(f"{'='*90}")
print(f"  RECOMMENDATION #5 -- Duplicate primary key in Divisions.csv")
print(f"{'='*90}")
print("""
  FINDING
  Divisions.csv defines DivisionID=2 twice, mapped to two different names:
  "North America" and "Central America" -- a primary key violation in the
  source file. No customer in the dataset actually belongs to a Central
  America country (Belize, Costa Rica, El Salvador, Guatemala, Honduras,
  Nicaragua, Panama are all absent from Customers.csv), so the ambiguity
  has zero impact on current reporting -- but the master data itself is
  broken and would misassign any future Central America customer to
  "North America" silently if left unresolved.

  BUSINESS IMPACT
  Any report grouped by Division carries a latent risk: a future customer
  correctly assigned DivisionID=2 could resolve to either name depending on
  which row the join happens to pick up, with no error raised.

  ACTION TAKEN IN THIS POC
  Colette confirmed via email: "Map 'North America' as the default."
  DivisionID=2 is kept as "North America"; "Central America" is reassigned
  to a new ID (6) in Silver, since it is a legitimate distinct division,
  not a placeholder to discard.

  RECOMMENDATION FOR ACME
  1. Fix Divisions.csv (or its source system) at the root: DivisionID must
     be enforced as a unique primary key going forward.
  2. If ACME expands into Central America commercially, the division
     already exists cleanly under ID 6 -- no further schema change needed.
""")
print(f"{'='*90}")

print("-- Example: the duplicate DivisionID as it appears in the raw source --")
display(
    spark.table("dbo.bronze_divisions")
    .select("division_id", "division_name")
    .orderBy("division_id")
)

print("-- Resolution applied in Silver: Central America reassigned to ID 6 --")
display(
    spark.table("silver.dim_division")
    .select("division_id", "division_name")
    .orderBy("division_id")
)


# =============================================================================
# RECOMMENDATION 6 -- Orders.TotalOrder always zero
# =============================================================================

print(f"{'='*90}")
print(f"  RECOMMENDATION #6 -- Orders.TotalOrder column is unusable (always 0)")
print(f"{'='*90}")
print("""
  FINDING
  TotalOrder in Orders.csv is 0 for all 6,571 rows, with no exceptions --
  it is not partially populated or occasionally wrong, it simply carries
  no data. The real order value has to be derived from Order_Details
  (Quantity x UnitPrice x (1-Discount)) instead.

  BUSINESS IMPACT
  Any report or downstream system that trusted Orders.TotalOrder as the
  source of order value would report every single order as worth $0 --
  a silent, total failure of the sales figures the business explicitly
  asked to analyze.

  ACTION TAKEN IN THIS POC
  gold.fact_sales never references Orders.TotalOrder at all. extended_amount
  is computed directly from Order_Details at the line-item grain
  (quantity * unit_price * (1 - discount)), which is both more accurate
  (line-level, not just order-level) and independent of this broken column.

  RECOMMENDATION FOR ACME
  1. Confirm with the Orders source-system owner whether TotalOrder was
     ever meant to be populated, or whether it's dead/legacy column that
     should be dropped from the export entirely to avoid future confusion.
  2. If a system-of-record order total IS needed going forward (as opposed
     to deriving it from Order_Details every time), it should be fixed at
     the source, not patched downstream.
""")
print(f"{'='*90}")

print("-- Example: TotalOrder was dropped during Silver normalization since it")
print("   carries no usable data -- it does not appear in silver.fact_orders --")
display(
    spark.table("silver.fact_orders")
    .select("order_id", "order_date")
    .limit(3)
)

print("-- The derived alternative already used in gold.fact_sales --")
display(
    spark.table("gold.fact_sales")
    .select("order_id", "line_no", "quantity", "unit_price", "discount", "extended_amount")
    .orderBy("order_id")
    .limit(5)
)


# =============================================================================
# RECOMMENDATION 7 -- Shipments02/03 overlap: 181 duplicates + 13 conflicts
# =============================================================================

print(f"{'='*90}")
print(f"  RECOMMENDATION #7 -- Shipments02.csv / Shipments03.csv overlap")
print(f"{'='*90}")
print("""
  FINDING
  194 (OrderID, LineNo) combinations appear in BOTH Shipments02.csv and
  Shipments03.csv -- consistent with a known export overlap (Colette's
  side to confirm the root cause). Splitting this further:
    - 181 of the 194 are TRUE duplicates -- every column matches exactly
      between the two files.
    - 13 of the 194 are CONFLICTING -- same OrderID/LineNo, but a
      DIFFERENT ShipperID between the two files (example: OrderID 16915,
      LineNo 2 -- ShipperID 5 in Shipments02.csv vs. ShipperID 1 in
      Shipments03.csv).

  BUSINESS IMPACT
  The 181 true duplicates are harmless -- they resolve to a single row
  automatically (see Action Taken). The 13 conflicting rows are the real
  concern: whichever file happens to load last in the pipeline silently
  "wins" and overwrites the other value, with no business rule behind
  that outcome and no visibility that a conflict occurred.

  ACTION TAKEN IN THIS POC
  Bronze preserves all 194 rows as-is from both source files (Bronze does
  not deduplicate -- that would hide a real characteristic of the source
  feed). In Silver, the 181 true duplicates collapse naturally to one row
  via the MERGE key (order_id, line_no) plus row_hash-based change
  detection, since an identical row produces no update. The 13 conflicting
  rows are NOT specifically resolved -- they are subject to whichever file
  the pipeline processes last, which is an unvalidated, incidental outcome
  rather than a deliberate business rule.

  RECOMMENDATION FOR ACME
  1. Confirm with the Shipments export owner whether Shipments02/03 overlap
     is an expected artifact of how the export is split (e.g. by date
     range with an intentional buffer) or a genuine export bug.
  2. For the 13 conflicting ShipperID rows specifically: define an explicit
     tie-breaking rule (e.g. "most recently exported file wins" or "flag
     for manual review") rather than relying on incidental file
     processing order, which could change unexpectedly between runs.

  ADDITIONAL OBSERVATION
  In most of the 13 conflicting keys, one of the two competing values is
  shipper_id=5 -- one of the two orphan shippers already flagged in
  Recommendation #2 (no master data in Shippers.csv). This may not be a
  coincidence: it could indicate shipper_id=5 is itself a secondary
  default/placeholder value used by the source system in ambiguous cases,
  similar in spirit to the emp_id=2 default pattern found in
  Recommendation #4. Not fully investigated here due to time constraints
  -- flagged for follow-up.
""")
print(f"{'='*90}")

print("-- Example: the 13 conflicting rows, with both source values shown --")

df_bronze_shipments = spark.table("dbo.bronze_shipments")

# Find keys with more than 1 physical row (Bronze preserves the 194
# overlapping rows as-is, since it does not deduplicate).
df_dup_keys = (
    df_bronze_shipments
    .groupBy("order_id", "line_no")
    .count()
    .filter(col("count") > 1)
    .select("order_id", "line_no")
)

df_dup_rows = (
    df_bronze_shipments
    .join(df_dup_keys, on=["order_id", "line_no"], how="inner")
    .select("order_id", "line_no", "shipper_id")
)

print(f"Physical duplicate-key rows in Bronze: {df_dup_rows.count()} (expect ~388 = 194 keys x 2 rows each)")

# Within those, isolate the keys where shipper_id actually differs
# between the two physical rows -- the 13 genuine conflicts.
df_conflicts = (
    df_dup_rows
    .groupBy("order_id", "line_no")
    .agg(F.countDistinct("shipper_id").alias("distinct_shipper_ids"))
    .filter(col("distinct_shipper_ids") > 1)
)

print(f"Conflicting keys found (different shipper_id per duplicate): {df_conflicts.count()}")

print("-- The actual conflicting rows, both versions shown side by side --")
display(
    df_dup_rows
    .join(df_conflicts.select("order_id", "line_no"), on=["order_id", "line_no"], how="inner")
    .orderBy("order_id", "line_no")
)
