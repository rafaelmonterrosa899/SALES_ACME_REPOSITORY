-- =============================================================================
-- VIEW: governance.dq_shipments
-- =============================================================================
-- Purpose : Data quality checks against silver.fact_shipments. One row per
--           rule violation found -- not one row per shipment.
--
-- Checks:
--   1. Orphan shipper_id -- not present in gold.dim_shipper
--   2. shipment_date earlier than order_date -- business rule violation
--      (the -8.95 year bug is already corrected upstream, but this check
--      stays in place to catch any regression in a future load)
--   3. employee_id mismatch between Shipments and Orders for the same
--      line, WITH concentration analysis -- flags any employee_id that
--      accounts for a disproportionate share of all mismatches (default/
--      placeholder value pattern). Threshold-based, not hardcoded to any
--      specific employee_id, so it generalizes to future occurrences.
--      Confirmed finding: 1,172 of 17,032 lines mismatch (6.9%), with
--      99.3% of those pointing to emp_id=2 (Erik Presley, President) --
--      almost certainly a source-system default value, not a real
--      business process where a different employee handles shipping.
-- =============================================================================

CREATE VIEW governance.dq_shipments AS

WITH employee_mismatches AS (
    SELECT
        o.order_id,
        sh.line_no,
        o.employee_id AS order_employee_id,
        sh.employee_id AS shipment_employee_id
    FROM silver.fact_orders o
    INNER JOIN silver.fact_shipments sh
        ON o.order_id = sh.order_id
    WHERE o.employee_id <> sh.employee_id
),

mismatch_totals AS (
    SELECT COUNT(*) AS total_mismatches
    FROM employee_mismatches
),

mismatch_concentration AS (
    SELECT
        em.shipment_employee_id,
        COUNT(*) AS mismatch_count,
        mt.total_mismatches,
        CAST(COUNT(*) AS FLOAT) / CAST(mt.total_mismatches AS FLOAT) AS concentration_pct
    FROM employee_mismatches em
    CROSS JOIN mismatch_totals mt
    GROUP BY em.shipment_employee_id, mt.total_mismatches
    HAVING CAST(COUNT(*) AS FLOAT) / CAST(mt.total_mismatches AS FLOAT) > 0.5
)

-- Check 1: orphan shipper_id
SELECT
    sh.order_id,
    'ORPHAN_SHIPPER_ID' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('shipper_id ', TRY_CAST(sh.shipper_id AS VARCHAR), ' not found in gold.dim_shipper') AS detail
FROM silver.fact_shipments sh
LEFT JOIN gold.dim_shipper s ON sh.shipper_id = s.shipper_id
WHERE s.shipper_id IS NULL AND sh.shipper_id IS NOT NULL

UNION ALL

-- Check 2: shipment_date earlier than order_date
SELECT
    sh.order_id,
    'SHIPMENT_BEFORE_ORDER' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('shipment_date ', TRY_CAST(sh.shipment_date AS VARCHAR), ' is earlier than order_date ', TRY_CAST(o.order_date AS VARCHAR)) AS detail
FROM silver.fact_shipments sh
INNER JOIN silver.fact_orders o ON sh.order_id = o.order_id
WHERE sh.shipment_date < o.order_date

UNION ALL

-- Check 3: employee_id mismatch with disproportionate concentration
-- (default/placeholder value pattern)
SELECT
    em.order_id,
    'EMPLOYEE_ID_DEFAULT_PATTERN' AS rule_failed,
    'WARNING' AS severity,
    CONCAT(
        'shipment_employee_id ', TRY_CAST(em.shipment_employee_id AS VARCHAR),
        ' differs from order_employee_id ', TRY_CAST(em.order_employee_id AS VARCHAR),
        ' -- this employee_id accounts for ',
        TRY_CAST(ROUND(mc.concentration_pct * 100, 1) AS VARCHAR),
        '% of all ', TRY_CAST(mc.total_mismatches AS VARCHAR),
        ' Orders/Shipments employee_id mismatches -- likely a source-system default value'
    ) AS detail
FROM employee_mismatches em
INNER JOIN mismatch_concentration mc
    ON em.shipment_employee_id = mc.shipment_employee_id;
