-- =============================================================================
-- VIEW: governance.dq_orders
-- =============================================================================
-- Purpose : Data quality checks against silver.fact_orders. One row per
--           rule violation found -- not one row per order.
--
-- Checks:
--   1. Orphan customer_id  -- not present in gold.dim_customer
--   2. Orphan employee_id  -- not present in gold.dim_employee
--   3. Orphan shipper_id   -- not present in gold.dim_shipper
--   4. Null/missing order_date
--
-- Note: the "employee_id defaults to president" finding is NOT checked
-- here -- it requires comparing against silver.fact_shipments.employee_id,
-- so it lives in governance.dq_shipments instead.
-- =============================================================================

CREATE VIEW governance.dq_orders AS

SELECT
    o.order_id,
    'ORPHAN_CUSTOMER_ID' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('customer_id ', TRY_CAST(o.customer_id AS VARCHAR), ' not found in gold.dim_customer') AS detail
FROM silver.fact_orders o
LEFT JOIN gold.dim_customer c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL AND o.customer_id IS NOT NULL

UNION ALL

SELECT
    o.order_id,
    'ORPHAN_EMPLOYEE_ID' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('employee_id ', TRY_CAST(o.employee_id AS VARCHAR), ' not found in gold.dim_employee') AS detail
FROM silver.fact_orders o
LEFT JOIN gold.dim_employee e ON o.employee_id = e.emp_id
WHERE e.emp_id IS NULL AND o.employee_id IS NOT NULL

UNION ALL

SELECT
    o.order_id,
    'ORPHAN_SHIPPER_ID' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('shipper_id ', TRY_CAST(o.shipper_id AS VARCHAR), ' not found in gold.dim_shipper') AS detail
FROM silver.fact_orders o
LEFT JOIN gold.dim_shipper s ON o.shipper_id = s.shipper_id
WHERE s.shipper_id IS NULL AND o.shipper_id IS NOT NULL

UNION ALL

SELECT
    o.order_id,
    'MISSING_ORDER_DATE' AS rule_failed,
    'ERROR' AS severity,
    'order_date is NULL' AS detail
FROM silver.fact_orders o
WHERE o.order_date IS NULL;
