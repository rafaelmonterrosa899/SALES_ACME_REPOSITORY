-- =============================================================================
-- VIEW: governance.dq_order_details
-- =============================================================================
-- Purpose : Data quality checks against silver.fact_order_details. One row
--           per rule violation found -- not one row per order line.
--
-- Checks:
--   1. Orphan product_id -- not present in gold.dim_product
--   2. discount out of logical range (< 0 or > 1)
--   3. quantity <= 0
--   4. unit_price <= 0
--   5. Negative margin -- the 25% discount finding from q01-q06. Recomputes
--      margin here (quantity * unit_price * (1-discount) - quantity * unit_cost)
--      independently of gold.fact_sales, so this check still works even if
--      Gold hasn't been rebuilt yet after a Silver change.
-- =============================================================================

CREATE VIEW governance.dq_order_details AS

SELECT
    od.order_id,
    'ORPHAN_PRODUCT_ID' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('product_id ', TRY_CAST(od.product_id AS VARCHAR), ' not found in gold.dim_product') AS detail
FROM silver.fact_order_details od
LEFT JOIN gold.dim_product p ON od.product_id = p.product_id
WHERE p.product_id IS NULL AND od.product_id IS NOT NULL

UNION ALL

SELECT
    od.order_id,
    'DISCOUNT_OUT_OF_RANGE' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('discount ', TRY_CAST(od.discount AS VARCHAR), ' is outside the valid 0-1 range') AS detail
FROM silver.fact_order_details od
WHERE od.discount < 0 OR od.discount > 1

UNION ALL

SELECT
    od.order_id,
    'NON_POSITIVE_QUANTITY' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('quantity ', TRY_CAST(od.quantity AS VARCHAR), ' is zero or negative') AS detail
FROM silver.fact_order_details od
WHERE od.quantity <= 0

UNION ALL

SELECT
    od.order_id,
    'NON_POSITIVE_UNIT_PRICE' AS rule_failed,
    'ERROR' AS severity,
    CONCAT('unit_price ', TRY_CAST(od.unit_price AS VARCHAR), ' is zero or negative') AS detail
FROM silver.fact_order_details od
WHERE od.unit_price <= 0

UNION ALL

SELECT
    od.order_id,
    'NEGATIVE_MARGIN' AS rule_failed,
    'WARNING' AS severity,
    CONCAT(
        'line margin is negative: extended_amount ',
        TRY_CAST(ROUND(od.quantity * od.unit_price * (1 - od.discount), 2) AS VARCHAR),
        ' vs. cost ', TRY_CAST(ROUND(od.quantity * p.unit_cost, 2) AS VARCHAR),
        ' -- discount of ', TRY_CAST(ROUND(od.discount * 100, 1) AS VARCHAR), '% likely too aggressive'
    ) AS detail
FROM silver.fact_order_details od
INNER JOIN gold.dim_product p ON od.product_id = p.product_id
WHERE (od.quantity * od.unit_price * (1 - od.discount)) - (od.quantity * p.unit_cost) < 0;
