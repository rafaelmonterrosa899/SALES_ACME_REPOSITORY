-- =============================================================================
-- VIEW: governance.dq_summary
-- =============================================================================
-- Purpose : Consolidates all three DQ views into one, tagging each row with
--           its source view. Used by the Fabric Data Pipeline as a single
--           query point to check for any quality issue after Gold finishes,
--           instead of querying three separate views.
-- =============================================================================

CREATE VIEW governance.dq_summary AS

SELECT 'dq_orders' AS source_view, order_id, rule_failed, severity, detail
FROM governance.dq_orders

UNION ALL

SELECT 'dq_order_details' AS source_view, order_id, rule_failed, severity, detail
FROM governance.dq_order_details

UNION ALL

SELECT 'dq_shipments' AS source_view, order_id, rule_failed, severity, detail
FROM governance.dq_shipments;
