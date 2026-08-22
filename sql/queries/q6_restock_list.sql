-- Q6: SKUs at or below their reorder point.
-- Runs on a schedule to drive purchasing.
--
-- Baseline plan  : full scan of inventory, filter, sort.
-- Optimized plan : inventory_restock_idx, PARTIAL on the cross-column
--                  predicate qty_available <= reorder_point, so the
--                  index contains only the rows this query wants.
SELECT i.variant_id,
       i.qty_on_hand,
       i.qty_reserved,
       i.qty_available,
       i.reorder_point,
       (i.reorder_point - i.qty_available) AS shortfall
FROM ${SCHEMA}.inventory i
WHERE i.qty_available <= i.reorder_point
ORDER BY shortfall DESC
LIMIT 100;
