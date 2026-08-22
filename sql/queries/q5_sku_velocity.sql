-- Q5: weekly sales velocity for one SKU.
-- Powers the seller-facing product analytics panel.
--
-- Baseline plan  : full scan of order_items filtered on variant_id.
-- Optimized plan : order_items_variant_placed_idx
--                  (variant_id, placed_at DESC) INCLUDE (quantity, line_total)
--                  -- equality then range, with the aggregated columns
--                  carried in the index, so it never touches the heap.
SELECT date_trunc('week', oi.placed_at)::date AS week,
       sum(oi.quantity)   AS units,
       sum(oi.line_total) AS revenue,
       count(*)           AS line_count
FROM ${SCHEMA}.order_items oi
WHERE oi.variant_id = %(variant_id)s
  AND oi.placed_at >= %(since)s
GROUP BY 1
ORDER BY 1;
