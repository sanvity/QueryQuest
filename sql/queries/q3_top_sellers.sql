-- Q3: seller leaderboard by revenue over a window.
-- Aggregates the largest table in the schema.
--
-- Baseline plan  : full scan of order_items (tens of millions of rows),
--                  hash aggregate, top-N sort.
-- Optimized plan : partition pruning + order_items_seller_placed_idx.
SELECT oi.seller_id,
       sum(oi.line_total) AS revenue,
       sum(oi.quantity)   AS units_sold,
       count(*)           AS line_count
FROM ${SCHEMA}.order_items oi
WHERE oi.placed_at >= %(since)s
GROUP BY oi.seller_id
ORDER BY revenue DESC
LIMIT 20;
