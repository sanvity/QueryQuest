-- Q2: gross and net revenue by region over a window.
-- The executive dashboard's home screen; runs on every page load.
--
-- Baseline plan  : full scan of orders, hash aggregate.
-- Optimized plan : partition pruning on placed_at drops every month
--                  outside the window before a single row is read; the
--                  remaining scan is served by orders_region_placed_idx.
-- See q2_revenue_by_region.mart.sql for the pre-aggregated form.
SELECT o.region_id,
       count(*)                                  AS order_count,
       sum(o.total_amount)                       AS gross_revenue,
       sum(o.total_amount) FILTER (
           WHERE o.status IN ('paid', 'fulfilled')
       )                                         AS net_revenue
FROM ${SCHEMA}.orders o
WHERE o.placed_at >= %(since)s
  AND o.placed_at <  %(until)s
GROUP BY o.region_id
ORDER BY gross_revenue DESC;
