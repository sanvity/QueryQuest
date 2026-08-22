-- Q2 (optimized): identical answer, read from the daily rollup.
--
-- The aggregation has already happened at refresh time, so this reads a
-- few thousand pre-summed day/region rows instead of scanning millions
-- of orders. This is the difference a materialized view actually makes:
-- not a faster scan, but no scan.
SELECT d.region_id,
       sum(d.order_count)   AS order_count,
       sum(d.gross_revenue) AS gross_revenue,
       sum(d.net_revenue)   AS net_revenue
FROM qq_mart.daily_region_revenue d
WHERE d.day >= %(since)s::date
  AND d.day <  %(until)s::date
GROUP BY d.region_id
ORDER BY gross_revenue DESC;
