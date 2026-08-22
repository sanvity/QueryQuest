-- Q3 (optimized): served from the seller day-grain rollup.
SELECT sd.seller_id,
       sum(sd.revenue)     AS revenue,
       sum(sd.units_sold)  AS units_sold,
       sum(sd.order_count) AS line_count
FROM qq_mart.seller_daily_sales sd
WHERE sd.day >= %(since)s::date
GROUP BY sd.seller_id
ORDER BY revenue DESC
LIMIT 20;
