-- Q7 (optimized): lifetime totals come from the rollup.
SELECT c.customer_id,
       c.customer_uid,
       c.full_name,
       c.status,
       r.region_code,
       coalesce(l.lifetime_orders, 0) AS lifetime_orders,
       coalesce(l.lifetime_value,  0) AS lifetime_value,
       l.last_order_at
FROM qq.customers c
JOIN qq.regions r ON r.region_id = c.region_id
LEFT JOIN qq_mart.customer_ltv l ON l.customer_id = c.customer_id
WHERE c.customer_id = %(customer_id)s;
