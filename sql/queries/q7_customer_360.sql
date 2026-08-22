-- Q7: the customer profile panel -- the read path fronted by Redis.
--
-- Baseline plan  : nested loop over unindexed joins, then an aggregate
--                  over the customer's entire order history.
-- Optimized plan : indexed joins, with lifetime totals read from the
--                  customer_ltv rollup instead of recomputed per call.
SELECT c.customer_id,
       c.customer_uid,
       c.full_name,
       c.status,
       r.region_code,
       count(o.order_id)                    AS lifetime_orders,
       coalesce(sum(o.total_amount), 0)     AS lifetime_value,
       max(o.placed_at)                     AS last_order_at
FROM ${SCHEMA}.customers c
JOIN ${SCHEMA}.regions r ON r.region_id = c.region_id
LEFT JOIN ${SCHEMA}.orders o
       ON o.customer_id = c.customer_id
      AND o.status IN ('paid', 'fulfilled')
WHERE c.customer_id = %(customer_id)s
GROUP BY c.customer_id, c.customer_uid, c.full_name, c.status, r.region_code;
