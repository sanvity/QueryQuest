-- Q1: a customer's recent order history.
-- The single most common OLTP read in the system.
--
-- Baseline plan   : sequential scan over every order + external sort.
-- Optimized plan  : index-only scan on
--                   orders_customer_placed_idx (customer_id, placed_at DESC)
--                   INCLUDE (status, total_amount, item_count)
--                   -- the ORDER BY is satisfied by the index, and the
--                   INCLUDE payload removes the heap fetch entirely.
SELECT o.order_id,
       o.placed_at,
       o.status,
       o.total_amount,
       o.item_count
FROM ${SCHEMA}.orders o
WHERE o.customer_id = %(customer_id)s
  AND o.placed_at >= %(since)s
ORDER BY o.placed_at DESC
LIMIT 50;
