-- Q4: orders still awaiting fulfilment, oldest first.
-- The operations queue, polled continuously.
--
-- Baseline plan  : full scan + filter on status + sort.
-- Optimized plan : orders_open_backlog_idx, a PARTIAL index over only
--                  the small fraction of rows whose status is still
--                  open. Small enough to stay resident in
--                  shared_buffers, so the poll costs a handful of
--                  buffer hits rather than a scan of the fact table.
SELECT o.order_id,
       o.customer_id,
       o.placed_at,
       o.total_amount
FROM ${SCHEMA}.orders o
WHERE o.status IN ('pending', 'paid')
  AND o.placed_at < %(cutoff)s
ORDER BY o.placed_at
LIMIT 200;
