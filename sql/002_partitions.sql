-- =====================================================================
-- QueryQuest :: 002_partitions.sql   (optimized schema only)
--
-- Monthly RANGE partitions on placed_at for orders and order_items.
--
-- Why monthly: the access pattern is overwhelmingly "recent window"
-- (last 7 / 30 / 90 days) plus a long tail of per-customer history.
-- Monthly granularity keeps the partition count in the dozens -- low
-- enough that planning time stays flat -- while letting the planner
-- prune away ~95% of the heap for a 30-day query.
--
-- Both tables use the same key, so the planner can also do
-- partition-wise joins (enable_partitionwise_join).
-- =====================================================================

-- Creates any missing monthly partitions covering [range_start, range_end).
-- Idempotent: re-running is a no-op, so it is safe on every deploy.
CREATE OR REPLACE FUNCTION qq.ensure_partitions(range_start date, range_end date)
RETURNS integer
LANGUAGE plpgsql
AS $fn$
DECLARE
    cur_month  date := date_trunc('month', range_start)::date;
    nxt_month  date;
    created    integer := 0;
    part_name  text;
    parent     text;
BEGIN
    WHILE cur_month < range_end LOOP
        nxt_month := (cur_month + interval '1 month')::date;

        FOREACH parent IN ARRAY ARRAY['orders', 'order_items'] LOOP
            part_name := format('%s_p%s', parent, to_char(cur_month, 'YYYYMM'));

            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'qq' AND c.relname = part_name
            ) THEN
                EXECUTE format(
                    'CREATE TABLE qq.%I PARTITION OF qq.%I FOR VALUES FROM (%L) TO (%L)',
                    part_name, parent, cur_month, nxt_month
                );
                created := created + 1;
            END IF;
        END LOOP;

        cur_month := nxt_month;
    END LOOP;

    RETURN created;
END
$fn$;

-- A DEFAULT partition is a safety net, not a design: rows that fall
-- outside every declared range land here instead of erroring the load.
-- The DQ suite alerts if it is ever non-empty, because a non-empty
-- default partition means the calendar has drifted past the partitions
-- that exist -- and it also blocks future ATTACH operations.
DO $default_parts$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'qq' AND c.relname = 'orders_pdefault'
    ) THEN
        CREATE TABLE qq.orders_pdefault      PARTITION OF qq.orders      DEFAULT;
        CREATE TABLE qq.order_items_pdefault PARTITION OF qq.order_items DEFAULT;
    END IF;
END
$default_parts$;
