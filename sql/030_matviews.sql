-- =====================================================================
-- QueryQuest :: 030_matviews.sql   (optimized schema only)
--
-- The reporting mart. These four rollups are the difference between a
-- dashboard that scans tens of millions of order lines on every page
-- load and one that reads a few thousand pre-aggregated rows.
--
-- Every matview carries a UNIQUE index, which is not optional: it is
-- what REFRESH MATERIALIZED VIEW CONCURRENTLY requires. Without it the
-- refresh takes an ACCESS EXCLUSIVE lock and the dashboard goes dark
-- for the duration of the rebuild.
--
-- Created WITH NO DATA so a fresh database is cheap to stand up; the
-- first refresh populates them.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS qq_mart;

-- ---------------------------------------------------------------------
-- Revenue by day and region -- the executive dashboard's home screen.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS qq_mart.daily_region_revenue AS
SELECT
    date_trunc('day', o.placed_at)::date       AS day,
    o.region_id,
    count(*)                                   AS order_count,
    count(*) FILTER (WHERE o.status = 'cancelled') AS cancelled_count,
    sum(o.total_amount)                        AS gross_revenue,
    sum(o.total_amount) FILTER (
        WHERE o.status IN ('paid', 'fulfilled')
    )                                          AS net_revenue,
    avg(o.total_amount)::numeric(12,2)         AS avg_order_value,
    sum(o.item_count)                          AS units_ordered
FROM qq.orders o
GROUP BY 1, 2
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS daily_region_revenue_pk
    ON qq_mart.daily_region_revenue (day, region_id);

-- ---------------------------------------------------------------------
-- Seller leaderboard source. Aggregated at day grain so any window
-- (7d / 30d / QTD) is a cheap re-sum rather than a re-scan.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS qq_mart.seller_daily_sales AS
SELECT
    date_trunc('day', oi.placed_at)::date AS day,
    oi.seller_id,
    count(DISTINCT oi.order_id)           AS order_count,
    sum(oi.quantity)                      AS units_sold,
    sum(oi.line_total)                    AS revenue,
    avg(oi.unit_price)::numeric(10,2)     AS avg_unit_price
FROM qq.order_items oi
GROUP BY 1, 2
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS seller_daily_sales_pk
    ON qq_mart.seller_daily_sales (day, seller_id);

CREATE INDEX IF NOT EXISTS seller_daily_sales_revenue_idx
    ON qq_mart.seller_daily_sales (day, revenue DESC);

-- ---------------------------------------------------------------------
-- Category performance at month grain -- merchandising review.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS qq_mart.category_month_sales AS
SELECT
    date_trunc('month', oi.placed_at)::date AS month,
    p.category_id,
    count(DISTINCT oi.order_id)             AS order_count,
    sum(oi.quantity)                        AS units_sold,
    sum(oi.line_total)                      AS revenue,
    sum(oi.discount)                        AS discount_given
FROM qq.order_items oi
JOIN qq.product_variants v ON v.variant_id = oi.variant_id
JOIN qq.products         p ON p.product_id = v.product_id
GROUP BY 1, 2
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS category_month_sales_pk
    ON qq_mart.category_month_sales (month, category_id);

-- ---------------------------------------------------------------------
-- Customer lifetime value + recency. Powers segmentation and the
-- cached customer-360 read path.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS qq_mart.customer_ltv AS
SELECT
    o.customer_id,
    count(*)                        AS lifetime_orders,
    sum(o.total_amount)             AS lifetime_value,
    avg(o.total_amount)::numeric(12,2) AS avg_order_value,
    min(o.placed_at)                AS first_order_at,
    max(o.placed_at)                AS last_order_at
FROM qq.orders o
WHERE o.status IN ('paid', 'fulfilled')
GROUP BY 1
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS customer_ltv_pk
    ON qq_mart.customer_ltv (customer_id);

CREATE INDEX IF NOT EXISTS customer_ltv_value_idx
    ON qq_mart.customer_ltv (lifetime_value DESC);

-- ---------------------------------------------------------------------
-- refresh_all()
--
-- CONCURRENTLY needs a populated matview to diff against, so the first
-- refresh of a fresh database has to be a blocking one. Checking
-- relispopulated rather than assuming is what stops a cold deploy from
-- failing with "CONCURRENTLY cannot be used when the materialized view
-- is not populated".
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq_mart.refresh_all(use_concurrently boolean DEFAULT true)
RETURNS TABLE (view_name text, refreshed_concurrently boolean, duration_ms integer)
LANGUAGE plpgsql
AS $fn$
DECLARE
    mv          text;
    populated   boolean;
    do_conc     boolean;
    t0          timestamptz;
BEGIN
    FOREACH mv IN ARRAY ARRAY[
        'daily_region_revenue', 'seller_daily_sales',
        'category_month_sales', 'customer_ltv'
    ] LOOP
        SELECT c.relispopulated INTO populated
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'qq_mart' AND c.relname = mv;

        do_conc := use_concurrently AND coalesce(populated, false);
        t0 := clock_timestamp();

        IF do_conc THEN
            EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY qq_mart.%I', mv);
        ELSE
            EXECUTE format('REFRESH MATERIALIZED VIEW qq_mart.%I', mv);
        END IF;

        view_name              := mv;
        refreshed_concurrently := do_conc;
        duration_ms            := (extract(epoch FROM clock_timestamp() - t0) * 1000)::integer;
        RETURN NEXT;
    END LOOP;
END
$fn$;
