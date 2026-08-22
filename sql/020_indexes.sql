-- =====================================================================
-- QueryQuest :: 020_indexes.sql   (optimized schema only)
--
-- Every index here exists because a specific query in sql/queries/ was
-- profiled with EXPLAIN (ANALYZE, BUFFERS) and found doing something
-- stupid. The comment on each one names the query it serves and the
-- plan node it removes. Indexes without a named consumer are cost, not
-- benefit -- they slow every write and inflate every vacuum.
--
-- Column order rule used throughout: equality predicates first, then
-- the range/sort column, then INCLUDE payload. That is what lets the
-- planner turn a filter + sort into a single ordered index scan.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Q1: customer order history.
--   WHERE customer_id = ? AND placed_at >= ? ORDER BY placed_at DESC
--
-- (customer_id, placed_at DESC) matches equality-then-range exactly, so
-- the sort disappears. The INCLUDE payload carries the three columns
-- the query projects, which makes it index-only: no heap fetch per row.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS orders_customer_placed_idx
    ON qq.orders (customer_id, placed_at DESC)
    INCLUDE (status, total_amount, item_count);

-- ---------------------------------------------------------------------
-- Q4: open-order backlog.
--   WHERE status IN ('pending','paid') ORDER BY placed_at
--
-- PARTIAL. Roughly 6% of orders are ever open, so a full index on
-- status would be ~16x larger and mostly consist of rows the query can
-- never want. The partial index is small enough to stay resident in
-- shared_buffers, which is the entire point.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS orders_open_backlog_idx
    ON qq.orders (placed_at)
    INCLUDE (order_id, customer_id, total_amount)
    WHERE status IN ('pending', 'paid');

-- ---------------------------------------------------------------------
-- Q2 fallback: revenue by region over a date window.
-- Serves the matview refresh as well as any ad-hoc drill-down that
-- misses the mart.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS orders_region_placed_idx
    ON qq.orders (region_id, placed_at)
    INCLUDE (total_amount, status);

-- ---------------------------------------------------------------------
-- BRIN on the fact table's time column.
--
-- order_items is loaded in placed_at order and never updated, so
-- physical order tracks logical order almost perfectly -- the ideal
-- BRIN case. At 128 pages per range this costs a few hundred KB for a
-- table of tens of millions of rows, versus a btree in the hundreds of
-- MB. It cannot replace the btrees below for selective lookups; it
-- exists to cheaply skip whole segments on wide scans.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS order_items_placed_brin_idx
    ON qq.order_items USING brin (placed_at) WITH (pages_per_range = 128);

-- ---------------------------------------------------------------------
-- Q5: SKU velocity -- units sold for one variant over a window.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS order_items_variant_placed_idx
    ON qq.order_items (variant_id, placed_at DESC)
    INCLUDE (quantity, line_total);

-- ---------------------------------------------------------------------
-- Q3: top sellers by revenue in a window, and the seller mart refresh.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS order_items_seller_placed_idx
    ON qq.order_items (seller_id, placed_at)
    INCLUDE (line_total, quantity);

-- Supports the order_items -> orders join and the FK's ON DELETE CASCADE.
CREATE INDEX IF NOT EXISTS order_items_order_idx
    ON qq.order_items (order_id, placed_at);

-- ---------------------------------------------------------------------
-- Q6: restock list.
--   WHERE qty_available <= reorder_point
--
-- PARTIAL on a cross-column predicate. Only a low-single-digit
-- percentage of SKUs are ever below their reorder point, so this index
-- holds only the rows the alerting job actually reads.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS inventory_restock_idx
    ON qq.inventory (variant_id)
    INCLUDE (qty_on_hand, qty_reserved, reorder_point)
    WHERE qty_available <= reorder_point;

-- ---------------------------------------------------------------------
-- Catalogue browse: active products in a category, cheapest first.
-- Partial on is_active because delisted products are never browsed.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS products_category_price_idx
    ON qq.products (category_id, base_price)
    INCLUDE (product_id, title, seller_id)
    WHERE is_active;

CREATE INDEX IF NOT EXISTS product_variants_product_idx
    ON qq.product_variants (product_id)
    WHERE is_active;

-- ---------------------------------------------------------------------
-- Case-insensitive login lookup. An expression index -- without it,
-- lower(email) = ? cannot use customers_email_uq at all.
-- ---------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS customers_email_lower_uq
    ON qq.customers (lower(email));

CREATE INDEX IF NOT EXISTS customers_region_signup_idx
    ON qq.customers (region_id, signup_at DESC)
    WHERE status = 'active';

-- ---------------------------------------------------------------------
-- Order detail page: payments and shipments for one order.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS payments_order_idx
    ON qq.payments (order_id, placed_at)
    INCLUDE (status, amount, method);

CREATE INDEX IF NOT EXISTS shipments_order_idx
    ON qq.shipments (order_id, placed_at);

-- Ops view: everything still moving. Partial -- delivered is terminal
-- and accounts for the overwhelming majority of rows.
CREATE INDEX IF NOT EXISTS shipments_in_flight_idx
    ON qq.shipments (status, shipped_at)
    WHERE status IN ('label_created', 'in_transit');

-- ---------------------------------------------------------------------
-- Planner statistics.
--
-- customer_id and region_id are correlated (a customer sits in one
-- region), so the planner multiplies their selectivities and badly
-- underestimates the row count, which pushes it into a nested loop
-- where a hash join is correct. Extended statistics teach it the
-- dependency.
-- ---------------------------------------------------------------------
CREATE STATISTICS IF NOT EXISTS orders_cust_region_stx (dependencies, ndistinct)
    ON customer_id, region_id, status FROM qq.orders;

CREATE STATISTICS IF NOT EXISTS order_items_variant_seller_stx (dependencies, ndistinct)
    ON variant_id, seller_id FROM qq.order_items;
