-- =====================================================================
-- QueryQuest :: 001_schema.sql
-- Core normalized (3NF) OLTP schema -- 12 domain tables.
--
-- This file is rendered twice, with the ${SCHEMA} placeholder
-- substituted by queryquest.db.render_sql():
--
--   qq           optimized -> partitioned, indexed, matviews on top
--   qq_baseline  naive     -> heap tables, primary keys only
--
-- Both schemas hold identical rows, so the perf numbers in
-- docs/BENCHMARKS.md are a measured A/B on one box rather than an
-- assertion.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS ${SCHEMA};

-- ---------------------------------------------------------------------
-- Enumerated domains. Native enums are 4 bytes on disk (vs a text
-- status column) and make illegal states unrepresentable without
-- paying for a lookup join on every read.
-- ---------------------------------------------------------------------
DO $enum$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'customer_status' AND n.nspname = '${SCHEMA}'
    ) THEN
        CREATE TYPE ${SCHEMA}.customer_status AS ENUM ('active','dormant','churned','blocked');
        CREATE TYPE ${SCHEMA}.order_status    AS ENUM ('pending','paid','fulfilled','cancelled','refunded');
        CREATE TYPE ${SCHEMA}.payment_status  AS ENUM ('authorized','captured','failed','refunded','chargeback');
        CREATE TYPE ${SCHEMA}.shipment_status AS ENUM ('label_created','in_transit','delivered','lost','returned');
        CREATE TYPE ${SCHEMA}.address_kind    AS ENUM ('billing','shipping');
    END IF;
END
$enum$;


-- =====================================================================
-- 1. regions -- geographic rollup dimension
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.regions (
    region_id     smallint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_code   text          NOT NULL,
    region_name   text          NOT NULL,
    country_code  char(2)       NOT NULL,
    currency_code char(3)       NOT NULL,
    created_at    timestamptz   NOT NULL DEFAULT now(),
    updated_at    timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT regions_code_uq    UNIQUE (region_code),
    CONSTRAINT regions_country_ck CHECK (country_code = upper(country_code))
);

-- =====================================================================
-- 2. customers
-- customer_uid is the natural key owned by the source system; the
-- surrogate customer_id is ours. The ETL matches on customer_uid, the
-- application joins on customer_id.
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.customers (
    customer_id    bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_uid   text          NOT NULL,
    email          text          NOT NULL,
    full_name      text          NOT NULL,
    region_id      smallint      NOT NULL REFERENCES ${SCHEMA}.regions (region_id),
    status         ${SCHEMA}.customer_status NOT NULL DEFAULT 'active',
    signup_at      timestamptz   NOT NULL,
    lifetime_value numeric(12,2) NOT NULL DEFAULT 0,
    -- content hash of the source payload. Lets the UPSERT skip rows that
    -- did not actually change, which keeps re-runs from bloating the heap
    -- with dead tuples for no reason.
    source_hash    bytea         NOT NULL,
    source_batch   bigint,
    updated_at     timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT customers_uid_uq   UNIQUE (customer_uid),
    CONSTRAINT customers_ltv_ck   CHECK (lifetime_value >= 0),
    CONSTRAINT customers_email_ck CHECK (position('@' in email) > 1)
);

-- =====================================================================
-- 3. customer_addresses
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.customer_addresses (
    address_id   bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  bigint      NOT NULL REFERENCES ${SCHEMA}.customers (customer_id) ON DELETE CASCADE,
    kind         ${SCHEMA}.address_kind NOT NULL,
    line1        text        NOT NULL,
    line2        text,
    city         text        NOT NULL,
    postal_code  text        NOT NULL,
    region_id    smallint    NOT NULL REFERENCES ${SCHEMA}.regions (region_id),
    is_default   boolean     NOT NULL DEFAULT false,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    -- One billing and one shipping address per customer. This doubles as
    -- the ETL's conflict target, so a re-delivered feed updates the
    -- address in place instead of accumulating duplicates.
    CONSTRAINT addresses_customer_kind_uq UNIQUE (customer_id, kind)
);

-- =====================================================================
-- 4. sellers -- marketplace supply side
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.sellers (
    seller_id     bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_uid    text          NOT NULL,
    display_name  text          NOT NULL,
    region_id     smallint      NOT NULL REFERENCES ${SCHEMA}.regions (region_id),
    rating        numeric(3,2)  NOT NULL DEFAULT 0,
    is_active     boolean       NOT NULL DEFAULT true,
    onboarded_at  timestamptz   NOT NULL,
    source_hash   bytea         NOT NULL,
    updated_at    timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT sellers_uid_uq    UNIQUE (seller_uid),
    CONSTRAINT sellers_rating_ck CHECK (rating >= 0 AND rating <= 5)
);

-- =====================================================================
-- 5. categories -- self-referencing hierarchy
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.categories (
    category_id   integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_slug text        NOT NULL,
    category_name text        NOT NULL,
    parent_id     integer     REFERENCES ${SCHEMA}.categories (category_id),
    depth         smallint    NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT categories_slug_uq    UNIQUE (category_slug),
    CONSTRAINT categories_depth_ck   CHECK (depth BETWEEN 0 AND 4),
    CONSTRAINT categories_acyclic_ck CHECK (parent_id IS NULL OR parent_id <> category_id)
);

-- =====================================================================
-- 6. products
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.products (
    product_id    bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_uid   text          NOT NULL,
    title         text          NOT NULL,
    seller_id     bigint        NOT NULL REFERENCES ${SCHEMA}.sellers (seller_id),
    category_id   integer       NOT NULL REFERENCES ${SCHEMA}.categories (category_id),
    base_price    numeric(10,2) NOT NULL,
    is_active     boolean       NOT NULL DEFAULT true,
    launched_at   timestamptz   NOT NULL,
    source_hash   bytea         NOT NULL,
    updated_at    timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT products_uid_uq   UNIQUE (product_uid),
    CONSTRAINT products_price_ck CHECK (base_price > 0)
);

-- =====================================================================
-- 7. product_variants -- SKU grain
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.product_variants (
    variant_id    bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku           text          NOT NULL,
    product_id    bigint        NOT NULL REFERENCES ${SCHEMA}.products (product_id) ON DELETE CASCADE,
    variant_label text          NOT NULL,
    price_delta   numeric(10,2) NOT NULL DEFAULT 0,
    weight_grams  integer       NOT NULL DEFAULT 0,
    is_active     boolean       NOT NULL DEFAULT true,
    source_hash   bytea         NOT NULL,
    updated_at    timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT variants_sku_uq    UNIQUE (sku),
    CONSTRAINT variants_weight_ck CHECK (weight_grams >= 0)
);

-- =====================================================================
-- 8. inventory -- one row per SKU. This is the contended row that the
--    concurrency suite hammers with SELECT ... FOR UPDATE.
--
--    qty_available is GENERATED and CHECKed, so "a SKU can never be
--    oversold" is something the database enforces rather than something
--    the application promises to remember.
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.inventory (
    variant_id     bigint      PRIMARY KEY REFERENCES ${SCHEMA}.product_variants (variant_id) ON DELETE CASCADE,
    qty_on_hand    integer     NOT NULL DEFAULT 0,
    qty_reserved   integer     NOT NULL DEFAULT 0,
    qty_available  integer     GENERATED ALWAYS AS (qty_on_hand - qty_reserved) STORED,
    reorder_point  integer     NOT NULL DEFAULT 0,
    row_version    bigint      NOT NULL DEFAULT 1,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT inventory_on_hand_ck  CHECK (qty_on_hand  >= 0),
    CONSTRAINT inventory_reserved_ck CHECK (qty_reserved >= 0),
    CONSTRAINT inventory_oversell_ck CHECK (qty_reserved <= qty_on_hand)
);

-- =====================================================================
-- 9. orders -- the fact table.
--
-- Under ${SCHEMA} = qq this is RANGE partitioned on placed_at (see
-- 002_partitions.sql). Under qq_baseline it stays a plain heap, so the
-- benchmark can attribute the win to partitioning instead of guessing.
--
-- A partitioned table's primary key must contain the partition key,
-- hence the composite (order_id, placed_at).
-- =====================================================================
-- A partitioned table cannot use GENERATED AS IDENTITY on its key, so
-- the surrogate comes from an explicit sequence instead. The natural key
-- stays (order_uid, placed_at); on a re-run the UPSERT matches on that
-- and leaves order_id untouched, which is what keeps downstream foreign
-- keys stable across replays.
CREATE SEQUENCE IF NOT EXISTS ${SCHEMA}.orders_order_id_seq AS bigint;
CREATE SEQUENCE IF NOT EXISTS ${SCHEMA}.order_items_id_seq  AS bigint;

CREATE TABLE IF NOT EXISTS ${SCHEMA}.orders (
    order_id      bigint        NOT NULL DEFAULT nextval('${SCHEMA}.orders_order_id_seq'),
    placed_at     timestamptz   NOT NULL,
    order_uid     text          NOT NULL,
    customer_id   bigint        NOT NULL,
    region_id     smallint      NOT NULL,
    status        ${SCHEMA}.order_status NOT NULL DEFAULT 'pending',
    currency_code char(3)       NOT NULL,
    subtotal      numeric(12,2) NOT NULL,
    shipping_fee  numeric(10,2) NOT NULL DEFAULT 0,
    tax_amount    numeric(10,2) NOT NULL DEFAULT 0,
    total_amount  numeric(12,2) NOT NULL,
    item_count    smallint      NOT NULL,
    source_hash   bytea         NOT NULL,
    source_batch  bigint,
    updated_at    timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT orders_total_ck CHECK (total_amount >= 0),
    CONSTRAINT orders_items_ck CHECK (item_count > 0)
) ${PARTITION_ORDERS};

-- =====================================================================
-- 10. order_items -- ~5 lines per order, the largest table by row count.
--     Carries placed_at so it partitions on the same key as orders,
--     which is what makes partition-wise joins possible.
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.order_items (
    order_item_id bigint        NOT NULL DEFAULT nextval('${SCHEMA}.order_items_id_seq'),
    placed_at     timestamptz   NOT NULL,
    order_id      bigint        NOT NULL,
    -- line_no makes (order_id, line_no) the natural key of a line item,
    -- so the ETL can UPSERT lines without deleting and re-inserting the
    -- whole order on every replay.
    line_no       smallint      NOT NULL,
    variant_id    bigint        NOT NULL,
    seller_id     bigint        NOT NULL,
    quantity      smallint      NOT NULL,
    unit_price    numeric(10,2) NOT NULL,
    discount      numeric(10,2) NOT NULL DEFAULT 0,
    line_total    numeric(12,2) NOT NULL,
    CONSTRAINT order_items_qty_ck      CHECK (quantity > 0),
    CONSTRAINT order_items_discount_ck CHECK (discount >= 0)
) ${PARTITION_ITEMS};

-- =====================================================================
-- 11. payments
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.payments (
    payment_id    bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_uid   text          NOT NULL,
    order_id      bigint        NOT NULL,
    placed_at     timestamptz   NOT NULL,
    status        ${SCHEMA}.payment_status NOT NULL,
    method        text          NOT NULL,
    amount        numeric(12,2) NOT NULL,
    processed_at  timestamptz   NOT NULL,
    source_hash   bytea         NOT NULL,
    CONSTRAINT payments_uid_uq    UNIQUE (payment_uid),
    CONSTRAINT payments_amount_ck CHECK (amount >= 0)
);

-- =====================================================================
-- 12. shipments
-- =====================================================================
CREATE TABLE IF NOT EXISTS ${SCHEMA}.shipments (
    shipment_id   bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_uid  text        NOT NULL,
    order_id      bigint      NOT NULL,
    placed_at     timestamptz NOT NULL,
    carrier       text        NOT NULL,
    status        ${SCHEMA}.shipment_status NOT NULL,
    shipped_at    timestamptz,
    delivered_at  timestamptz,
    tracking_code text,
    CONSTRAINT shipments_uid_uq   UNIQUE (shipment_uid),
    CONSTRAINT shipments_dates_ck CHECK (
        delivered_at IS NULL OR shipped_at IS NULL OR delivered_at >= shipped_at
    )
);
