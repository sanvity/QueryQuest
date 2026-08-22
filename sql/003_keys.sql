-- =====================================================================
-- QueryQuest :: 003_keys.sql
-- Primary keys, natural-key uniqueness and foreign keys for the two
-- fact tables. Applied to BOTH schema variants, because keys are
-- correctness, not tuning -- the baseline is meant to be a naive
-- schema, not a broken one. Only *secondary* indexes are withheld from
-- the baseline.
--
-- Note the composite shapes: on a partitioned table every unique
-- constraint must contain the partition key, so the natural key of an
-- order is (order_uid, placed_at) rather than order_uid alone. The DQ
-- rule `orders_uid_stability` compensates by rejecting a batch in which
-- one order_uid arrives with two different placed_at values.
-- =====================================================================

DO $keys$
BEGIN
    -- ---- orders -----------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_orders_pk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.orders
                 ADD CONSTRAINT ${SCHEMA}_orders_pk PRIMARY KEY (order_id, placed_at)';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_orders_uid_uq') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.orders
                 ADD CONSTRAINT ${SCHEMA}_orders_uid_uq UNIQUE (order_uid, placed_at)';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_orders_customer_fk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.orders
                 ADD CONSTRAINT ${SCHEMA}_orders_customer_fk
                 FOREIGN KEY (customer_id) REFERENCES ${SCHEMA}.customers (customer_id)';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_orders_region_fk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.orders
                 ADD CONSTRAINT ${SCHEMA}_orders_region_fk
                 FOREIGN KEY (region_id) REFERENCES ${SCHEMA}.regions (region_id)';
    END IF;

    -- ---- order_items ------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_order_items_pk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.order_items
                 ADD CONSTRAINT ${SCHEMA}_order_items_pk PRIMARY KEY (order_item_id, placed_at)';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_order_items_line_uq') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.order_items
                 ADD CONSTRAINT ${SCHEMA}_order_items_line_uq UNIQUE (order_id, line_no, placed_at)';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_order_items_order_fk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.order_items
                 ADD CONSTRAINT ${SCHEMA}_order_items_order_fk
                 FOREIGN KEY (order_id, placed_at)
                 REFERENCES ${SCHEMA}.orders (order_id, placed_at) ON DELETE CASCADE';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_order_items_variant_fk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.order_items
                 ADD CONSTRAINT ${SCHEMA}_order_items_variant_fk
                 FOREIGN KEY (variant_id) REFERENCES ${SCHEMA}.product_variants (variant_id)';
    END IF;

    -- ---- payments / shipments ---------------------------------------
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_payments_order_fk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.payments
                 ADD CONSTRAINT ${SCHEMA}_payments_order_fk
                 FOREIGN KEY (order_id, placed_at)
                 REFERENCES ${SCHEMA}.orders (order_id, placed_at) ON DELETE CASCADE';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${SCHEMA}_shipments_order_fk') THEN
        EXECUTE 'ALTER TABLE ${SCHEMA}.shipments
                 ADD CONSTRAINT ${SCHEMA}_shipments_order_fk
                 FOREIGN KEY (order_id, placed_at)
                 REFERENCES ${SCHEMA}.orders (order_id, placed_at) ON DELETE CASCADE';
    END IF;
END
$keys$;
