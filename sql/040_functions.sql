-- =====================================================================
-- QueryQuest :: 040_functions.sql
-- Transactional inventory primitives.
--
-- These are the database-side half of the concurrency story. The
-- application-side half (lock ordering across multiple SKUs, retry with
-- backoff, NOWAIT / SKIP LOCKED policies) lives in
-- queryquest/concurrency/inventory.py.
--
-- The invariant being defended: qty_reserved <= qty_on_hand, always,
-- under any interleaving of concurrent reservations. It is enforced in
-- three independent layers, deliberately:
--
--   1. SELECT ... FOR UPDATE serialises writers on the contended row.
--   2. The UPDATE re-checks availability inside the same transaction.
--   3. CHECK (qty_reserved <= qty_on_hand) is the backstop -- if the
--      first two are ever wrong, the transaction aborts rather than
--      overselling.
--
-- A read-modify-write without (1) is the textbook lost update: two
-- sessions both read qty_available = 1, both decide "yes", both write.
-- tests/test_concurrency.py demonstrates that failure directly.
-- =====================================================================

-- ---------------------------------------------------------------------
-- reserve_inventory: take a row lock, verify, reserve.
--
-- Returns TRUE if the reservation succeeded, FALSE if there was not
-- enough stock. Insufficient stock is an expected business outcome, not
-- an exception -- raising here would force every caller into a
-- try/except for the normal case.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq.reserve_inventory(p_variant_id bigint, p_qty integer)
RETURNS boolean
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_available integer;
BEGIN
    IF p_qty <= 0 THEN
        RAISE EXCEPTION 'reserve_inventory: quantity must be positive, got %', p_qty
            USING ERRCODE = 'check_violation';
    END IF;

    -- FOR UPDATE: every other transaction touching this SKU now queues
    -- behind us until we commit or roll back. The lock is on one row, so
    -- reservations for other SKUs proceed in parallel unaffected.
    SELECT qty_available INTO v_available
    FROM qq.inventory
    WHERE variant_id = p_variant_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'reserve_inventory: unknown variant_id %', p_variant_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;

    IF v_available < p_qty THEN
        RETURN false;
    END IF;

    UPDATE qq.inventory
       SET qty_reserved = qty_reserved + p_qty,
           row_version  = row_version + 1,
           updated_at   = now()
     WHERE variant_id = p_variant_id;

    RETURN true;
END
$fn$;

-- ---------------------------------------------------------------------
-- reserve_inventory_nowait: same contract, but fails fast instead of
-- queueing. For latency-sensitive callers that would rather retry than
-- block behind a long transaction.
-- Raises SQLSTATE 55P03 (lock_not_available) when the row is busy.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq.reserve_inventory_nowait(p_variant_id bigint, p_qty integer)
RETURNS boolean
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_available integer;
BEGIN
    SELECT qty_available INTO v_available
    FROM qq.inventory
    WHERE variant_id = p_variant_id
    FOR UPDATE NOWAIT;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'reserve_inventory_nowait: unknown variant_id %', p_variant_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;

    IF v_available < p_qty THEN
        RETURN false;
    END IF;

    UPDATE qq.inventory
       SET qty_reserved = qty_reserved + p_qty,
           row_version  = row_version + 1,
           updated_at   = now()
     WHERE variant_id = p_variant_id;

    RETURN true;
END
$fn$;

-- ---------------------------------------------------------------------
-- reserve_many: reserve several SKUs atomically.
--
-- The ORDER BY on the locking SELECT is the whole trick. Two carts
-- containing the same two SKUs in opposite order will deadlock if each
-- locks in cart order; locking in a globally consistent order
-- (ascending variant_id) makes that deadlock impossible rather than
-- merely unlikely.
--
-- Either every line is reserved or none is: a partial reservation is a
-- silently broken cart.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq.reserve_many(p_variant_ids bigint[], p_quantities integer[])
RETURNS boolean
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_locked   record;
    v_idx      integer;
    v_want     integer;
BEGIN
    IF array_length(p_variant_ids, 1) IS DISTINCT FROM array_length(p_quantities, 1) THEN
        RAISE EXCEPTION 'reserve_many: variant/quantity arrays must be the same length'
            USING ERRCODE = 'check_violation';
    END IF;

    -- Deterministic global lock order.
    FOR v_locked IN
        SELECT i.variant_id, i.qty_available
        FROM qq.inventory i
        WHERE i.variant_id = ANY (p_variant_ids)
        ORDER BY i.variant_id
        FOR UPDATE
    LOOP
        v_want := 0;
        FOR v_idx IN 1 .. array_length(p_variant_ids, 1) LOOP
            IF p_variant_ids[v_idx] = v_locked.variant_id THEN
                v_want := v_want + p_quantities[v_idx];
            END IF;
        END LOOP;

        IF v_locked.qty_available < v_want THEN
            RETURN false;   -- caller's transaction rolls back; nothing reserved
        END IF;

        UPDATE qq.inventory
           SET qty_reserved = qty_reserved + v_want,
               row_version  = row_version + 1,
               updated_at   = now()
         WHERE variant_id = v_locked.variant_id;
    END LOOP;

    RETURN true;
END
$fn$;

-- ---------------------------------------------------------------------
-- release_inventory: cart abandoned or reservation expired.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq.release_inventory(p_variant_id bigint, p_qty integer)
RETURNS boolean
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_reserved integer;
BEGIN
    SELECT qty_reserved INTO v_reserved
    FROM qq.inventory WHERE variant_id = p_variant_id FOR UPDATE;

    IF NOT FOUND OR v_reserved < p_qty THEN
        RETURN false;
    END IF;

    UPDATE qq.inventory
       SET qty_reserved = qty_reserved - p_qty,
           row_version  = row_version + 1,
           updated_at   = now()
     WHERE variant_id = p_variant_id;

    RETURN true;
END
$fn$;

-- ---------------------------------------------------------------------
-- fulfil_reservation: stock physically leaves the warehouse. Reserved
-- and on-hand both drop, so qty_available is unchanged -- which is
-- correct, because the units were already spoken for.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq.fulfil_reservation(p_variant_id bigint, p_qty integer)
RETURNS boolean
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_reserved integer;
BEGIN
    SELECT qty_reserved INTO v_reserved
    FROM qq.inventory WHERE variant_id = p_variant_id FOR UPDATE;

    IF NOT FOUND OR v_reserved < p_qty THEN
        RETURN false;
    END IF;

    UPDATE qq.inventory
       SET qty_reserved = qty_reserved - p_qty,
           qty_on_hand  = qty_on_hand  - p_qty,
           row_version  = row_version + 1,
           updated_at   = now()
     WHERE variant_id = p_variant_id;

    RETURN true;
END
$fn$;

-- ---------------------------------------------------------------------
-- claim_restock_batch: worker queue pattern.
--
-- SKIP LOCKED is the opposite policy to FOR UPDATE: instead of queueing
-- behind a busy row, step over it. N restock workers can drain the same
-- queue concurrently with zero contention and zero double-processing.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq.claim_restock_batch(p_limit integer DEFAULT 50)
RETURNS TABLE (variant_id bigint, shortfall integer)
LANGUAGE sql
AS $fn$
    SELECT i.variant_id, (i.reorder_point - i.qty_available) AS shortfall
    FROM qq.inventory i
    WHERE i.qty_available <= i.reorder_point
    ORDER BY (i.reorder_point - i.qty_available) DESC
    LIMIT p_limit
    FOR UPDATE SKIP LOCKED;
$fn$;

-- ---------------------------------------------------------------------
-- unsafe_reserve_inventory: the bug, preserved on purpose.
--
-- Identical to reserve_inventory but with no FOR UPDATE. Used only by
-- tests/test_concurrency.py to demonstrate that the lost update is real
-- and that the locking version is what prevents it. Never call this
-- from application code.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION qq.unsafe_reserve_inventory(p_variant_id bigint, p_qty integer)
RETURNS boolean
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_available integer;
BEGIN
    SELECT qty_available INTO v_available
    FROM qq.inventory WHERE variant_id = p_variant_id;   -- no lock: race lives here

    IF NOT FOUND OR v_available < p_qty THEN
        RETURN false;
    END IF;

    PERFORM pg_sleep(0.01);   -- widen the window so the race is reliable

    UPDATE qq.inventory
       SET qty_reserved = qty_reserved + p_qty,
           row_version  = row_version + 1
     WHERE variant_id = p_variant_id;

    RETURN true;
END
$fn$;
