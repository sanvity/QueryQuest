-- =====================================================================
-- QueryQuest :: 000_meta.sql
-- Pipeline control plane. Deliberately kept in its own schema so the
-- "12 tables" of the domain model stay 12 tables, and so an operator
-- can grant read on qq_meta without granting anything on the data.
--
-- Everything the ETL needs to be *idempotent* lives here: batch
-- registry, watermarks, rejects, DQ verdicts.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS qq_meta;

DO $enum$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'batch_state' AND n.nspname = 'qq_meta'
    ) THEN
        CREATE TYPE qq_meta.batch_state AS ENUM
            ('running','succeeded','failed','skipped_duplicate','quarantined');
        CREATE TYPE qq_meta.dq_severity AS ENUM ('warn','error');
    END IF;
END
$enum$;

-- ---------------------------------------------------------------------
-- etl_batches: the idempotency ledger.
--
-- batch_key is a deterministic fingerprint of the input (source name +
-- content hash of the file). Re-submitting identical input hits the
-- unique constraint, and the pipeline short-circuits to
-- 'skipped_duplicate' instead of re-processing. That, plus UPSERTs
-- keyed on natural keys, is what makes a re-run a no-op.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS qq_meta.etl_batches (
    batch_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_key       text        NOT NULL,
    source_name     text        NOT NULL,
    target_table    text        NOT NULL,
    state           qq_meta.batch_state NOT NULL DEFAULT 'running',
    rows_read       bigint      NOT NULL DEFAULT 0,
    rows_duplicate  bigint      NOT NULL DEFAULT 0,
    rows_rejected   bigint      NOT NULL DEFAULT 0,
    rows_inserted   bigint      NOT NULL DEFAULT 0,
    rows_updated    bigint      NOT NULL DEFAULT 0,
    rows_unchanged  bigint      NOT NULL DEFAULT 0,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    duration_ms     integer,
    error_text      text,
    CONSTRAINT etl_batches_key_uq UNIQUE (batch_key)
);

CREATE INDEX IF NOT EXISTS etl_batches_target_started_idx
    ON qq_meta.etl_batches (target_table, started_at DESC);

-- ---------------------------------------------------------------------
-- etl_watermarks: high-water mark per source, for incremental pulls.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS qq_meta.etl_watermarks (
    source_name     text        PRIMARY KEY,
    watermark_ts    timestamptz NOT NULL,
    last_batch_id   bigint      REFERENCES qq_meta.etl_batches (batch_id),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- etl_rejects: quarantine. A bad row does not kill the batch; it lands
-- here with the rule that rejected it and the raw payload, so it can be
-- replayed after the upstream fix.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS qq_meta.etl_rejects (
    reject_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id      bigint      NOT NULL REFERENCES qq_meta.etl_batches (batch_id) ON DELETE CASCADE,
    target_table  text        NOT NULL,
    rule_name     text        NOT NULL,
    reason        text        NOT NULL,
    natural_key   text,
    raw_payload   jsonb       NOT NULL,
    rejected_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS etl_rejects_batch_idx ON qq_meta.etl_rejects (batch_id);
CREATE INDEX IF NOT EXISTS etl_rejects_rule_idx  ON qq_meta.etl_rejects (rule_name, rejected_at DESC);

-- ---------------------------------------------------------------------
-- dq_results: one row per rule per batch. This is the audit trail that
-- answers "was the data good on the 14th?" months later.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS qq_meta.dq_results (
    dq_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id      bigint      NOT NULL REFERENCES qq_meta.etl_batches (batch_id) ON DELETE CASCADE,
    rule_name     text        NOT NULL,
    severity      qq_meta.dq_severity NOT NULL,
    rows_checked  bigint      NOT NULL,
    rows_failed   bigint      NOT NULL,
    fail_ratio    numeric(6,5) NOT NULL,
    passed        boolean     NOT NULL,
    detail        text,
    checked_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dq_results_batch_idx ON qq_meta.dq_results (batch_id);

-- ---------------------------------------------------------------------
-- query_benchmarks: every timing the bench harness produces is written
-- here, tagged with the schema variant it ran against. The numbers in
-- docs/BENCHMARKS.md are generated from this table -- nothing is
-- hand-typed.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS qq_meta.query_benchmarks (
    bench_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        text        NOT NULL,
    query_name    text        NOT NULL,
    variant       text        NOT NULL,          -- 'baseline' | 'optimized'
    repetitions   integer     NOT NULL,
    p50_ms        numeric(10,3) NOT NULL,
    p95_ms        numeric(10,3) NOT NULL,
    p99_ms        numeric(10,3) NOT NULL,
    mean_ms       numeric(10,3) NOT NULL,
    min_ms        numeric(10,3) NOT NULL,
    max_ms        numeric(10,3) NOT NULL,
    rows_returned bigint,
    shared_hit    bigint,
    shared_read   bigint,
    plan_json     jsonb,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS query_benchmarks_run_idx
    ON qq_meta.query_benchmarks (run_id, query_name, variant);

-- ---------------------------------------------------------------------
-- cache_stats: hit/miss counters flushed from the Redis layer, so the
-- throughput claim is backed by a table you can query.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS qq_meta.cache_stats (
    stat_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id         text        NOT NULL,
    workload       text        NOT NULL,
    duration_s     numeric(10,3) NOT NULL,
    queries_total  bigint      NOT NULL,
    queries_per_min numeric(12,2) NOT NULL,
    hits           bigint      NOT NULL,
    misses         bigint      NOT NULL,
    hit_ratio      numeric(6,5) NOT NULL,
    p50_ms         numeric(10,3) NOT NULL,
    p95_ms         numeric(10,3) NOT NULL,
    p99_ms         numeric(10,3) NOT NULL,
    concurrency    integer     NOT NULL,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
