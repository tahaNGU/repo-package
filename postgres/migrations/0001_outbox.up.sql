-- Transactional outbox table.
--
-- {{table}} is the (optionally schema-qualified) table name and {{prefix}} is
-- the unqualified name used for constraint and index names. The default
-- rendering uses "outbox_messages" for both.
--
-- Every statement is idempotent, so this file is safe to re-apply.

CREATE TABLE IF NOT EXISTS {{table}} (
    -- Identity. The id is supplied by the application before COMMIT (UUIDv7),
    -- so it is stable across retries and usable as a deduplication key by
    -- consumers. seq gives a monotonic enqueue order, used for ordered
    -- delivery and for paging.
    id               uuid        PRIMARY KEY,
    seq              bigserial   NOT NULL,

    -- Payload. Bytes are stored verbatim; the outbox never parses them.
    event_type       text        NOT NULL,
    payload          bytea       NOT NULL,
    content_type     text        NOT NULL DEFAULT '',
    headers          jsonb       NOT NULL DEFAULT '{}'::jsonb,

    -- Routing and correlation metadata.
    aggregate_type   text        NOT NULL DEFAULT '',
    aggregate_id     text        NOT NULL DEFAULT '',
    ordering_key     text,

    -- Delivery state.
    state            text        NOT NULL DEFAULT 'pending',
    attempts         integer     NOT NULL DEFAULT 0,

    -- available_at is when a worker may take this row, and it carries two
    -- meanings so that claiming needs exactly one index and one predicate:
    --
    --   state = 'pending': when the next delivery attempt may start (set by
    --                      the retry backoff, or by the application to delay
    --                      first delivery).
    --   state = 'leased':  when the current lease expires, i.e. when another
    --                      worker may reclaim the row.
    --
    -- For published and failed rows it is simply the last value it held.
    available_at     timestamptz NOT NULL DEFAULT now(),

    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    published_at     timestamptz,

    -- Lease. lease_epoch is the fencing token: it is incremented on every
    -- claim, and a worker may only update a row whose epoch still matches the
    -- one it was handed. lease_owner is for operators.
    lease_owner      text,
    lease_epoch      bigint      NOT NULL DEFAULT 0,

    -- Operator-facing failure summary. Truncated and, if the application
    -- configures a redactor, redacted before it is stored.
    last_error       text,
    last_error_kind  text,

    CONSTRAINT {{prefix}}_state_check
        CHECK (state IN ('pending', 'leased', 'published', 'failed')),
    CONSTRAINT {{prefix}}_attempts_check
        CHECK (attempts >= 0),
    CONSTRAINT {{prefix}}_lease_check
        CHECK ((state = 'leased') = (lease_owner IS NOT NULL)),
    CONSTRAINT {{prefix}}_published_check
        CHECK ((state = 'published') = (published_at IS NOT NULL))
);

-- The claim index. One partial index serves the whole hot path:
--
--   * claiming due messages and reclaiming expired leases, in one
--     index-ordered scan with no sort (the LIMIT stops the scan early);
--   * counting pending and leased rows for queue-depth metrics;
--   * min(available_at) for publishing lag.
--
-- It is partial, so published rows -- which are the bulk of a healthy table --
-- are not in it at all, and its size tracks the backlog rather than the table.
CREATE INDEX IF NOT EXISTS {{prefix}}_due_idx
    ON {{table}} (available_at, seq)
    WHERE state IN ('pending', 'leased');

-- Listing and counting terminal failures.
CREATE INDEX IF NOT EXISTS {{prefix}}_failed_idx
    ON {{table}} (seq)
    WHERE state = 'failed';

-- Retention sweeps. Drop this index if you never delete published rows.
CREATE INDEX IF NOT EXISTS {{prefix}}_published_idx
    ON {{table}} (published_at)
    WHERE state = 'published';
