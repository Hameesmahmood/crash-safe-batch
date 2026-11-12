-- The engine's own tables. They know nothing about any particular workflow.

CREATE TABLE runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow         text        NOT NULL,
    input            jsonb       NOT NULL DEFAULT '{}',
    status           text        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'succeeded', 'compensating', 'compensated', 'failed')),
    -- The step being run, or being undone while compensating.
    step_index       int         NOT NULL DEFAULT 0,
    -- How far the current step (or its compensation) has got, in input rows.
    cursor           bigint      NOT NULL DEFAULT 0,
    -- Bumped by every checkpoint. A write that expects an older version is from a stale worker.
    version          int         NOT NULL DEFAULT 0,
    -- Tries of the current step, for steps that allow retries.
    attempt          int         NOT NULL DEFAULT 0,
    lease_owner      text,
    lease_expires_at timestamptz,
    error            text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Workers look for runs that are waiting, or whose worker has stopped renewing its lease.
CREATE INDEX runs_claimable ON runs (lease_expires_at NULLS FIRST)
    WHERE status IN ('pending', 'running', 'compensating');

-- How far each step got, so compensation knows what to undo. A batch step that failed halfway is
-- 'partial': its committed chunks (the first `rows` input rows) still need undoing.
CREATE TABLE run_steps (
    run_id      bigint      NOT NULL REFERENCES runs (id),
    step_index  int         NOT NULL,
    status      text        NOT NULL CHECK (status IN ('done', 'partial', 'compensated')),
    rows        bigint      NOT NULL DEFAULT 0,
    finished_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, step_index)
);
