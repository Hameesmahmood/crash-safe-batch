-- Tables for the example workflow: importing transactions into a ledger.

CREATE TABLE accounts (
    id            int    PRIMARY KEY,
    balance_cents bigint NOT NULL DEFAULT 0
);

-- No unique constraint on (run_id, source_line) or txn_id, on purpose. Production code would add one
-- as a second line of defence. Leaving it out lets the crash test see a duplicate as a different
-- result, not hide it behind a constraint error.
CREATE TABLE ledger_entries (
    id                  bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              bigint  NOT NULL REFERENCES runs (id),
    source_line         int     NOT NULL,
    txn_id              text    NOT NULL,
    account_id          int     NOT NULL REFERENCES accounts (id),
    amount_cents        bigint  NOT NULL,
    balance_after_cents bigint  NOT NULL,
    -- Compensation never deletes. It posts a reversing entry, as a real ledger would.
    reversal            boolean NOT NULL DEFAULT false
);

CREATE INDEX ledger_entries_run ON ledger_entries (run_id, source_line);

CREATE TABLE statements (
    run_id        bigint NOT NULL REFERENCES runs (id),
    account_id    int    NOT NULL REFERENCES accounts (id),
    entry_count   int    NOT NULL,
    credits_cents bigint NOT NULL,
    debits_cents  bigint NOT NULL,
    closing_cents bigint NOT NULL,
    status        text   NOT NULL DEFAULT 'issued' CHECK (status IN ('issued', 'void')),
    PRIMARY KEY (run_id, account_id)
);
