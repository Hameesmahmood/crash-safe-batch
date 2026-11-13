import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { parse } from "csv-parse";
import type { Pool, PoolClient } from "pg";
import { batchStep, defineWorkflow, step, type ChunkContext, type StepContext, type Workflow } from "../engine/workflow.ts";
import { notifyStatementService } from "./notify.ts";

export interface LedgerInput {
  /** Path to a CSV in the format `generateTransactions` writes. */
  file: string;
}

export interface Transaction {
  /** 1-based position in the file, not counting the header. */
  line: number;
  txnId: string;
  accountId: number;
  amountCents: number;
  occurredAt: string;
}

/**
 * Streams transactions from the CSV, skipping the first `from`. Skipping re-parses those lines, which
 * is cheap next to writing them, and a row count can't land mid-row the way a byte offset can.
 */
export async function* readTransactions(
  file: string,
  from: number,
  options: { onParsed?: () => void } = {},
): AsyncGenerator<Transaction> {
  const input = createReadStream(file);
  // Line 1 is the header, so data line N is file line N + 1. `onParsed` lets tests see how far the
  // parser has got ahead of the worker.
  const onParsed = options.onParsed;
  const parser = input.pipe(parse({ from_line: from + 2, on_record: onParsed ? (record) => (onParsed(), record) : undefined }));
  let line = from;
  try {
    for await (const record of parser as AsyncIterable<string[]>) {
      line++;
      const [txnId, accountId, amountCents, occurredAt] = record;
      yield { line, txnId: txnId!, accountId: Number(accountId), amountCents: Number(amountCents), occurredAt: occurredAt! };
    }
  } finally {
    // Stopping early (shutdown, failure) must close the file too, not only the parser.
    input.destroy();
    parser.destroy();
  }
}

interface Posting {
  line: number;
  txnId: string;
  accountId: number;
  amountCents: number;
}

/**
 * Writes ledger entries, each with its account's running balance after it, and updates the balances.
 * Balances are read with FOR UPDATE, worked out in order in memory, then written in two statements.
 */
async function post(tx: PoolClient, runId: string, postings: Posting[], reversal: boolean): Promise<void> {
  if (postings.length === 0) return;
  // Locking in id order means two runs touching the same accounts can't deadlock.
  const accountIds = [...new Set(postings.map((p) => p.accountId))].sort((a, b) => a - b);

  await tx.query("INSERT INTO accounts (id) SELECT unnest($1::int[]) ON CONFLICT DO NOTHING", [accountIds]);
  const { rows: current } = await tx.query<{ id: number; balance_cents: string }>(
    "SELECT id, balance_cents FROM accounts WHERE id = ANY($1::int[]) ORDER BY id FOR UPDATE",
    [accountIds],
  );
  const balances = new Map(current.map((a) => [a.id, Number(a.balance_cents)]));

  const after = postings.map((p) => {
    const balance = balances.get(p.accountId)! + p.amountCents;
    balances.set(p.accountId, balance);
    return balance;
  });

  await tx.query(
    `INSERT INTO ledger_entries (run_id, source_line, txn_id, account_id, amount_cents, balance_after_cents, reversal)
     SELECT $1, *, $7 FROM unnest($2::int[], $3::text[], $4::int[], $5::bigint[], $6::bigint[])`,
    [
      runId,
      postings.map((p) => p.line),
      postings.map((p) => p.txnId),
      postings.map((p) => p.accountId),
      postings.map((p) => p.amountCents),
      after,
      reversal,
    ],
  );
  await tx.query(
    `UPDATE accounts a SET balance_cents = v.balance
     FROM unnest($1::int[], $2::bigint[]) AS v(id, balance)
     WHERE a.id = v.id`,
    [[...balances.keys()], [...balances.values()]],
  );
}

/** Posts a chunk of transactions: one ledger entry each. */
export async function postTransactions(rows: Transaction[], ctx: ChunkContext<LedgerInput>): Promise<void> {
  await post(ctx.tx, ctx.run.id, rows, false);
}

/**
 * Undoes a chunk by posting a reversing entry for each entry the chunk posted. It reverses what the
 * ledger says was posted, not what the file says, and it never deletes, as a real ledger wouldn't.
 */
export async function reverseTransactions(rows: Transaction[], ctx: ChunkContext<LedgerInput>): Promise<void> {
  const { rows: posted } = await ctx.tx.query<{ source_line: number; txn_id: string; account_id: number; amount_cents: string }>(
    `SELECT source_line, txn_id, account_id, amount_cents FROM ledger_entries
     WHERE run_id = $1 AND NOT reversal AND source_line = ANY($2::int[])
     ORDER BY source_line, id`,
    [ctx.run.id, rows.map((r) => r.line)],
  );
  const reversals = posted.map((e) => ({ line: e.source_line, txnId: e.txn_id, accountId: e.account_id, amountCents: -Number(e.amount_cents) }));
  await post(ctx.tx, ctx.run.id, reversals, true);
}

/** One statement per account the run touched: entry count, money in, money out, closing balance. */
export async function buildStatements({ tx, run }: StepContext<LedgerInput>): Promise<void> {
  await tx.query(
    `INSERT INTO statements (run_id, account_id, entry_count, credits_cents, debits_cents, closing_cents)
     SELECT $1, account_id, count(*),
            coalesce(sum(amount_cents) FILTER (WHERE amount_cents > 0), 0),
            coalesce(-sum(amount_cents) FILTER (WHERE amount_cents < 0), 0),
            (array_agg(balance_after_cents ORDER BY source_line DESC, id DESC))[1]
     FROM ledger_entries
     WHERE run_id = $1 AND NOT reversal
     GROUP BY account_id`,
    [run.id],
  );
}

/** Statements are never deleted either. Undoing them marks them void. */
export async function voidStatements({ tx, run }: StepContext<LedgerInput>): Promise<void> {
  await tx.query("UPDATE statements SET status = 'void' WHERE run_id = $1", [run.id]);
}

export interface LedgerOptions {
  /** Rows per chunk. Defaults to 500. */
  chunkSize?: number;
  /** Where the statement service listens, such as http://127.0.0.1:4100. */
  statementServiceUrl: string;
  /** Tries of the notify step before the import is undone. Defaults to 5. */
  notifyAttempts?: number;
}

export function ledgerImport(options: LedgerOptions): Workflow<LedgerInput> {
  return defineWorkflow<LedgerInput>("ledger-import", [
    batchStep<LedgerInput, Transaction>("post", {
      size: options.chunkSize ?? 500,
      source: (input, from) => readTransactions(input.file, from),
      handle: postTransactions,
      compensate: reverseTransactions,
    }),
    step<LedgerInput>("summarise", { run: buildStatements, compensate: voidStatements }),
    // Nothing to undo: if notify fails for good, the service never accepted the statements.
    step<LedgerInput>("notify", {
      attempts: options.notifyAttempts ?? 5,
      run: (ctx) => notifyStatementService(options.statementServiceUrl, ctx),
    }),
  ]);
}

/**
 * A fingerprint of everything the import wrote, leaving out values that legitimately differ between
 * runs (generated IDs, run IDs). Two databases with the same digest hold the same ledger.
 */
export async function ledgerDigest(pool: Pool): Promise<string> {
  const hash = createHash("sha256");
  const entries = await pool.query({
    text: `SELECT source_line, txn_id, account_id, amount_cents, balance_after_cents, reversal
           FROM ledger_entries ORDER BY source_line, reversal, id`,
    rowMode: "array",
  });
  for (const row of entries.rows) hash.update(row.join(",") + "\n");
  hash.update("accounts\n");
  const accounts = await pool.query({ text: "SELECT id, balance_cents FROM accounts ORDER BY id", rowMode: "array" });
  for (const row of accounts.rows) hash.update(row.join(",") + "\n");
  hash.update("statements\n");
  const statements = await pool.query({
    text: `SELECT account_id, entry_count, credits_cents, debits_cents, closing_cents, status
           FROM statements ORDER BY account_id, run_id`,
    rowMode: "array",
  });
  for (const row of statements.rows) hash.update(row.join(",") + "\n");
  return hash.digest("hex");
}
