import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { parse } from "csv-parse";
import type { Pool } from "pg";
import { batchStep, defineWorkflow, type ChunkContext, type Workflow } from "../engine/workflow.ts";

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
export async function* readTransactions(file: string, from: number): AsyncGenerator<Transaction> {
  const input = createReadStream(file);
  // Line 1 is the header, so data line N is file line N + 1.
  const parser = input.pipe(parse({ from_line: from + 2 }));
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

/**
 * Posts a chunk of transactions: one ledger entry each, with the account's running balance after it.
 * Balances are read with FOR UPDATE, worked out in order in memory, then written in two statements.
 */
export async function postTransactions(rows: Transaction[], ctx: ChunkContext<LedgerInput>): Promise<void> {
  const { tx, run } = ctx;
  const accountIds = [...new Set(rows.map((r) => r.accountId))].sort((a, b) => a - b);

  await tx.query("INSERT INTO accounts (id) SELECT unnest($1::int[]) ON CONFLICT DO NOTHING", [accountIds]);
  const { rows: current } = await tx.query<{ id: number; balance_cents: string }>(
    "SELECT id, balance_cents FROM accounts WHERE id = ANY($1::int[]) ORDER BY id FOR UPDATE",
    [accountIds],
  );
  const balances = new Map(current.map((a) => [a.id, Number(a.balance_cents)]));

  const after = rows.map((r) => {
    const balance = balances.get(r.accountId)! + r.amountCents;
    balances.set(r.accountId, balance);
    return balance;
  });

  await tx.query(
    `INSERT INTO ledger_entries (run_id, source_line, txn_id, account_id, amount_cents, balance_after_cents)
     SELECT $1, * FROM unnest($2::int[], $3::text[], $4::int[], $5::bigint[], $6::bigint[])`,
    [run.id, rows.map((r) => r.line), rows.map((r) => r.txnId), rows.map((r) => r.accountId), rows.map((r) => r.amountCents), after],
  );
  await tx.query(
    `UPDATE accounts a SET balance_cents = v.balance
     FROM unnest($1::int[], $2::bigint[]) AS v(id, balance)
     WHERE a.id = v.id`,
    [[...balances.keys()], [...balances.values()]],
  );
}

export function ledgerImport(options: { chunkSize?: number } = {}): Workflow<LedgerInput> {
  return defineWorkflow<LedgerInput>("ledger-import", [
    batchStep<LedgerInput, Transaction>("post", {
      size: options.chunkSize ?? 500,
      source: (input, from) => readTransactions(input.file, from),
      handle: postTransactions,
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
  return hash.digest("hex");
}
