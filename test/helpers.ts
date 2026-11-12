import { mkdtemp, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Runner, type RunnerOptions } from "../src/engine/runner.ts";
import type { Workflow } from "../src/engine/workflow.ts";
import { ledgerDigest, ledgerImport, type LedgerInput } from "../src/example/ledger.ts";
import { generateTransactions } from "../src/example/transactions.ts";
import { createTestDatabase, type TestDatabase } from "./db.ts";

export const quiet = () => {};

/** Writes a seeded CSV to a temporary folder and returns its path. */
export async function writeCsv(rows: number, seed = 1): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "csb-"));
  const file = join(dir, `tx-${rows}-${seed}.csv`);
  await writeFile(file, [...generateTransactions({ rows, seed })].join("\n") + "\n");
  return file;
}

/** A runner tuned for tests: silent, retries at once, short polls. */
export function testRunner(db: TestDatabase, options: Partial<RunnerOptions> & { workflow?: Workflow<LedgerInput> } = {}): Runner {
  const { workflow, ...rest } = options;
  return new Runner({
    pool: db.pool,
    workflows: [workflow ?? ledgerImport({ chunkSize: 100 })],
    retryDelayMs: () => 0,
    pollMs: 10,
    log: quiet,
    ...rest,
  });
}

const baselines = new Map<string, string>();

/** The digest of an import that was never interrupted, run in its own database. */
export async function baselineDigest(file: string, chunkSize = 100): Promise<string> {
  const key = `${file}:${chunkSize}`;
  if (!baselines.has(key)) {
    const db = await createTestDatabase();
    try {
      const runner = testRunner(db, { workflow: ledgerImport({ chunkSize }) });
      await runner.store.create("ledger-import", { file });
      await runner.runUntilIdle();
      baselines.set(key, await ledgerDigest(db.pool));
    } finally {
      await db.drop();
    }
  }
  return baselines.get(key)!;
}

/** Final balances worked out straight from the CSV, without the engine. */
export function balancesFromCsv(file: string): Map<number, number> {
  const balances = new Map<number, number>();
  for (const line of readFileSync(file, "utf8").trim().split("\n").slice(1)) {
    const [, account, amount] = line.split(",");
    balances.set(Number(account), (balances.get(Number(account)) ?? 0) + Number(amount));
  }
  return balances;
}

export async function count(db: TestDatabase, sql: string, params: unknown[] = []): Promise<number> {
  const { rows } = await db.pool.query<{ n: string }>(`SELECT count(*) AS n FROM (${sql}) q`, params);
  return Number(rows[0]!.n);
}

/** A promise you resolve from outside, for holding a worker at a chosen point. */
export function gate(): { wait: Promise<void>; open: () => void } {
  let open!: () => void;
  const wait = new Promise<void>((resolve) => (open = resolve));
  return { wait, open };
}

/** Polls until the condition holds, or fails after a few seconds. */
export async function eventually(condition: () => Promise<boolean>, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for a condition.");
    await new Promise((r) => setTimeout(r, 20));
  }
}
