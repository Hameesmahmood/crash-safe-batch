import { PermanentError, type StepContext } from "../engine/workflow.ts";
import type { LedgerInput } from "./ledger.ts";

export interface StatementBatch {
  run: string;
  accounts: number;
  entries: number;
  creditsCents: number;
  debitsCents: number;
}

/**
 * Tells the statement service that a run's statements are ready.
 *
 * The call is at-least-once: if the worker crashes after the service accepted the batch but before
 * the step's checkpoint commits, the step runs again and sends it again. The idempotency key is the
 * same every time for a run, so the service can recognise the repeat and accept it only once.
 *
 * Server errors and timeouts are worth retrying. A 4xx means the request itself is wrong, so it
 * fails for good straight away.
 */
export async function notifyStatementService(baseUrl: string, { tx, run }: StepContext<LedgerInput>): Promise<void> {
  const { rows } = await tx.query<{ accounts: string; entries: string | null; credits: string | null; debits: string | null }>(
    `SELECT count(*) AS accounts, sum(entry_count) AS entries, sum(credits_cents) AS credits, sum(debits_cents) AS debits
     FROM statements WHERE run_id = $1 AND status = 'issued'`,
    [run.id],
  );
  const totals = rows[0]!;
  const batch: StatementBatch = {
    run: run.id,
    accounts: Number(totals.accounts),
    entries: Number(totals.entries ?? 0),
    creditsCents: Number(totals.credits ?? 0),
    debitsCents: Number(totals.debits ?? 0),
  };

  const response = await fetch(new URL("/statements", baseUrl), {
    method: "POST",
    headers: { "content-type": "application/json", "idempotency-key": `${run.workflow}/${run.id}` },
    body: JSON.stringify(batch),
    signal: AbortSignal.timeout(5_000),
  });
  // Read the body either way, so the connection can be reused.
  const text = await response.text();
  if (response.ok) return;
  const message = `statement service answered ${response.status}: ${text.slice(0, 200)}`;
  if (response.status >= 400 && response.status < 500 && response.status !== 408 && response.status !== 429) {
    throw new PermanentError(message);
  }
  throw new Error(message);
}
