/**
 * Synthetic ledger transactions. The same seed always gives the same file, so a crashed run and an
 * uninterrupted run can be compared row for row.
 */

export const CSV_HEADER = "txn_id,account_id,amount_cents,occurred_at";

export interface GenerateOptions {
  rows: number;
  seed: number;
  accounts?: number;
}

/** Yields the CSV one line at a time, header first, so large files never sit in memory. */
export function* generateTransactions({ rows, seed, accounts = 1000 }: GenerateOptions): Generator<string> {
  const random = mulberry32(seed);
  const start = Date.UTC(2026, 0, 1);
  let at = start;
  yield CSV_HEADER;
  for (let i = 1; i <= rows; i++) {
    const account = 1 + Math.floor(random() * accounts);
    // Mostly small debits and credits, with the occasional large credit.
    const credit = random() < 0.45;
    const size = random() < 0.03 ? 50_000 + Math.floor(random() * 450_000) : 100 + Math.floor(random() * 20_000);
    at += 1_000 + Math.floor(random() * 60_000);
    yield `t${String(i).padStart(7, "0")},${account},${credit ? size : -size},${new Date(at).toISOString()}`;
  }
}

/** A small, fast, seedable random number generator. Not for anything secret. */
export function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}
