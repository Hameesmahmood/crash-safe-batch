import { readFileSync } from "node:fs";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ledgerDigest } from "../src/example/ledger.ts";
import { createTestDatabase, type TestDatabase } from "./db.ts";
import { balancesFromCsv, baselineDigest, count, testRunner, writeCsv } from "./helpers.ts";
import { failsOnceAfterWrites } from "./workflows.ts";

describe("Runner, running the ledger import", () => {
  let file: string;
  let db: TestDatabase;
  beforeAll(async () => {
    file = await writeCsv(2000);
  });
  beforeEach(async () => {
    db = await createTestDatabase();
  });
  afterEach(() => db.drop());

  it("posts every transaction exactly once and finishes the run", async () => {
    const runner = testRunner(db);
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(await runner.store.get(id)).toMatchObject({ status: "succeeded", stepIndex: 1, leaseOwner: null, leaseExpiresAt: null });
    expect(await count(db, "SELECT * FROM ledger_entries")).toBe(2000);
    expect(await count(db, "SELECT DISTINCT source_line FROM ledger_entries")).toBe(2000);
    const steps = await db.pool.query("SELECT status, rows FROM run_steps WHERE run_id = $1", [id]);
    expect(steps.rows).toEqual([{ status: "done", rows: "2000" }]);
  });

  it("ends with the balances you get by adding up the CSV by hand", async () => {
    const runner = testRunner(db);
    await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    const { rows } = await db.pool.query<{ id: number; balance_cents: string }>("SELECT id, balance_cents FROM accounts");
    expect(new Map(rows.map((a) => [a.id, Number(a.balance_cents)]))).toEqual(balancesFromCsv(file));
  });

  it("records each entry's running balance in file order", async () => {
    const runner = testRunner(db);
    await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    const running = new Map<number, number>();
    const expected = readFileSync(file, "utf8").trim().split("\n").slice(1).map((line) => {
      const [, account, amount] = line.split(",").map(Number) as [number, number, number];
      running.set(account, (running.get(account) ?? 0) + amount);
      return running.get(account);
    });
    const { rows } = await db.pool.query<{ b: string }>("SELECT balance_after_cents AS b FROM ledger_entries ORDER BY source_line");
    expect(rows.map((r) => Number(r.b))).toEqual(expected);
  });

  it("resumes from the last checkpoint after a chunk fails, with the same result as a clean run", async () => {
    const sources: number[] = [];
    const runner = testRunner(db, { workflow: failsOnceAfterWrites(700, sources) });
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    // Read from the start once, then again from the last checkpoint (7 chunks of 100), never from 0.
    expect(sources).toEqual([0, 700]);
    expect(await runner.store.get(id)).toMatchObject({ status: "succeeded", attempt: 0, error: null });
    expect(await ledgerDigest(db.pool)).toBe(await baselineDigest(file));
  });

  it("never gives the same run to two workers", async () => {
    const claims: string[] = [];
    const log = (event: string, fields?: Record<string, unknown>) => {
      if (event === "claimed") claims.push(fields!.run as string);
    };
    const a = testRunner(db, { workerId: "a", log });
    const b = testRunner(db, { workerId: "b", log });
    const ids = await Promise.all([1, 2, 3, 4].map(() => a.store.create("ledger-import", { file })));

    await Promise.all([a.runUntilIdle(), b.runUntilIdle()]);

    expect(claims.sort()).toEqual([...ids].sort());
    for (const id of ids) expect(await count(db, "SELECT * FROM ledger_entries WHERE run_id = $1", [id])).toBe(2000);
  });

  it("leaves a run alone while its worker's lease is live, and takes it over once it expires", async () => {
    const runner = testRunner(db, { workerId: "second" });
    const id = await runner.store.create("ledger-import", { file });
    // The first worker claims the run and then goes silent: no checkpoints, so no lease renewals.
    await runner.store.claim("first", ["ledger-import"]);

    expect(await runner.runUntilIdle()).toBe(0);
    expect(await count(db, "SELECT * FROM ledger_entries")).toBe(0);

    await db.pool.query("UPDATE runs SET lease_expires_at = now() - interval '1 second'");
    expect(await runner.runUntilIdle()).toBe(1);
    expect(await runner.store.get(id)).toMatchObject({ status: "succeeded" });
    expect(await ledgerDigest(db.pool)).toBe(await baselineDigest(file));
  });

  it("never claims a finished run again", async () => {
    const runner = testRunner(db);
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();
    const finished = await runner.store.get(id);

    expect(await runner.runUntilIdle()).toBe(0);
    expect(await runner.store.get(id)).toEqual(finished);
  });
});
