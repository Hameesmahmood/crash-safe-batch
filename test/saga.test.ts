import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ledgerDigest } from "../src/example/ledger.ts";
import { balances, baselineDigest, count, createFixture, ledger, testRunner, writeCsv, type Fixture } from "./helpers.ts";
import type { Runner } from "../src/engine/runner.ts";
import { duringUndo, notifyCrashesOnceAfterSending, postFailsForGoodAt, undoAlwaysFails, undoFailsOnceAfterWrites } from "./workflows.ts";

describe("compensation and the notify step", () => {
  let file: string;
  let db: Fixture;
  beforeAll(async () => {
    file = await writeCsv(2000);
  });
  beforeEach(async () => {
    db = await createFixture();
  });
  afterEach(() => db.drop());

  const allZero = async () => [...(await balances(db)).values()].every((b) => b === 0);
  const steps = async (id: string) =>
    (await db.pool.query("SELECT step_index, status, rows FROM run_steps WHERE run_id = $1 ORDER BY step_index", [id])).rows;

  it("sends the statements again after a crash, and the idempotency key makes the service accept them once", async () => {
    const runner = testRunner(db, { workflow: notifyCrashesOnceAfterSending(ledger(db)) });
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(await runner.store.get(id)).toMatchObject({ status: "succeeded" });
    expect(db.service.requests).toBe(2);
    expect([...db.service.accepted.keys()]).toEqual([`ledger-import/${id}`]);
    expect(await ledgerDigest(db.pool)).toBe(await baselineDigest(file));
  });

  it("retries a notify that fails for a while, then succeeds without undoing anything", async () => {
    db.service.fail(503, 3);
    const runner = testRunner(db);
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(await runner.store.get(id)).toMatchObject({ status: "succeeded", attempt: 0, error: null });
    expect(db.service.requests).toBe(4);
    expect(await count(db, "SELECT * FROM ledger_entries WHERE reversal")).toBe(0);
  });

  it("retries when the service can't be reached, and says why", async () => {
    await db.service.close();
    const runner = testRunner(db);
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(await runner.store.get(id)).toMatchObject({ status: "compensated", error: expect.stringMatching(/unreachable: ECONNREFUSED/) });
    expect(await allZero()).toBe(true);
  });

  it("doesn't retry a request the service rejects as invalid", async () => {
    db.service.fail(422);
    const runner = testRunner(db);
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(await runner.store.get(id)).toMatchObject({ status: "compensated", error: expect.stringContaining("422") });
    expect(db.service.requests).toBe(1);
    expect(await allZero()).toBe(true);
  });

  it("undoes the steps in reverse and records each one as compensated", async () => {
    db.service.fail(503);
    const order: string[] = [];
    const runner = testRunner(db, { log: (event, fields) => event === "step undone" && order.push(fields!.step as string) });
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(order).toEqual(["summarise", "post"]);
    expect(await steps(id)).toEqual([
      { step_index: 0, status: "compensated", rows: "2000" },
      { step_index: 1, status: "compensated", rows: "0" },
    ]);
    expect(await count(db, "SELECT * FROM statements WHERE status = 'void'")).toBe(await count(db, "SELECT * FROM accounts"));
    // Reversals carry their own running balance, which ends at zero for every account.
    const closing = await db.pool.query(
      "SELECT DISTINCT ON (account_id) balance_after_cents AS b FROM ledger_entries ORDER BY account_id, id DESC",
    );
    expect(closing.rows.every((r) => r.b === "0")).toBe(true);
  });

  it("undoes only the chunks a batch step committed before it failed", async () => {
    let undone = 0;
    const workflow = duringUndo(postFailsForGoodAt(ledger(db), 700), (_, rows) => (undone += rows));
    const runner = testRunner(db, { workflow });
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(undone).toBe(700);
    expect(await runner.store.get(id)).toMatchObject({ status: "compensated", error: "post: row rejected" });
    expect(await steps(id)).toEqual([{ step_index: 0, status: "compensated", rows: "700" }]);
    expect(await count(db, "SELECT * FROM ledger_entries WHERE NOT reversal")).toBe(700);
    expect(await count(db, "SELECT * FROM ledger_entries WHERE reversal")).toBe(700);
    expect(await count(db, "SELECT * FROM statements")).toBe(0);
    expect(await allZero()).toBe(true);
  });

  it("finishes the undo after a crash partway through it, without reversing anything twice", async () => {
    db.service.fail(503);
    const sources: number[] = [];
    const runner = testRunner(db, { workflow: undoFailsOnceAfterWrites(ledger(db), 700, sources) });
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    // Forward from 0, the undo from 0, then the undo again from its checkpoint.
    expect(sources).toEqual([0, 0, 700]);
    expect(await runner.store.get(id)).toMatchObject({ status: "compensated" });
    expect(await count(db, "SELECT * FROM ledger_entries WHERE reversal")).toBe(2000);
    expect(await count(db, "SELECT source_line FROM ledger_entries WHERE reversal GROUP BY source_line HAVING count(*) > 1")).toBe(0);
    expect(await allZero()).toBe(true);
  });

  it("hands a run back mid-undo on shutdown, and another worker finishes the undo", async () => {
    db.service.fail(503);
    const a: Runner = testRunner(db, { workerId: "a", workflow: duringUndo(ledger(db), (firstRow) => firstRow === 1000 && a.stop()) });
    const id = await a.store.create("ledger-import", { file });
    await a.runUntilIdle();
    expect(await a.store.get(id)).toMatchObject({ status: "compensating", stepIndex: 0, cursor: 1100, leaseOwner: null });

    const b = testRunner(db, { workerId: "b" });
    expect(await b.runUntilIdle()).toBe(1);
    expect(await b.store.get(id)).toMatchObject({ status: "compensated" });
    expect(await count(db, "SELECT * FROM ledger_entries WHERE reversal")).toBe(2000);
    expect(await allZero()).toBe(true);
  });

  it("stops and marks the run failed when an undo fails for good, leaving it for a person", async () => {
    db.service.fail(503);
    const runner = testRunner(db, { workflow: undoAlwaysFails(ledger(db)) });
    const id = await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();

    expect(await runner.store.get(id)).toMatchObject({
      status: "failed",
      leaseOwner: null,
      leaseExpiresAt: null,
      error: "undoing summarise: statements table is read-only",
    });
    expect(await runner.runUntilIdle()).toBe(0);
    // It stopped before undoing post, so nothing was reversed.
    expect(await count(db, "SELECT * FROM ledger_entries WHERE reversal")).toBe(0);
  });
});
