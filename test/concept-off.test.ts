// Each guarantee, shown twice: once as built, and once switched off to show what it prevents.
// The switched-off versions exist only here, never in src/.
import { setTimeout as sleep } from "node:timers/promises";
import type { Pool, PoolClient } from "pg";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { Runner } from "../src/engine/runner.ts";
import { RunStore, type Changes, type RunRow } from "../src/engine/store.ts";
import { defineWorkflow, type BatchStep } from "../src/engine/workflow.ts";
import { ledgerDigest, readTransactions, type LedgerInput, type Transaction } from "../src/example/ledger.ts";
import {
  balances,
  balancesFromCsv,
  baselineDigest,
  count,
  createFixture,
  eventually,
  gate,
  ledger,
  testRunner,
  writeCsv,
  type Fixture,
} from "./helpers.ts";
import { duringChunk, failsOnceAfterWrites, readAhead, recordsSources, stallsAt, withoutCompensation } from "./workflows.ts";

/** Claims any run whose lease is free and sets it running, whatever state it was in. */
class NoStateMachineStore extends RunStore {
  override async claim(owner: string, workflows: readonly string[]): Promise<RunRow | undefined> {
    const { rows } = await this.pool.query(
      `UPDATE runs SET status = 'running', lease_owner = $1, lease_expires_at = now() + interval '30 seconds',
              version = version + 1, updated_at = now()
       WHERE id = (SELECT id FROM runs
                   WHERE workflow = ANY($2) AND (lease_expires_at IS NULL OR lease_expires_at <= now())
                   ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
       RETURNING id`,
      [owner, workflows],
    );
    return rows[0] ? this.get(String(rows[0].id)) : undefined;
  }
}

/** Commits a chunk's writes, then saves the checkpoint in a second transaction. */
class SeparateCheckpointStore extends RunStore {
  private crashed = false;
  private readonly crashAtCursor: number;

  constructor(pool: Pool, crashAtCursor: number) {
    super(pool, 30_000);
    this.crashAtCursor = crashAtCursor;
  }

  override async commitChunk(run: RunRow, nextCursor: number, work: (tx: PoolClient) => Promise<void>): Promise<RunRow> {
    await this.transaction(work);
    if (run.cursor === this.crashAtCursor && !this.crashed) {
      this.crashed = true;
      throw new Error("simulated crash between the writes and the checkpoint");
    }
    return this.transaction((tx) => this.advance(tx, run, { cursor: nextCursor, lease: "renew" }));
  }
}

/** Writes to the run whatever its version is now: last writer wins. */
class BlindWriteStore extends RunStore {
  protected override async advance(db: Pool | PoolClient, run: RunRow, changes: Changes): Promise<RunRow> {
    const { rows } = await db.query("SELECT version, status FROM runs WHERE id = $1", [run.id]);
    return super.advance(db, { ...run, version: rows[0].version, status: rows[0].status }, changes);
  }
}

/** On shutdown, just stops: the run keeps this worker's lease until it runs out. */
class NoHandoffStore extends RunStore {
  override async release(run: RunRow): Promise<RunRow> {
    return run;
  }
}

describe("each guarantee, switched on and off", () => {
  let file: string;
  let db: Fixture;
  beforeAll(async () => {
    file = await writeCsv(2000);
  });
  beforeEach(async () => {
    db = await createFixture();
  });
  afterEach(() => db.drop());

  describe("the state machine", () => {
    // notify fails for good, so the run is undone: every posting reversed, balances back to zero.
    async function compensatedRun(): Promise<string> {
      db.service.fail(503);
      const runner = testRunner(db);
      const id = await runner.store.create("ledger-import", { file });
      await runner.runUntilIdle();
      expect(await runner.store.get(id)).toMatchObject({ status: "compensated" });
      return id;
    }

    it("with it, a compensated run is never picked up again", async () => {
      const id = await compensatedRun();
      const before = await ledgerDigest(db.pool);

      const again = testRunner(db, { workerId: "later" });
      expect(await again.runUntilIdle()).toBe(0);
      expect(await again.store.get(id)).toMatchObject({ status: "compensated" });
      expect(await ledgerDigest(db.pool)).toBe(before);
    });

    it("switched off, a worker resumes the compensated run and posts every row again", async () => {
      const id = await compensatedRun();

      // Stop the worker once it has re-posted the whole file, before it goes any further.
      const naive: Runner = testRunner(db, {
        workerId: "naive",
        store: new NoStateMachineStore(db.pool, 30_000),
        workflow: duringChunk(ledger(db), (firstRow, rows) => firstRow + rows === 2000 && naive.stop()),
      });
      await naive.runUntilIdle();

      expect(await naive.store.get(id)).toMatchObject({ status: "running" });
      expect(await count(db, "SELECT * FROM ledger_entries WHERE NOT reversal")).toBe(4000);
      expect(await balances(db)).toEqual(balancesFromCsv(file));
    });
  });

  describe("checkpoint in the same transaction as the chunk's writes", () => {
    it("with it, a crash after the writes loses nothing and repeats nothing", async () => {
      const runner = testRunner(db, { workflow: failsOnceAfterWrites(ledger(db), 700) });
      await runner.store.create("ledger-import", { file });
      await runner.runUntilIdle();

      expect(await count(db, "SELECT * FROM ledger_entries")).toBe(2000);
      expect(await ledgerDigest(db.pool)).toBe(await baselineDigest(file));
    });

    it("switched off, a crash between the writes and the checkpoint posts that chunk twice", async () => {
      const runner = testRunner(db, { store: new SeparateCheckpointStore(db.pool, 700) });
      await runner.store.create("ledger-import", { file });
      await runner.runUntilIdle();

      expect(await count(db, "SELECT * FROM ledger_entries")).toBe(2100);
      expect(await count(db, "SELECT source_line FROM ledger_entries GROUP BY source_line HAVING count(*) > 1")).toBe(100);
      expect(await ledgerDigest(db.pool)).not.toBe(await baselineDigest(file));
    });
  });

  describe("saga compensation", () => {
    it("with it, when notify fails for good, every posting is reversed and every balance is back to zero", async () => {
      db.service.fail(503);
      const runner = testRunner(db);
      const id = await runner.store.create("ledger-import", { file });
      await runner.runUntilIdle();

      expect(await runner.store.get(id)).toMatchObject({ status: "compensated", leaseOwner: null, error: expect.stringContaining("503") });
      expect(db.service.requests).toBe(5);
      expect(db.service.accepted.size).toBe(0);
      expect(await count(db, "SELECT * FROM ledger_entries WHERE reversal")).toBe(2000);
      expect([...(await balances(db)).values()].every((b) => b === 0)).toBe(true);
      expect(await count(db, "SELECT * FROM statements WHERE status = 'issued'")).toBe(0);
    });

    it("switched off, the run gives up with the balances changed and no statement sent", async () => {
      db.service.fail(503);
      const runner = testRunner(db, { workflow: withoutCompensation(ledger(db)) });
      await runner.store.create("ledger-import", { file });
      await runner.runUntilIdle();

      expect(db.service.accepted.size).toBe(0);
      expect(await count(db, "SELECT * FROM ledger_entries WHERE reversal")).toBe(0);
      expect(await balances(db)).toEqual(balancesFromCsv(file));
      expect(await count(db, "SELECT * FROM statements WHERE status = 'issued'")).toBeGreaterThan(0);
    });
  });

  describe("backpressure", () => {
    // Imports a larger file against a deliberately slow database, and tracks the most rows the parser
    // ever had out ahead of the last commit.
    async function peakRowsAhead(read: (file: string, from: number, onParsed: () => void) => AsyncIterable<Transaction>) {
      const big = await writeCsv(20_000, 2);
      let parsed = 0;
      let peak = 0;
      const base = ledger(db, 100);
      const post = base.steps[0] as BatchStep<LedgerInput, Transaction>;
      const slowPost: BatchStep<LedgerInput, Transaction> = {
        ...post,
        source: (input, from) => read(input.file, from, () => parsed++),
        handle: async (rows, ctx) => {
          peak = Math.max(peak, parsed - ctx.firstRow);
          await sleep(2);
          await post.handle(rows, ctx);
        },
      };
      const workflow = defineWorkflow(base.name, [slowPost, ...base.steps.slice(1)]);
      const runner = testRunner(db, { workflow });
      const id = await runner.store.create("ledger-import", { file: big });
      await runner.runUntilIdle();
      expect(await runner.store.get(id)).toMatchObject({ status: "succeeded" });
      return peak;
    }

    it("with it, the parser stays within a chunk plus the stream's buffers of the worker", async () => {
      const peak = await peakRowsAhead((file, from, onParsed) => readTransactions(file, from, { onParsed }));
      // Measured at 1,605: one chunk of 100 plus one 64 KiB read of the file (about 1,500 rows).
      expect(peak).toBeLessThan(2_000);
    });

    it("switched off, the whole file ends up in memory ahead of the worker", async () => {
      const peak = await peakRowsAhead((file, from, onParsed) => readAhead(readTransactions(file, from, { onParsed })));
      // Measured at 19,800: everything but the first chunk.
      expect(peak).toBeGreaterThan(19_000);
    });
  });

  describe("optimistic locking on the run's version", () => {
    // A worker commits five chunks, then freezes mid-chunk. Its lease runs out, a second worker takes
    // the run over and finishes it, and then the first worker wakes up and tries to carry on.
    async function staleWorker(store?: RunStore) {
      const hold = gate();
      const events: string[] = [];
      const stale = testRunner(db, { workerId: "stale", workflow: stallsAt(ledger(db), 551, hold.wait), store, log: (e) => events.push(e) });
      const fresh = testRunner(db, { workerId: "fresh" });
      const id = await fresh.store.create("ledger-import", { file });

      const staleDone = stale.runUntilIdle();
      await eventually(async () => (await fresh.store.get(id))!.cursor === 500);
      await db.pool.query("UPDATE runs SET lease_expires_at = now() - interval '1 second'");
      await fresh.runUntilIdle();
      hold.open();
      await staleDone;
      return events;
    }

    it("with it, the stale worker's chunk is rejected and the result matches a clean run", async () => {
      const events = await staleWorker();

      expect(events).toContain("lost ownership");
      expect(await count(db, "SELECT * FROM ledger_entries")).toBe(2000);
      expect(await ledgerDigest(db.pool)).toBe(await baselineDigest(file));
    });

    it("switched off, the stale worker posts rows the other worker already posted", async () => {
      await staleWorker(new BlindWriteStore(db.pool, 30_000));

      expect(await count(db, "SELECT * FROM ledger_entries")).toBeGreaterThan(2000);
      expect(await count(db, "SELECT source_line FROM ledger_entries GROUP BY source_line HAVING count(*) > 1")).toBeGreaterThan(0);
      expect(await ledgerDigest(db.pool)).not.toBe(await baselineDigest(file));
    });
  });

  describe("graceful shutdown", () => {
    // Worker A is asked to stop while it is posting rows 501 to 600.
    async function stopMidChunk(store?: RunStore): Promise<string> {
      const a: Runner = testRunner(db, {
        workerId: "a",
        store,
        workflow: duringChunk(ledger(db), (firstRow) => firstRow === 500 && a.stop()),
      });
      const id = await a.store.create("ledger-import", { file });
      await a.runUntilIdle();
      return id;
    }

    it("with it, the chunk in progress commits, and another worker resumes straight away without redoing anything", async () => {
      const id = await stopMidChunk();
      expect(await testRunner(db).store.get(id)).toMatchObject({ status: "running", cursor: 600, leaseOwner: null });
      expect(await count(db, "SELECT 1 FROM runs WHERE id = $1 AND lease_expires_at <= now()", [id])).toBe(1);

      const sources: number[] = [];
      const b = testRunner(db, { workerId: "b", workflow: recordsSources(ledger(db), sources) });
      expect(await b.runUntilIdle()).toBe(1);
      expect(sources).toEqual([600]);
      expect(await count(db, "SELECT * FROM ledger_entries")).toBe(2000);
      expect(await ledgerDigest(db.pool)).toBe(await baselineDigest(file));
    });

    it("switched off, the run is stuck with the stopped worker until its lease runs out", async () => {
      const id = await stopMidChunk(new NoHandoffStore(db.pool, 30_000));

      const b = testRunner(db, { workerId: "b" });
      expect(await b.runUntilIdle()).toBe(0);
      expect(await b.store.get(id)).toMatchObject({ status: "running", leaseOwner: "a" });
      expect(await count(db, "SELECT 1 FROM runs WHERE id = $1 AND lease_expires_at > now() + interval '25 seconds'", [id])).toBe(1);
    });
  });
});
