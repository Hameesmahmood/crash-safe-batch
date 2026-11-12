// Each guarantee, shown twice: once as built, and once switched off to show what it prevents.
// The switched-off versions exist only here, never in src/.
import type { Pool, PoolClient } from "pg";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RunStore, type Changes, type RunRow } from "../src/engine/store.ts";
import { ledgerDigest } from "../src/example/ledger.ts";
import { createTestDatabase, type TestDatabase } from "./db.ts";
import { baselineDigest, count, eventually, gate, testRunner, writeCsv } from "./helpers.ts";
import { failsOnceAfterWrites, stallsAt } from "./workflows.ts";

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

describe("each guarantee, switched on and off", () => {
  let file: string;
  let db: TestDatabase;
  beforeAll(async () => {
    file = await writeCsv(2000);
  });
  beforeEach(async () => {
    db = await createTestDatabase();
  });
  afterEach(() => db.drop());

  describe("checkpoint in the same transaction as the chunk's writes", () => {
    it("with it, a crash after the writes loses nothing and repeats nothing", async () => {
      const runner = testRunner(db, { workflow: failsOnceAfterWrites(700) });
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

  describe("optimistic locking on the run's version", () => {
    // A worker commits five chunks, then freezes mid-chunk. Its lease runs out, a second worker takes
    // the run over and finishes it, and then the first worker wakes up and tries to carry on.
    async function staleWorker(store?: RunStore) {
      const hold = gate();
      const events: string[] = [];
      const stale = testRunner(db, { workerId: "stale", workflow: stallsAt(551, hold.wait), store, log: (e) => events.push(e) });
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
});
