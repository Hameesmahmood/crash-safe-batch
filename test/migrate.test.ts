import { afterEach, describe, expect, it } from "vitest";
import { migrate } from "../src/db/migrate.ts";
import { createTestDatabase, type TestDatabase } from "./db.ts";

describe("migrate", () => {
  let db: TestDatabase;
  afterEach(() => db?.drop());

  it("applies every migration once, in order", async () => {
    db = await createTestDatabase({ migrate: false });
    expect(await migrate(db.pool)).toEqual(["001_engine.sql", "002_ledger_example.sql"]);
    expect(await migrate(db.pool)).toEqual([]);
  });

  it("is safe when several processes start at once", async () => {
    db = await createTestDatabase({ migrate: false });
    const results = await Promise.all([migrate(db.pool), migrate(db.pool), migrate(db.pool)]);
    expect(results.flat().sort()).toEqual(["001_engine.sql", "002_ledger_example.sql"]);
  });

  it("rejects a status the state machine doesn't know", async () => {
    db = await createTestDatabase();
    await expect(db.pool.query("INSERT INTO runs (workflow, status) VALUES ('x', 'paused')")).rejects.toThrow(/check constraint/);
  });
});
