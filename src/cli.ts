// Usage:
//   node src/cli.ts start <csv file>     queue a ledger import and print its run ID
//   node src/cli.ts worker [--chunk 500] work until stopped (Ctrl-C or SIGTERM stops it cleanly)
//   node src/cli.ts status <run id>      show a run
//   node src/cli.ts digest               fingerprint of the ledger, for comparing two databases
//
// Connects to DATABASE_URL, or the database from compose.yaml.
import { resolve } from "node:path";
import { parseArgs } from "node:util";
import pg from "pg";
import { migrate } from "./db/migrate.ts";
import { Runner } from "./engine/runner.ts";
import { ledgerDigest, ledgerImport } from "./example/ledger.ts";

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    chunk: { type: "string", default: "500" },
    lease: { type: "string", default: "30000" },
  },
});
const [command, arg] = positionals;
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL ?? "postgres://batch:batch@localhost:5433/batch" });

try {
  await migrate(pool);
  // The cursor counts rows, not chunks, so workers can use any chunk size, even mid-run.
  const runner = new Runner({ pool, workflows: [ledgerImport({ chunkSize: Number(values.chunk) })], leaseMs: Number(values.lease) });

  switch (command) {
    case "start": {
      if (!arg) throw new Error("Usage: start <csv file>");
      console.log(await runner.store.create("ledger-import", { file: resolve(arg) }));
      break;
    }
    case "worker": {
      for (const signal of ["SIGTERM", "SIGINT"] as const) process.once(signal, () => runner.stop());
      await runner.work();
      break;
    }
    case "status": {
      if (!arg) throw new Error("Usage: status <run id>");
      console.log(JSON.stringify((await runner.store.get(arg)) ?? null, null, 2));
      break;
    }
    case "digest": {
      console.log(await ledgerDigest(pool));
      break;
    }
    default:
      throw new Error("Commands: start <csv>, worker, status <run id>, digest");
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
} finally {
  await pool.end();
}
