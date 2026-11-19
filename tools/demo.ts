// The demo: kill a worker with SIGKILL partway through a 50,000-row import, let a second worker
// resume it, and compare the result with an import that was never interrupted.
//
//   npm run demo            (needs Postgres: docker compose up -d --wait)
//
// It uses two scratch databases next to the one in DATABASE_URL and drops them at the end.
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { Runner } from "../src/engine/runner.ts";
import { RunStore, type RunRow } from "../src/engine/store.ts";
import { ledgerDigest, ledgerImport } from "../src/example/ledger.ts";
import { startStatementService } from "../src/example/statement-service.ts";
import { ensureCsv, scratchDatabase } from "./lib.ts";

const ROWS = 50_000;
const LEASE_MS = 2_000;
const num = (n: number) => n.toLocaleString("en");
const step = (text: string) => console.log(`\n${text}`);
const detail = (text: string) => console.log(`   ${text}`);

const file = await ensureCsv(ROWS, 42);
step(`1. ${num(ROWS)} transactions to import, from ${file.replace(process.cwd() + "/", "")}`);

// The reference: the same import, uninterrupted, in its own database.
const cleanDb = await scratchDatabase("demo_clean");
const cleanService = await startStatementService();
const clean = new Runner({ pool: cleanDb.pool, workflows: [ledgerImport({ statementServiceUrl: cleanService.url })], log: () => {} });
await clean.store.create("ledger-import", { file });
await clean.runUntilIdle();
const cleanDigest = await ledgerDigest(cleanDb.pool);
await cleanService.close();
await cleanDb.drop();
detail(`An uninterrupted import, run first for comparison, has digest ${cleanDigest.slice(0, 16)}`);

const db = await scratchDatabase("demo");
const service = await startStatementService();
const store = new RunStore(db.pool, LEASE_MS);
const id = await store.create("ledger-import", { file });
const env = { ...process.env, DATABASE_URL: db.url, STATEMENT_SERVICE_URL: service.url };
const worker = (name: string) => {
  const child = spawn(process.execPath, ["src/cli.ts", "worker", "--lease", String(LEASE_MS)], { env, stdio: ["ignore", "pipe", "inherit"] });
  child.stdout!.setEncoding("utf8");
  return { name, child };
};
const run = async () => (await store.get(id)) as RunRow;

const a = worker("A");
step(`2. Run ${id} queued. Worker A (pid ${a.child.pid}) starts posting it in chunks of 500.`);

while ((await run()).cursor < ROWS * 0.4) await sleep(20);
a.child.kill("SIGKILL");
await new Promise((resolve) => a.child.once("exit", resolve));
const killed = await run();
step(`3. Worker A killed with SIGKILL. It had no chance to finish its chunk or clean up.`);
detail(`Last checkpoint: row ${num(killed.cursor)}. Anything A wrote after that rolled back with its open transaction.`);
detail(`The run still names A as its owner, with a lease that runs out in ${LEASE_MS / 1000} s.`);

const startB = performance.now();
const b = worker("B");
let claimedAt: number | undefined;
b.child.stdout!.on("data", (data: string) => {
  const line = data.split("\n").find((l) => l.includes('"event":"claimed"'));
  if (line && claimedAt === undefined) claimedAt = JSON.parse(line).cursor;
});
step(`4. Worker B (pid ${b.child.pid}) starts and waits for A's lease to run out.`);
while (claimedAt === undefined) await sleep(20);
detail(`B claimed the run ${((performance.now() - startB) / 1000).toFixed(1)} s later and resumed at row ${num(claimedAt)}.`);

let finished = await run();
while (finished.status === "running") {
  await sleep(100);
  finished = await run();
}
b.child.kill("SIGTERM");
await new Promise((resolve) => b.child.once("exit", resolve));

const count = async (sql: string) => Number((await db.pool.query(sql)).rows[0].n);
const entries = await count("SELECT count(*) AS n FROM ledger_entries");
const lines = await count("SELECT count(DISTINCT source_line) AS n FROM ledger_entries");
const digest = await ledgerDigest(db.pool);
step(`5. Run ${id} ${finished.status}.`);
detail(`${num(entries)} ledger entries for ${num(lines)} distinct CSV lines: nothing skipped, nothing posted twice.`);
const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;
detail(`The statement service received ${plural(service.requests, "request")} and accepted ${plural(service.accepted.size, "batch")}.`.replace("batchs", "batches"));
detail(`Digest of this run:              ${digest.slice(0, 16)}`);
detail(`Digest of the uninterrupted run: ${cleanDigest.slice(0, 16)}`);
console.log(digest === cleanDigest ? "\nIdentical: the crash left no trace in the result." : "\nDIFFERENT: the crash changed the result.");

await service.close();
await db.drop();
process.exitCode = digest === cleanDigest && finished.status === "succeeded" ? 0 : 1;
