// The crash test. Runs the ledger import once without interruption and fingerprints the result, then
// runs it again in fresh databases while killing worker processes with SIGKILL at random moments and
// starting new ones, until each run finishes. Every interrupted run must match the clean one.
//
//   node tools/crash.ts [--rows 50000] [--trials 10] [--workers 2] [--chunk 500] [--lease 1000]
//                       [--min-life 100] [--max-life 900] [--out docs/results/crash-latest.json]
//
// Needs Postgres (docker compose up -d). Exits with 1 if any trial differs from the clean run.
import { spawn, type ChildProcess } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { parseArgs } from "node:util";
import { RunStore } from "../src/engine/store.ts";
import { ledgerDigest } from "../src/example/ledger.ts";
import { startStatementService } from "../src/example/statement-service.ts";
import { ensureCsv, gitSha, scratchDatabase, writeJson } from "./lib.ts";

const { values } = parseArgs({
  options: {
    rows: { type: "string", default: "50000" },
    seed: { type: "string", default: "42" },
    trials: { type: "string", default: "10" },
    workers: { type: "string", default: "2" },
    chunk: { type: "string", default: "500" },
    lease: { type: "string", default: "1000" },
    "min-life": { type: "string", default: "100" },
    "max-life": { type: "string", default: "900" },
    out: { type: "string", default: "docs/results/crash-latest.json" },
  },
});
const config = {
  rows: Number(values.rows),
  seed: Number(values.seed),
  trials: Number(values.trials),
  workers: Number(values.workers),
  chunk: Number(values.chunk),
  leaseMs: Number(values.lease),
  minLifeMs: Number(values["min-life"]),
  maxLifeMs: Number(values["max-life"]),
};

interface Outcome {
  status: string;
  digest: string;
  seconds: number;
  /** Workers killed with SIGKILL. */
  kills: number;
  /** Of those, how many had claimed the run, so died holding it. */
  killsHoldingRun: number;
  entries: number;
  distinctLines: number;
  notifyRequests: number;
  notifyAccepted: number;
}

/** Runs one import to the end. With `kill`, every worker is SIGKILLed after a random lifetime and replaced. */
async function importOnce(name: string, file: string, kill: boolean): Promise<Outcome> {
  const db = await scratchDatabase(name);
  const service = await startStatementService();
  const store = new RunStore(db.pool, config.leaseMs);
  const id = await store.create("ledger-import", { file });
  const env = { ...process.env, DATABASE_URL: db.url, STATEMENT_SERVICE_URL: service.url };
  const args = ["src/cli.ts", "worker", "--chunk", String(config.chunk), "--lease", String(config.leaseMs)];

  const live = new Set<ChildProcess>();
  let finished = false;
  let kills = 0;
  let killsHoldingRun = 0;
  const startWorker = () => {
    const child = spawn(process.execPath, args, { env, stdio: ["ignore", "pipe", "inherit"] });
    let holding = false;
    child.stdout!.on("data", (data: Buffer) => {
      if (data.includes('"event":"claimed"')) holding = true;
    });
    live.add(child);
    child.once("exit", () => live.delete(child));
    if (kill) {
      const life = config.minLifeMs + Math.random() * (config.maxLifeMs - config.minLifeMs);
      setTimeout(() => {
        if (!live.has(child) || finished) return;
        child.kill("SIGKILL");
        kills++;
        if (holding) killsHoldingRun++;
        startWorker();
      }, life);
    }
  };

  const started = performance.now();
  for (let i = 0; i < (kill ? config.workers : 1); i++) startWorker();
  let run = await store.get(id);
  while (run!.status === "pending" || run!.status === "running" || run!.status === "compensating") {
    await sleep(50);
    run = await store.get(id);
  }
  finished = true;
  const seconds = (performance.now() - started) / 1000;
  await Promise.all([...live].map((child) => new Promise((resolve) => (child.once("exit", resolve), child.kill("SIGTERM")))));

  const count = async (sql: string) => Number((await db.pool.query(sql)).rows[0].n);
  const outcome: Outcome = {
    status: run!.status,
    digest: await ledgerDigest(db.pool),
    seconds: Math.round(seconds * 10) / 10,
    kills,
    killsHoldingRun,
    entries: await count("SELECT count(*) AS n FROM ledger_entries"),
    distinctLines: await count("SELECT count(DISTINCT source_line) AS n FROM ledger_entries"),
    notifyRequests: service.requests,
    notifyAccepted: service.accepted.size,
  };
  await service.close();
  await db.drop();
  return outcome;
}

const file = await ensureCsv(config.rows, config.seed);
const startedAt = new Date().toISOString();
console.log(`crash test: ${config.rows.toLocaleString("en")} rows, ${config.trials} trials, ${config.workers} workers, chunk ${config.chunk}`);

const clean = await importOnce("crash_clean", file, false);
if (clean.status !== "succeeded") throw new Error(`The clean run ended ${clean.status}.`);
console.log(`clean run: ${clean.seconds} s, digest ${clean.digest.slice(0, 12)}`);

const trials: (Outcome & { trial: number; match: boolean })[] = [];
for (let trial = 1; trial <= config.trials; trial++) {
  const outcome = await importOnce(`crash_trial_${trial}`, file, true);
  const match = outcome.status === "succeeded" && outcome.digest === clean.digest;
  trials.push({ trial, match, ...outcome });
  console.log(
    `trial ${trial}: ${match ? "match" : "MISMATCH"}, ${outcome.kills} kills (${outcome.killsHoldingRun} holding the run), ` +
      `${outcome.seconds} s, ${outcome.entries} entries, notify sent ${outcome.notifyRequests}x, accepted ${outcome.notifyAccepted}x`,
  );
}

const summary = {
  trials: trials.length,
  matches: trials.filter((t) => t.match).length,
  mismatches: trials.filter((t) => !t.match).length,
  kills: trials.reduce((n, t) => n + t.kills, 0),
  killsHoldingRun: trials.reduce((n, t) => n + t.killsHoldingRun, 0),
  duplicateEntries: trials.reduce((n, t) => n + (t.entries - t.distinctLines), 0),
  notifyRequests: trials.reduce((n, t) => n + t.notifyRequests, 0),
  notifyAccepted: trials.reduce((n, t) => n + t.notifyAccepted, 0),
};
await writeJson(values.out, { startedAt, commit: gitSha(), node: process.version, config, clean, summary, trials });
console.log(
  `\n${summary.matches}/${summary.trials} trials match the clean run. ${summary.kills} SIGKILLs, ${summary.killsHoldingRun} ` +
    `of them while holding the run. Duplicate entries: ${summary.duplicateEntries}. Wrote ${values.out}.`,
);
process.exitCode = summary.mismatches === 0 ? 0 : 1;
