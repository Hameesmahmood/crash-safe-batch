// Throughput against chunk size. Runs the import's `post` step at each chunk size in a fresh database
// and reports rows per second (the median of several runs). Small chunks mean many small
// transactions, each with its own checkpoint; large ones mean fewer, bigger transactions but more
// work lost when a worker dies mid-chunk.
//
//   node tools/bench.ts [--rows 20000] [--sizes 1,10,50,100,250,500,1000,2500,5000] [--repeats 3]
//                       [--out docs/results/bench-latest.json]
import { parseArgs } from "node:util";
import { Runner } from "../src/engine/runner.ts";
import { ledgerImport } from "../src/example/ledger.ts";
import { startStatementService } from "../src/example/statement-service.ts";
import { ensureCsv, gitSha, scratchDatabase, writeJson } from "./lib.ts";

const { values } = parseArgs({
  options: {
    rows: { type: "string", default: "20000" },
    seed: { type: "string", default: "42" },
    sizes: { type: "string", default: "1,10,50,100,250,500,1000,2500,5000" },
    repeats: { type: "string", default: "3" },
    out: { type: "string", default: "docs/results/bench-latest.json" },
  },
});
const config = {
  rows: Number(values.rows),
  seed: Number(values.seed),
  sizes: values.sizes.split(",").map(Number),
  repeats: Number(values.repeats),
};

async function postSeconds(file: string, chunk: number): Promise<number> {
  const db = await scratchDatabase("bench");
  const service = await startStatementService();
  try {
    let started = 0;
    let finished = 0;
    const runner = new Runner({
      pool: db.pool,
      workflows: [ledgerImport({ chunkSize: chunk, statementServiceUrl: service.url })],
      log: (event, fields) => {
        if (event === "claimed") started = performance.now();
        if (event === "step done" && fields?.step === "post") finished = performance.now();
      },
    });
    await runner.store.create("ledger-import", { file });
    await runner.runUntilIdle();
    if (!finished) throw new Error(`The run with chunk ${chunk} didn't finish posting.`);
    return (finished - started) / 1000;
  } finally {
    await service.close();
    await db.drop();
  }
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid]! : (s[mid - 1]! + s[mid]!) / 2;
};

const file = await ensureCsv(config.rows, config.seed);
const startedAt = new Date().toISOString();
console.log(`benchmark: ${config.rows.toLocaleString("en")} rows, ${config.repeats} runs per chunk size`);
await postSeconds(file, 500); // Warm up the database and the JIT; not recorded.

const results = [];
for (const chunk of config.sizes) {
  const seconds: number[] = [];
  for (let i = 0; i < config.repeats; i++) seconds.push(await postSeconds(file, chunk));
  const rowsPerSecond = seconds.map((s) => Math.round(config.rows / s));
  const result = {
    chunk,
    transactions: Math.ceil(config.rows / chunk),
    rowsPerSecond,
    median: Math.round(median(rowsPerSecond)),
    min: Math.min(...rowsPerSecond),
    max: Math.max(...rowsPerSecond),
  };
  results.push(result);
  console.log(`chunk ${String(chunk).padStart(5)}: ${String(result.median).padStart(7)} rows/s (runs: ${rowsPerSecond.join(", ")})`);
}

await writeJson(values.out, { startedAt, commit: gitSha(), node: process.version, config, results });
console.log(`Wrote ${values.out}.`);
