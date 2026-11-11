// Writes a synthetic transactions CSV.
// Usage: node tools/generate-csv.ts [--rows 50000] [--seed 42] [--out data/transactions.csv]
import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { once } from "node:events";
import { parseArgs } from "node:util";
import { generateTransactions } from "../src/example/transactions.ts";

const { values } = parseArgs({
  options: {
    rows: { type: "string", default: "50000" },
    seed: { type: "string", default: "42" },
    out: { type: "string", default: "data/transactions.csv" },
  },
});

const rows = Number(values.rows);
await mkdir(dirname(values.out), { recursive: true });
const file = createWriteStream(values.out);
for (const line of generateTransactions({ rows, seed: Number(values.seed) })) {
  // Respect the stream's buffer: wait for it to drain instead of queueing the whole file in memory.
  if (!file.write(line + "\n")) await once(file, "drain");
}
file.end();
await once(file, "finish");
console.log(`Wrote ${rows.toLocaleString("en")} rows to ${values.out}`);
