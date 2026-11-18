// Shared by the crash harness and the benchmark.
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import { createWriteStream, existsSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import pg from "pg";
import { migrate } from "../src/db/migrate.ts";
import { generateTransactions } from "../src/example/transactions.ts";

export const ADMIN_URL = process.env.DATABASE_URL ?? "postgres://batch:batch@localhost:5433/batch";

/** The seeded CSV for this size, written once and reused. */
export async function ensureCsv(rows: number, seed: number): Promise<string> {
  const file = resolve(`data/tx-${rows}-${seed}.csv`);
  if (existsSync(file)) return file;
  await mkdir(dirname(file), { recursive: true });
  const out = createWriteStream(file);
  for (const line of generateTransactions({ rows, seed })) {
    if (!out.write(line + "\n")) await once(out, "drain");
  }
  out.end();
  await once(out, "finish");
  return file;
}

export interface ScratchDatabase {
  url: string;
  pool: pg.Pool;
  drop(): Promise<void>;
}

/** A new, migrated database next to the one in ADMIN_URL, so every trial starts empty. */
export async function scratchDatabase(name: string): Promise<ScratchDatabase> {
  const admin = new pg.Client({ connectionString: ADMIN_URL });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
  await admin.query(`CREATE DATABASE ${name}`);
  await admin.end();

  const url = new URL(ADMIN_URL);
  url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString() });
  await migrate(pool);
  return {
    url: url.toString(),
    pool,
    async drop() {
      await pool.end();
      const client = new pg.Client({ connectionString: ADMIN_URL });
      await client.connect();
      await client.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      await client.end();
    },
  };
}

export function gitSha(): string {
  try {
    const sha = execFileSync("git", ["rev-parse", "--short", "HEAD"], { encoding: "utf8" }).trim();
    const dirty = execFileSync("git", ["status", "--porcelain", "--", "src", "tools"], { encoding: "utf8" }).trim();
    return dirty ? `${sha}-dirty` : sha;
  } catch {
    return "unknown";
  }
}

export async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(value, null, 2) + "\n");
}
