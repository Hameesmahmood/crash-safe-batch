import { randomUUID } from "node:crypto";
import pg from "pg";
import { inject } from "vitest";
import { migrate } from "../src/db/migrate.ts";

export interface TestDatabase {
  pool: pg.Pool;
  url: string;
  drop: () => Promise<void>;
}

/** A new, empty, migrated database. Tests never share rows. */
export async function createTestDatabase(options: { migrate?: boolean } = {}): Promise<TestDatabase> {
  const adminUrl = inject("adminUrl");
  const name = `t_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`);
  await admin.end();

  const url = new URL(adminUrl);
  url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString() });
  if (options.migrate !== false) await migrate(pool);

  return {
    pool,
    url: url.toString(),
    drop: async () => {
      await pool.end();
      const client = new pg.Client({ connectionString: adminUrl });
      await client.connect();
      await client.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      await client.end();
    },
  };
}
