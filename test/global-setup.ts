import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import type { TestProject } from "vitest/node";

let container: StartedPostgreSqlContainer | undefined;

// One real Postgres for the whole run. Each test file then creates its own database inside it.
export async function setup(project: TestProject) {
  container = await new PostgreSqlContainer("postgres:18-alpine").start();
  project.provide("adminUrl", container.getConnectionUri());
}

export async function teardown() {
  await container?.stop();
}

declare module "vitest" {
  export interface ProvidedContext {
    adminUrl: string;
  }
}
