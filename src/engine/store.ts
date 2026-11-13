import type { Pool, PoolClient } from "pg";
import { assertMove, CLAIMABLE, type RunStatus } from "./states.ts";

export interface RunRow {
  id: string;
  workflow: string;
  input: unknown;
  status: RunStatus;
  stepIndex: number;
  cursor: number;
  version: number;
  attempt: number;
  leaseOwner: string | null;
  leaseExpiresAt: Date | null;
  error: string | null;
}

/** Another worker took the run over (its lease had expired). This worker must stop touching it. */
export class LostOwnership extends Error {
  readonly runId: string;

  constructor(runId: string) {
    super(`Run ${runId} is now owned by another worker.`);
    this.name = "LostOwnership";
    this.runId = runId;
  }
}

type Lease = "renew" | "release" | "clear" | { retryInMs: number };

export interface Changes {
  status?: RunStatus;
  stepIndex?: number;
  cursor?: number;
  attempt?: number;
  error?: string | null;
  lease: Lease;
}

/**
 * Every write the engine makes to a run. Each one is a compare-and-set on `version`: it applies only
 * if nobody has changed the run since this worker last read it. That's the optimistic lock. Every
 * write bumps the version, so a matching version also proves the status this worker read is current.
 */
export class RunStore {
  protected readonly pool: Pool;
  protected readonly leaseMs: number;

  constructor(pool: Pool, leaseMs: number) {
    this.pool = pool;
    this.leaseMs = leaseMs;
  }

  async create(workflow: string, input: unknown): Promise<string> {
    const { rows } = await this.pool.query<{ id: string }>("INSERT INTO runs (workflow, input) VALUES ($1, $2) RETURNING id", [
      workflow,
      JSON.stringify(input),
    ]);
    return String(rows[0]!.id);
  }

  async get(id: string): Promise<RunRow | undefined> {
    const { rows } = await this.pool.query("SELECT * FROM runs WHERE id = $1", [id]);
    return rows[0] ? toRun(rows[0]) : undefined;
  }

  /**
   * Takes one run that is waiting, or whose worker stopped renewing its lease. SKIP LOCKED means
   * workers never wait on each other or take the same run. Claiming bumps the version, so a worker
   * that lost its lease can no longer commit anything to this run.
   */
  async claim(owner: string, workflows: readonly string[]): Promise<RunRow | undefined> {
    const { rows } = await this.pool.query(
      `WITH next AS (
         SELECT id FROM runs
         WHERE workflow = ANY($3)
           AND status = ANY($4)
           AND (status = 'pending' OR lease_expires_at IS NULL OR lease_expires_at <= now())
         ORDER BY id
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       UPDATE runs r
       -- pending -> running is the only move a claim makes, and the state machine allows it.
       SET status = CASE WHEN r.status = 'pending' THEN 'running' ELSE r.status END,
           lease_owner = $1,
           lease_expires_at = now() + $2 * interval '1 millisecond',
           version = r.version + 1,
           updated_at = now()
       FROM next
       WHERE r.id = next.id
       RETURNING r.*`,
      [owner, this.leaseMs, workflows, CLAIMABLE],
    );
    return rows[0] ? toRun(rows[0]) : undefined;
  }

  /**
   * The transactional checkpoint. The chunk's own writes and the cursor move commit together or
   * not at all, and only if this worker still owns the run.
   */
  async commitChunk(run: RunRow, nextCursor: number, work: (tx: PoolClient) => Promise<void>): Promise<RunRow> {
    return this.transaction(async (tx) => {
      await work(tx);
      return this.advance(tx, run, { cursor: nextCursor, lease: "renew" });
    });
  }

  /** Marks the current step done and moves to the next one, or finishes the run after the last step. */
  async completeStep(
    run: RunRow,
    options: { rows: number; last: boolean; work?: (tx: PoolClient) => Promise<void> },
  ): Promise<RunRow> {
    return this.transaction(async (tx) => {
      await options.work?.(tx);
      await tx.query("INSERT INTO run_steps (run_id, step_index, status, rows) VALUES ($1, $2, 'done', $3)", [
        run.id,
        run.stepIndex,
        options.rows,
      ]);
      return this.advance(
        tx,
        run,
        options.last
          ? { status: "succeeded", stepIndex: run.stepIndex + 1, cursor: 0, attempt: 0, error: null, lease: "clear" }
          : { stepIndex: run.stepIndex + 1, cursor: 0, attempt: 0, error: null, lease: "renew" },
      );
    });
  }

  /**
   * The current step failed for good. Records how much of it committed (a batch step that failed
   * halfway still has chunks to undo), then points the run at the last step with work to undo.
   * The worker keeps its lease and carries on with the undo.
   */
  async beginCompensation(run: RunRow, options: { error: string; committedRows: number }): Promise<RunRow> {
    return this.transaction(async (tx) => {
      if (options.committedRows > 0) {
        await tx.query("INSERT INTO run_steps (run_id, step_index, status, rows) VALUES ($1, $2, 'partial', $3)", [
          run.id,
          run.stepIndex,
          options.committedRows,
        ]);
      }
      const last = await this.lastStepToUndo(tx, run.id);
      return this.advance(tx, run, {
        status: "compensating",
        stepIndex: last ?? run.stepIndex,
        cursor: 0,
        attempt: 0,
        error: options.error,
        lease: "renew",
      });
    });
  }

  /** How many input rows of the run's current step committed, if that step has anything to undo. */
  async rowsToUndo(run: RunRow): Promise<number | undefined> {
    const { rows } = await this.pool.query<{ rows: string }>(
      "SELECT rows FROM run_steps WHERE run_id = $1 AND step_index = $2 AND status IN ('done', 'partial')",
      [run.id, run.stepIndex],
    );
    return rows[0] ? Number(rows[0].rows) : undefined;
  }

  /**
   * Marks the current step undone, in the same transaction as the undo's last writes, and moves
   * to the next step back. After the first step, the run is compensated.
   */
  async completeUndo(run: RunRow, work?: (tx: PoolClient) => Promise<void>): Promise<RunRow> {
    return this.transaction(async (tx) => {
      await work?.(tx);
      await tx.query("UPDATE run_steps SET status = 'compensated', finished_at = now() WHERE run_id = $1 AND step_index = $2", [
        run.id,
        run.stepIndex,
      ]);
      const next = await this.lastStepToUndo(tx, run.id);
      return this.advance(
        tx,
        run,
        next === undefined
          ? { status: "compensated", stepIndex: 0, cursor: 0, attempt: 0, lease: "clear" }
          : { stepIndex: next, cursor: 0, attempt: 0, lease: "renew" },
      );
    });
  }

  /** An undo failed for good. The run stops where it is, half undone, for a person to look at. */
  async giveUp(run: RunRow, error: string): Promise<RunRow> {
    return this.advance(this.pool, run, { status: "failed", error, lease: "clear" });
  }

  /** A try of the current step failed. Hand the run back, to be claimed again after a delay. */
  async recordFailure(run: RunRow, error: string, retryInMs: number): Promise<RunRow> {
    return this.advance(this.pool, run, { attempt: run.attempt + 1, error, lease: { retryInMs } });
  }

  /** Gives the run back straight away, for another worker to claim. Used on graceful shutdown. */
  async release(run: RunRow): Promise<RunRow> {
    return this.advance(this.pool, run, { lease: "release" });
  }

  protected async advance(db: Pool | PoolClient, run: RunRow, changes: Changes): Promise<RunRow> {
    const params: unknown[] = [run.id, run.version];
    const sets = ["version = version + 1", "updated_at = now()"];
    const set = (column: string, value: unknown) => {
      params.push(value);
      sets.push(`${column} = $${params.length}`);
    };

    if (changes.status !== undefined && changes.status !== run.status) {
      assertMove(run.status, changes.status);
      set("status", changes.status);
    }
    if (changes.stepIndex !== undefined) set("step_index", changes.stepIndex);
    if (changes.cursor !== undefined) set("cursor", changes.cursor);
    if (changes.attempt !== undefined) set("attempt", changes.attempt);
    if (changes.error !== undefined) set("error", changes.error);

    const lease = changes.lease;
    if (lease === "renew") {
      params.push(this.leaseMs);
      sets.push(`lease_expires_at = now() + $${params.length} * interval '1 millisecond'`);
    } else if (lease === "release") {
      sets.push("lease_owner = NULL", "lease_expires_at = now()");
    } else if (lease === "clear") {
      sets.push("lease_owner = NULL", "lease_expires_at = NULL");
    } else {
      params.push(lease.retryInMs);
      sets.push("lease_owner = NULL", `lease_expires_at = now() + $${params.length} * interval '1 millisecond'`);
    }

    const { rows } = await db.query(
      `UPDATE runs SET ${sets.join(", ")} WHERE id = $1 AND version = $2 RETURNING *`,
      params,
    );
    if (!rows[0]) throw new LostOwnership(run.id);
    return toRun(rows[0]);
  }

  private async lastStepToUndo(tx: PoolClient, runId: string): Promise<number | undefined> {
    const { rows } = await tx.query<{ step_index: number | null }>(
      "SELECT max(step_index) AS step_index FROM run_steps WHERE run_id = $1 AND status IN ('done', 'partial')",
      [runId],
    );
    return rows[0]?.step_index ?? undefined;
  }

  protected async transaction<T>(work: (tx: PoolClient) => Promise<T>): Promise<T> {
    const tx = await this.pool.connect();
    let broken = false;
    try {
      await tx.query("BEGIN");
      const result = await work(tx);
      await tx.query("COMMIT");
      return result;
    } catch (error) {
      await tx.query("ROLLBACK").catch(() => {
        broken = true;
      });
      throw error;
    } finally {
      // A connection that can't even roll back is thrown away, not returned to the pool.
      tx.release(broken);
    }
  }
}

function toRun(row: Record<string, unknown>): RunRow {
  return {
    id: String(row.id),
    workflow: row.workflow as string,
    input: row.input,
    status: row.status as RunStatus,
    stepIndex: Number(row.step_index),
    cursor: Number(row.cursor),
    version: Number(row.version),
    attempt: Number(row.attempt),
    leaseOwner: (row.lease_owner as string | null) ?? null,
    leaseExpiresAt: (row.lease_expires_at as Date | null) ?? null,
    error: (row.error as string | null) ?? null,
  };
}
