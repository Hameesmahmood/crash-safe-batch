import { hostname } from "node:os";
import { setTimeout as sleep } from "node:timers/promises";
import type { Pool } from "pg";
import { LostOwnership, RunStore, type RunRow } from "./store.ts";
import { PermanentError, type BatchStep, type ChunkContext, type RunInfo, type Step, type Workflow } from "./workflow.ts";

export type Log = (event: string, fields?: Record<string, unknown>) => void;

export interface RunnerOptions {
  pool: Pool;
  workflows: readonly Workflow<any>[];
  /** Shows up as `lease_owner`. Defaults to host and process ID. */
  workerId?: string;
  /** How long a claim lasts without a checkpoint. Must be longer than the slowest chunk. */
  leaseMs?: number;
  /** How long an idle worker waits before looking for work again. */
  pollMs?: number;
  /** Delay before a failed step is tried again. Defaults to exponential backoff with full jitter. */
  retryDelayMs?: (attempt: number) => number;
  /** Swappable so tests can switch individual guarantees off. */
  store?: RunStore;
  log?: Log;
}

/** Thrown inside a run when the worker is shutting down, after the run has been handed back. */
class Stopping extends Error {}

/**
 * The latest known state of the run being executed. Every commit replaces it, so a failure is
 * always recorded against the current version, not the one from claim time.
 */
interface Current {
  run: RunRow;
}

export class Runner {
  readonly store: RunStore;
  private readonly workflows: Map<string, Workflow<any>>;
  private readonly workerId: string;
  private readonly pollMs: number;
  private readonly retryDelayMs: (attempt: number) => number;
  private readonly log: Log;
  private readonly stopController = new AbortController();

  constructor(options: RunnerOptions) {
    const leaseMs = options.leaseMs ?? 30_000;
    this.store = options.store ?? new RunStore(options.pool, leaseMs);
    this.workflows = new Map(options.workflows.map((w) => [w.name, w]));
    this.workerId = options.workerId ?? `${hostname()}:${process.pid}`;
    this.pollMs = options.pollMs ?? 500;
    this.retryDelayMs = options.retryDelayMs ?? fullJitter;
    this.log = options.log ?? jsonLog;
  }

  get signal(): AbortSignal {
    return this.stopController.signal;
  }

  /** Asks the worker to stop. The chunk in progress commits, then the run is handed back. */
  stop(): void {
    this.stopController.abort();
  }

  /** Works until stopped, polling when there's nothing to do. */
  async work(): Promise<void> {
    while (!this.signal.aborted) {
      const ran = await this.runUntilIdle();
      if (ran === 0) await sleep(this.pollMs, undefined, { signal: this.signal }).catch(() => {});
    }
  }

  /** Claims and runs work until none is left (or the worker is stopped). Returns how many claims it made. */
  async runUntilIdle(): Promise<number> {
    let claims = 0;
    while (!this.signal.aborted) {
      const run = await this.store.claim(this.workerId, [...this.workflows.keys()]);
      if (!run) break;
      claims++;
      await this.execute(run);
    }
    return claims;
  }

  private async execute(claimed: RunRow): Promise<void> {
    const workflow = this.workflows.get(claimed.workflow)!;
    const current: Current = { run: claimed };
    this.log("claimed", { run: claimed.id, status: claimed.status, step: claimed.stepIndex, cursor: claimed.cursor });

    // Forwards through the steps while running, backwards through the finished ones while compensating.
    while (current.run.status === "running" || current.run.status === "compensating") {
      const step = workflow.steps[current.run.stepIndex]!;
      try {
        if (current.run.status === "running") await this.forward(current, step, workflow);
        else await this.undo(current, step);
      } catch (error) {
        if (error instanceof Stopping) return;
        if (error instanceof LostOwnership) {
          this.log("lost ownership", { run: claimed.id });
          return;
        }
        if (!(await this.fail(current, step, error))) return;
      }
    }
    this.log(current.run.status, { run: claimed.id, error: current.run.error ?? undefined });
  }

  private async forward<I>(current: Current, step: Step<I>, workflow: Workflow<I>): Promise<void> {
    const info = runInfo<I>(current.run);
    const last = current.run.stepIndex === workflow.steps.length - 1;
    if (step.kind === "batch") {
      await this.chunks(current, step, info, Infinity, step.handle);
      current.run = await this.store.completeStep(current.run, { rows: current.run.cursor, last });
    } else {
      current.run = await this.store.completeStep(current.run, {
        rows: 0,
        last,
        work: (tx) => step.run({ run: info, tx, signal: this.signal }),
      });
    }
    this.log("step done", { run: info.id, step: step.name });
    await this.stopIfAsked(current.run);
  }

  /** Undoes the current step: only the rows it committed, chunk by chunk, checkpointed like the forward pass. */
  private async undo<I>(current: Current, step: Step<I>): Promise<void> {
    const info = runInfo<I>(current.run);
    const rows = await this.store.rowsToUndo(current.run);
    if (rows === undefined || !step.compensate) {
      // Never ran, or has nothing to undo.
      current.run = await this.store.completeUndo(current.run);
    } else if (step.kind === "batch") {
      await this.chunks(current, step, info, rows, step.compensate);
      current.run = await this.store.completeUndo(current.run);
      this.log("step undone", { run: info.id, step: step.name, rows });
    } else {
      current.run = await this.store.completeUndo(current.run, (tx) => step.compensate!({ run: info, tx, signal: this.signal }));
      this.log("step undone", { run: info.id, step: step.name });
    }
    await this.stopIfAsked(current.run);
  }

  /**
   * Feeds the step's rows, from the run's cursor up to `limit`, through `handler` in chunks, each in
   * a transaction with its checkpoint. The source is pulled one row at a time, and the next row is
   * only pulled after the previous chunk has committed. That is the backpressure: a slow database
   * slows the reader down.
   */
  private async chunks<I, R>(
    current: Current,
    step: BatchStep<I, R>,
    info: RunInfo<I>,
    limit: number,
    handler: (rows: R[], ctx: ChunkContext<I>) => Promise<void>,
  ): Promise<void> {
    let chunk: R[] = [];
    const flush = async () => {
      const firstRow = current.run.cursor;
      const rows = chunk;
      chunk = [];
      current.run = await this.store.commitChunk(current.run, firstRow + rows.length, (tx) =>
        handler(rows, { run: info, tx, signal: this.signal, firstRow }),
      );
      await this.stopIfAsked(current.run);
    };

    if (current.run.cursor >= limit) return;
    for await (const row of step.source(info.input, current.run.cursor)) {
      chunk.push(row);
      const end = current.run.cursor + chunk.length;
      if (chunk.length === step.size || end === limit) await flush();
      if (end === limit) return; // Leaving the loop closes the source.
    }
    if (chunk.length > 0) await flush();
  }

  /**
   * Decides what a failure means. Returns true if this worker should carry on with the run, which
   * happens when it has just moved the run to compensating.
   */
  private async fail<I>(current: Current, step: Step<I>, error: unknown): Promise<boolean> {
    const run = current.run;
    const message = error instanceof Error ? error.message : String(error);
    const tries = run.attempt + 1;
    const fields = { run: run.id, step: step.name, status: run.status, attempt: tries, error: message };
    try {
      if (tries < step.attempts && !(error instanceof PermanentError)) {
        const delay = this.retryDelayMs(tries);
        current.run = await this.store.recordFailure(run, message, delay);
        this.log("step failed, will retry", { ...fields, retryInMs: delay });
        return false;
      }
      if (run.status === "running") {
        // The rows this step committed before failing are undone along with the finished steps.
        const committedRows = step.kind === "batch" ? run.cursor : 0;
        current.run = await this.store.beginCompensation(run, { error: `${step.name}: ${message}`, committedRows });
        this.log("step failed for good, compensating", fields);
        return true;
      }
      current.run = await this.store.giveUp(run, `undoing ${step.name}: ${message}`);
      this.log("undo failed for good", fields);
      return false;
    } catch (recordError) {
      if (recordError instanceof LostOwnership) {
        this.log("lost ownership", { run: run.id });
        return false;
      }
      throw recordError;
    }
  }

  private async stopIfAsked(run: RunRow): Promise<void> {
    if (!this.signal.aborted || (run.status !== "running" && run.status !== "compensating")) return;
    await this.store.release(run);
    this.log("released on shutdown", { run: run.id, status: run.status, step: run.stepIndex, cursor: run.cursor });
    throw new Stopping();
  }
}
function runInfo<I>(run: RunRow): RunInfo<I> {
  return { id: run.id, workflow: run.workflow, input: run.input as I };
}

/** Exponential backoff with full jitter: a random wait between 0 and min(60 s, 1 s × 2^attempt). */
export function fullJitter(attempt: number): number {
  return Math.floor(Math.random() * Math.min(60_000, 1_000 * 2 ** attempt));
}

const jsonLog: Log = (event, fields) => console.log(JSON.stringify({ at: new Date().toISOString(), event, ...fields }));
