import { hostname } from "node:os";
import { setTimeout as sleep } from "node:timers/promises";
import type { Pool } from "pg";
import { LostOwnership, RunStore, type RunRow } from "./store.ts";
import type { BatchStep, RunInfo, SimpleStep, Step, Workflow } from "./workflow.ts";

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
    try {
      while (current.run.status === "running") {
        const step = workflow.steps[current.run.stepIndex]!;
        if (step.kind === "batch") await this.runBatch(current, step, workflow);
        else await this.runSimple(current, step, workflow);
      }
      if (current.run.status === "succeeded") this.log("succeeded", { run: claimed.id });
    } catch (error) {
      if (error instanceof Stopping) return;
      if (error instanceof LostOwnership) {
        this.log("lost ownership", { run: claimed.id });
        return;
      }
      await this.fail(current.run, workflow.steps[current.run.stepIndex]!, error);
    }
  }

  private async runBatch<I>(current: Current, step: BatchStep<I, unknown>, workflow: Workflow<I>): Promise<void> {
    const info = runInfo<I>(current.run);
    let chunk: unknown[] = [];

    // The source is pulled one row at a time, and the next row is only pulled after the previous
    // chunk has committed. That is the backpressure: a slow database slows the reader down.
    for await (const row of step.source(info.input, current.run.cursor)) {
      chunk.push(row);
      if (chunk.length === step.size) {
        await this.commit(current, step, info, chunk);
        chunk = [];
        await this.stopIfAsked(current.run);
      }
    }
    if (chunk.length > 0) await this.commit(current, step, info, chunk);

    const rows = current.run.cursor;
    current.run = await this.store.completeStep(current.run, { rows, last: this.isLast(current.run, workflow) });
    this.log("step done", { run: info.id, step: step.name, rows });
    await this.stopIfAsked(current.run);
  }

  private async commit<I>(current: Current, step: BatchStep<I, unknown>, info: RunInfo<I>, rows: unknown[]): Promise<void> {
    const firstRow = current.run.cursor;
    current.run = await this.store.commitChunk(current.run, firstRow + rows.length, (tx) =>
      step.handle(rows, { run: info, tx, signal: this.signal, firstRow }),
    );
  }

  private async runSimple<I>(current: Current, step: SimpleStep<I>, workflow: Workflow<I>): Promise<void> {
    const info = runInfo<I>(current.run);
    current.run = await this.store.completeStep(current.run, {
      rows: 0,
      last: this.isLast(current.run, workflow),
      work: (tx) => step.run({ run: info, tx, signal: this.signal }),
    });
    this.log("step done", { run: info.id, step: step.name });
    await this.stopIfAsked(current.run);
  }

  private async fail<I>(run: RunRow, step: Step<I>, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    const tries = run.attempt + 1;
    try {
      if (tries < step.attempts) {
        const delay = this.retryDelayMs(tries);
        await this.store.recordFailure(run, message, delay);
        this.log("step failed, will retry", { run: run.id, step: step.name, attempt: tries, retryInMs: delay, error: message });
      } else {
        // Out of tries. Compensation (undoing finished steps) arrives with the saga support; until
        // then the run waits here, failed but not yet compensated.
        await this.store.recordFailure(run, message, 0x7fffffff);
        this.log("step failed for good", { run: run.id, step: step.name, attempt: tries, error: message });
      }
    } catch (recordError) {
      if (recordError instanceof LostOwnership) this.log("lost ownership", { run: run.id });
      else throw recordError;
    }
  }

  private async stopIfAsked(run: RunRow): Promise<void> {
    if (!this.signal.aborted || run.status !== "running") return;
    await this.store.release(run);
    this.log("released on shutdown", { run: run.id, step: run.stepIndex, cursor: run.cursor });
    throw new Stopping();
  }

  private isLast(run: RunRow, workflow: Workflow<unknown>): boolean {
    return run.stepIndex === workflow.steps.length - 1;
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
