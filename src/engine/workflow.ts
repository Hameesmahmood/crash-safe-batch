import type { PoolClient } from "pg";

export interface RunInfo<I> {
  id: string;
  workflow: string;
  input: I;
}

export interface StepContext<I> {
  run: RunInfo<I>;
  /** The transaction the step's writes belong to. It commits together with the checkpoint, or not at all. */
  tx: PoolClient;
  /** Aborted when the worker is shutting down. Long steps can check it; the engine always does between chunks. */
  signal: AbortSignal;
}

export interface ChunkContext<I> extends StepContext<I> {
  /** The zero-based index, within the step's input, of the first row in this chunk. */
  firstRow: number;
}

/** A step that runs once, in one transaction with its checkpoint. */
export interface SimpleStep<I> {
  kind: "simple";
  name: string;
  /** Tries before the step counts as failed for good. */
  attempts: number;
  run(ctx: StepContext<I>): Promise<void>;
  compensate?(ctx: StepContext<I>): Promise<void>;
}

/** A step that works through a large input in chunks, checkpointing after each one. */
export interface BatchStep<I, R> {
  kind: "batch";
  name: string;
  attempts: number;
  /** Rows per chunk, and so per transaction. */
  size: number;
  /** Yields the step's input rows, skipping the first `from`. Pulled lazily, so a slow consumer slows the source. */
  source(input: I, from: number): AsyncIterable<R>;
  handle(rows: R[], ctx: ChunkContext<I>): Promise<void>;
  /** Undoes a chunk. Runs over the same rows and chunk size as `handle`, and is checkpointed the same way. */
  compensate?(rows: R[], ctx: ChunkContext<I>): Promise<void>;
}

// Method syntax above keeps parameters bivariant, so a BatchStep<I, Row> fits here.
export type Step<I> = SimpleStep<I> | BatchStep<I, unknown>;

export interface Workflow<I> {
  name: string;
  steps: readonly Step<I>[];
}

export function defineWorkflow<I>(name: string, steps: readonly Step<I>[]): Workflow<I> {
  if (steps.length === 0) throw new Error(`Workflow ${name} has no steps.`);
  const names = new Set<string>();
  for (const s of steps) {
    if (names.has(s.name)) throw new Error(`Workflow ${name} has two steps called ${s.name}.`);
    names.add(s.name);
  }
  return { name, steps };
}

type SimpleOptions<I> = Omit<SimpleStep<I>, "kind" | "name" | "attempts"> & { attempts?: number };
type BatchOptions<I, R> = Omit<BatchStep<I, R>, "kind" | "name" | "attempts"> & { attempts?: number };

export function step<I>(name: string, options: SimpleOptions<I>): SimpleStep<I> {
  return { kind: "simple", name, attempts: checkAttempts(name, options.attempts ?? 3), ...withoutAttempts(options) };
}

export function batchStep<I, R>(name: string, options: BatchOptions<I, R>): BatchStep<I, R> {
  if (!Number.isInteger(options.size) || options.size < 1) throw new Error(`Step ${name} needs a chunk size of at least 1.`);
  return { kind: "batch", name, attempts: checkAttempts(name, options.attempts ?? 3), ...withoutAttempts(options) };
}

function checkAttempts(name: string, attempts: number): number {
  if (!Number.isInteger(attempts) || attempts < 1) throw new Error(`Step ${name} needs at least 1 attempt.`);
  return attempts;
}

function withoutAttempts<T extends { attempts?: number }>(options: T): Omit<T, "attempts"> {
  const { attempts: _, ...rest } = options;
  return rest;
}
