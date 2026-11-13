// Variants of the ledger workflow that misbehave on purpose, for putting the engine under stress.
import { defineWorkflow, PermanentError, type BatchStep, type SimpleStep, type Step, type Workflow } from "../src/engine/workflow.ts";
import type { LedgerInput, Transaction } from "../src/example/ledger.ts";

type Post = BatchStep<LedgerInput, Transaction>;
type Simple = SimpleStep<LedgerInput>;
type Ledger = Workflow<LedgerInput>;

function change<S extends Step<LedgerInput>>(workflow: Ledger, name: string, edit: (step: S) => S): Ledger {
  return defineWorkflow(
    workflow.name,
    workflow.steps.map((s) => (s.name === name ? edit(s as S) : s)),
  );
}

const changePost = (workflow: Ledger, edit: (post: Post) => Post) => change<Post>(workflow, "post", edit);

/** Records where each read of the CSV started. */
export function recordsSources(workflow: Ledger, sources: number[]): Ledger {
  return changePost(workflow, (post) => ({
    ...post,
    source: (input, from) => {
      sources.push(from);
      return post.source(input, from);
    },
  }));
}

/** Does the chunk starting at `firstRow` in full, then throws once, before the checkpoint. */
export function failsOnceAfterWrites(workflow: Ledger, firstRow: number, sources: number[] = []): Ledger {
  let failed = false;
  return changePost(recordsSources(workflow, sources), (post) => ({
    ...post,
    handle: async (rows, ctx) => {
      await post.handle(rows, ctx);
      if (ctx.firstRow === firstRow && !failed) {
        failed = true;
        throw new Error("simulated crash after the chunk's writes");
      }
    },
  }));
}

/** The chunk starting at `firstRow` fails in a way retrying can't fix. */
export function postFailsForGoodAt(workflow: Ledger, firstRow: number): Ledger {
  return changePost(workflow, (post) => ({
    ...post,
    handle: async (rows, ctx) => {
      if (ctx.firstRow === firstRow) throw new PermanentError("row rejected");
      await post.handle(rows, ctx);
    },
  }));
}

/** Calls `during` while each chunk is being posted, before it commits. */
export function duringChunk(workflow: Ledger, during: (firstRow: number, rows: number) => void): Ledger {
  return changePost(workflow, (post) => ({
    ...post,
    handle: async (rows, ctx) => {
      during(ctx.firstRow, rows.length);
      await post.handle(rows, ctx);
    },
  }));
}

/** Calls `during` while each chunk of `post` is being undone, before the undo commits. */
export function duringUndo(workflow: Ledger, during: (firstRow: number, rows: number) => void): Ledger {
  return changePost(workflow, (post) => ({
    ...post,
    compensate: async (rows, ctx) => {
      during(ctx.firstRow, rows.length);
      await post.compensate!(rows, ctx);
    },
  }));
}

/** Freezes before reading row `line` until `hold` resolves, like a worker paused by the OS or a long GC. */
export function stallsAt(workflow: Ledger, line: number, hold: Promise<void>): Ledger {
  return changePost(workflow, (post) => ({
    ...post,
    source: async function* (input, from) {
      for await (const row of post.source(input, from)) {
        if (row.line === line) await hold;
        yield row;
      }
    },
  }));
}

/** Undoes the chunk starting at `firstRow` in full, then throws once, before the undo's checkpoint. */
export function undoFailsOnceAfterWrites(workflow: Ledger, firstRow: number, sources: number[] = []): Ledger {
  let failed = false;
  return changePost(workflow, (post) => ({
    ...post,
    source: (input, from) => {
      sources.push(from);
      return post.source(input, from);
    },
    compensate: async (rows, ctx) => {
      await post.compensate!(rows, ctx);
      if (ctx.firstRow === firstRow && !failed) {
        failed = true;
        throw new Error("simulated crash after the undo's writes");
      }
    },
  }));
}

/** Undoing `summarise` never works. */
export function undoAlwaysFails(workflow: Ledger): Ledger {
  return change<Simple>(workflow, "summarise", (summarise) => ({
    ...summarise,
    compensate: async () => {
      throw new Error("statements table is read-only");
    },
  }));
}

/** Sends the notification, then crashes once before the step's checkpoint commits. */
export function notifyCrashesOnceAfterSending(workflow: Ledger): Ledger {
  let crashed = false;
  return change<Simple>(workflow, "notify", (notify) => ({
    ...notify,
    run: async (ctx) => {
      await notify.run(ctx);
      if (!crashed) {
        crashed = true;
        throw new Error("simulated crash after the service accepted the batch");
      }
    },
  }));
}

/** The workflow with no undo steps: what you have without a saga. */
export function withoutCompensation(workflow: Ledger): Ledger {
  return defineWorkflow(
    workflow.name,
    workflow.steps.map(({ compensate: _, ...s }) => s as Step<LedgerInput>),
  );
}

/**
 * Reads the whole source as fast as it can into an unbounded queue, and hands rows out from there.
 * The classic way to lose backpressure: the reader no longer waits for the worker.
 */
export async function* readAhead<T>(source: AsyncIterable<T>): AsyncGenerator<T> {
  const queue: T[] = [];
  let next = 0;
  let finished = false;
  let failure: unknown;
  let wake = () => {};
  const pump = (async () => {
    try {
      for await (const row of source) {
        queue.push(row);
        wake();
      }
    } catch (error) {
      failure = error;
    } finally {
      finished = true;
      wake();
    }
  })();

  while (true) {
    if (next < queue.length) {
      yield queue[next++]!;
      continue;
    }
    if (failure) throw failure;
    if (finished) break;
    await new Promise<void>((resolve) => (wake = resolve));
  }
  await pump;
}
