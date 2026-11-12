// Variants of the ledger workflow that misbehave on purpose, for putting the engine under stress.
import { defineWorkflow, type BatchStep, type Workflow } from "../src/engine/workflow.ts";
import { ledgerImport, type LedgerInput, type Transaction } from "../src/example/ledger.ts";

type Post = BatchStep<LedgerInput, Transaction>;

function withPost(change: (post: Post) => Post, chunkSize = 100): Workflow<LedgerInput> {
  const post = ledgerImport({ chunkSize }).steps[0] as Post;
  return defineWorkflow("ledger-import", [change(post)]);
}

/** Does the chunk starting at `firstRow` in full, then throws once, before the checkpoint. */
export function failsOnceAfterWrites(firstRow: number, sources: number[] = []): Workflow<LedgerInput> {
  let failed = false;
  return withPost((post) => ({
    ...post,
    source: (input, from) => {
      sources.push(from);
      return post.source(input, from);
    },
    handle: async (rows, ctx) => {
      await post.handle(rows, ctx);
      if (ctx.firstRow === firstRow && !failed) {
        failed = true;
        throw new Error("simulated crash after the chunk's writes");
      }
    },
  }));
}

/** Freezes before reading row `line` until `hold` resolves, like a worker paused by the OS or a long GC. */
export function stallsAt(line: number, hold: Promise<void>): Workflow<LedgerInput> {
  return withPost((post) => ({
    ...post,
    source: async function* (input, from) {
      for await (const row of post.source(input, from)) {
        if (row.line === line) await hold;
        yield row;
      }
    },
  }));
}
