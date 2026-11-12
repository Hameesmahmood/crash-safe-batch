/**
 * The run state machine. Every status change in the engine goes through `assertMove`, and the SQL
 * that applies it also checks the current status, so an illegal move fails in both places.
 *
 *   pending ──> running ──> succeeded
 *                  │
 *                  └──> compensating ──> compensated
 *                             │
 *                             └──> failed   (an undo itself failed for good; needs a human)
 */
export const RUN_STATUSES = ["pending", "running", "succeeded", "compensating", "compensated", "failed"] as const;
export type RunStatus = (typeof RUN_STATUSES)[number];

const NEXT: Record<RunStatus, readonly RunStatus[]> = {
  pending: ["running"],
  running: ["succeeded", "compensating"],
  compensating: ["compensated", "failed"],
  succeeded: [],
  compensated: [],
  failed: [],
};

export class IllegalTransition extends Error {
  readonly from: RunStatus;
  readonly to: RunStatus;

  constructor(from: RunStatus, to: RunStatus) {
    super(`A run can't move from ${from} to ${to}.`);
    this.name = "IllegalTransition";
    this.from = from;
    this.to = to;
  }
}

export function canMove(from: RunStatus, to: RunStatus): boolean {
  return NEXT[from].includes(to);
}

export function assertMove(from: RunStatus, to: RunStatus): void {
  if (!canMove(from, to)) throw new IllegalTransition(from, to);
}

/** A finished run never runs again. */
export function isFinished(status: RunStatus): boolean {
  return NEXT[status].length === 0;
}

/** Statuses a worker can pick up: waiting, or in progress with a worker that stopped renewing its lease. */
export const CLAIMABLE: readonly RunStatus[] = ["pending", "running", "compensating"];
