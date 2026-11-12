import { describe, expect, it } from "vitest";
import { assertMove, canMove, IllegalTransition, isFinished, RUN_STATUSES, type RunStatus } from "../src/engine/states.ts";

const allowed: [RunStatus, RunStatus][] = [
  ["pending", "running"],
  ["running", "succeeded"],
  ["running", "compensating"],
  ["compensating", "compensated"],
  ["compensating", "failed"],
];

describe("run state machine", () => {
  it("allows exactly the documented moves", () => {
    for (const from of RUN_STATUSES) {
      for (const to of RUN_STATUSES) {
        const expected = allowed.some(([f, t]) => f === from && t === to);
        expect(canMove(from, to), `${from} -> ${to}`).toBe(expected);
      }
    }
  });

  it("names the illegal move when it refuses one", () => {
    expect(() => assertMove("compensated", "running")).toThrow(IllegalTransition);
    expect(() => assertMove("compensated", "running")).toThrow("A run can't move from compensated to running.");
  });

  it("treats succeeded, compensated and failed as final", () => {
    expect(RUN_STATUSES.filter(isFinished)).toEqual(["succeeded", "compensated", "failed"]);
  });
});
