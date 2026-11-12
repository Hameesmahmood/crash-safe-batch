import { describe, expect, it } from "vitest";
import { batchStep, defineWorkflow, step } from "../src/engine/workflow.ts";

const noop = async () => {};
const empty = async function* () {};

describe("defineWorkflow", () => {
  it("refuses a workflow with no steps", () => {
    expect(() => defineWorkflow("w", [])).toThrow("has no steps");
  });

  it("refuses two steps with the same name", () => {
    expect(() => defineWorkflow("w", [step("a", { run: noop }), step("a", { run: noop })])).toThrow("two steps called a");
  });

  it("refuses a chunk size below 1 and attempts below 1", () => {
    expect(() => batchStep("b", { size: 0, source: empty, handle: noop })).toThrow("chunk size");
    expect(() => step("s", { run: noop, attempts: 0 })).toThrow("at least 1 attempt");
  });

  it("defaults to 3 attempts", () => {
    expect(step("s", { run: noop }).attempts).toBe(3);
    expect(batchStep("b", { size: 10, source: empty, handle: noop }).attempts).toBe(3);
  });
});
