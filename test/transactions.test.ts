import { describe, expect, it } from "vitest";
import { CSV_HEADER, generateTransactions } from "../src/example/transactions.ts";

const lines = (rows: number, seed: number) => [...generateTransactions({ rows, seed })];

describe("generateTransactions", () => {
  it("gives the same file for the same seed", () => {
    expect(lines(1000, 7)).toEqual(lines(1000, 7));
  });

  it("gives a different file for a different seed", () => {
    expect(lines(1000, 7)).not.toEqual(lines(1000, 8));
  });

  it("writes a header, then one well-formed row per transaction", () => {
    const [header, ...rows] = lines(500, 1);
    expect(header).toBe(CSV_HEADER);
    expect(rows).toHaveLength(500);
    for (const [i, row] of rows.entries()) {
      const [txn, account, amount, at] = row.split(",");
      expect(txn).toBe(`t${String(i + 1).padStart(7, "0")}`);
      expect(Number(account)).toBeGreaterThanOrEqual(1);
      expect(Number(account)).toBeLessThanOrEqual(1000);
      expect(Number.isInteger(Number(amount)) && Number(amount) !== 0).toBe(true);
      expect(Number.isNaN(Date.parse(at!))).toBe(false);
    }
  });
});
