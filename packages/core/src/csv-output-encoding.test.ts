import { expect, it } from "vitest";
import { encodeOwnedCsvOutput } from "./csv-output-encoding.js";
import { jsonSafeValue } from "./evaluator.js";
import { intervalValue } from "./interval.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { timestampFromEpoch } from "./timestamp.js";

function legacyCell(value: unknown): string | undefined {
  if (value === null || value === undefined) return "";
  const safe = jsonSafeValue(value);
  const text =
    typeof safe === "string" || typeof safe === "number" || typeof safe === "boolean"
      ? String(safe)
      : JSON.stringify(safe);
  return text !== undefined && /[",\n\r]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}
const cases: unknown[] = [
  null,
  undefined,
  false,
  123,
  NaN,
  Infinity,
  -Infinity,
  9007199254740993n,
  "plain",
  'comma,quote"\n\r雪😀',
  "\ud800x\udfff",
  new Uint8Array([0, 255]),
  { x: [null, "é", 1] },
  [1],
  [1, 2],
  {},
  [],
  Symbol("absent"),
  () => 1,
  timestampFromEpoch(123456789n, "nanos"),
  intervalValue("1 month 2 days"),
];
it.each(
  cases.map((value, index) => ({ value, index })),
)("encodes CSV case $index with exact admission and rollback", ({ value }) => {
  const row = { 'a,"\n': value };
  const columns = Object.keys(row);
  const expected = new TextEncoder().encode(
    `${columns.map(legacyCell).join(",")}\n${[legacyCell(value)].join(",")}\n`,
  );
  const pool = new MemoryBudget(1024 * 1024);
  const scope = new MemoryScope(pool);
  const owner = encodeOwnedCsvOutput(row, columns, true, scope);
  expect(owner.value).toEqual(expected);
  const peak = pool.peakBytes;
  scope.dispose();
  expect(pool.usedBytes).toBe(owner.bytes);
  owner.release();
  expect(pool.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  const same = encodeOwnedCsvOutput(row, columns, true, exact);
  expect(same.value).toEqual(expected);
  same.release();
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => encodeOwnedCsvOutput(row, columns, true, tight)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
it("preserves empty/header-only output and missing columns", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  for (const [row, columns, header, expected] of [
    [undefined, ["a", "b"], true, "a,b\n"],
    [{}, [], true, "\n\n"],
    [{}, ["missing"], false, "\n"],
    [undefined, [], false, ""],
  ] as const) {
    const output = encodeOwnedCsvOutput(row, columns, header, scope);
    expect(new TextDecoder().decode(output.value)).toBe(expected);
    output.release();
  }
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
it("rolls back prepared cells after a later cell fails", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const error = new Error("getter failed");
  expect(() =>
    encodeOwnedCsvOutput(
      {
        a: "prepared",
        get b() {
          throw error;
        },
      },
      ["a", "b"],
      true,
      scope,
    ),
  ).toThrow(error);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
