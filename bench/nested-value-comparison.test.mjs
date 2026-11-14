import assert from "node:assert/strict";
import test from "node:test";
import {
  DuckDBBlobValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTimestampValue,
  DuckDBVariantValue,
} from "@duckdb/node-api";
import { timestampFromEpoch } from "../packages/core/dist/index.js";
import { nestedDifference } from "./nested-value-comparison.mjs";

test("compares extended-year timestamps by exact epoch, without string parsing", () => {
  const micros = 1608814800000000000n;
  const reference = new DuckDBTimestampValue(micros);
  assert.equal(nestedDifference(timestampFromEpoch(micros, "micros"), reference), undefined);
  assert.ok(nestedDifference(timestampFromEpoch(micros + 1n, "micros"), reference));
  assert.ok(nestedDifference(reference.toString(), reference));
});
test("distinguishes null containers from containers of null members", () => {
  assert.ok(nestedDifference(null, new DuckDBStructValue({ a: null })));
  assert.equal(nestedDifference({ a: null }, new DuckDBStructValue({ a: null })), undefined);
  assert.ok(nestedDifference([], new DuckDBListValue([null])));
  assert.ok(nestedDifference([null], null));
});
test("preserves map membership and distinguishes absent keys from null values", () => {
  const reference = new DuckDBMapValue([{ key: "k1", value: 1 }]);
  assert.equal(nestedDifference({ k1: 1 }, reference), undefined);
  assert.ok(nestedDifference({ k1: 1, k3: null }, reference));
  assert.ok(nestedDifference({ k1: null }, new DuckDBMapValue([])));
});
test("preserves typed map keys and ignores entry order", () => {
  const reference = new DuckDBMapValue([
    { key: 2n, value: "b" },
    { key: 1n, value: "a" },
  ]);
  assert.equal(
    nestedDifference(
      new Map([
        [1n, "a"],
        [2n, "b"],
      ]),
      reference,
    ),
    undefined,
  );
  assert.ok(nestedDifference({ 1: "a", 2: "b" }, reference));
});
test("preserves binary bytes and bigint precision", () => {
  assert.equal(
    nestedDifference(Uint8Array.of(255, 0), new DuckDBBlobValue(Uint8Array.of(255, 0))),
    undefined,
  );
  assert.ok(nestedDifference(Uint8Array.of(255), new DuckDBBlobValue(Uint8Array.of(255, 0))));
  assert.ok(nestedDifference(Number(9007199254740993n), 9007199254740993n));
  assert.equal(nestedDifference(9007199254740993n, 9007199254740993n), undefined);
});
test("unwraps non-null variants but refuses ambiguous embedded nulls", () => {
  assert.equal(
    nestedDifference(["a"], new DuckDBVariantValue(new DuckDBListValue(["a"]))),
    undefined,
  );
  assert.throws(() => nestedDifference({}, new DuckDBVariantValue(null)), /separate SQL-null/);
});
test("rejects missing struct members and differing list lengths", () => {
  assert.ok(nestedDifference({}, new DuckDBStructValue({ a: null })));
  assert.ok(nestedDifference([], new Array(2)));
});
