import {
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBVariantValue,
} from "@duckdb/node-api";
import { isTimestampValue } from "../packages/core/dist/index.js";

// Reference-type-guided normalization preserves null containers and typed map keys.
// Timestamps compare exact epochs; timezone annotation validation is separate.
export function nestedDifference(actual, expected, path = "rows") {
  const fail = () => ({ path, actual: diagnostic(actual), expected: diagnostic(expected) });
  if (actual === null || expected === null || actual === undefined || expected === undefined)
    return Object.is(actual, expected) ? undefined : fail();
  if (expected instanceof DuckDBVariantValue) {
    if (expected.value === null)
      throw new Error("Embedded VARIANT null needs a separate SQL-null contract");
    return nestedDifference(actual, expected.value, path);
  }
  let nanos;
  if (expected instanceof DuckDBTimestampValue || expected instanceof DuckDBTimestampTZValue)
    nanos = expected.micros * 1000n;
  else if (expected instanceof DuckDBTimestampNanosecondsValue) nanos = expected.nanos;
  else if (expected instanceof DuckDBTimestampMillisecondsValue) nanos = expected.millis * 1000000n;
  else if (expected instanceof DuckDBTimestampSecondsValue) nanos = expected.seconds * 1000000000n;
  if (nanos !== undefined) {
    if (!expected.isFinite)
      throw new Error("Infinite timestamp comparison needs an explicit contract");
    const actualNanos = isTimestampValue(actual)
      ? actual.epochNanoseconds
      : actual instanceof Date
        ? BigInt(actual.getTime()) * 1000000n
        : undefined;
    return actualNanos === nanos ? undefined : fail();
  }
  if (expected instanceof DuckDBDateValue) {
    if (!expected.isFinite) throw new Error("Infinite date comparison needs an explicit contract");
    return actual instanceof Date && actual.getTime() === expected.days * 86400000
      ? undefined
      : fail();
  }
  if (expected instanceof DuckDBBlobValue) {
    if (!(actual instanceof Uint8Array)) return fail();
    return nestedDifference([...actual], [...expected.bytes], path);
  }
  if (expected instanceof DuckDBMapValue) {
    const entries =
      actual instanceof Map
        ? [...actual]
        : typeof actual === "object" &&
            !Array.isArray(actual) &&
            Object.getPrototypeOf(actual) === Object.prototype
          ? Object.entries(actual)
          : undefined;
    if (!entries || entries.length !== expected.entries.length) return fail();
    const remaining = new Set(entries.map((_, index) => index));
    for (const entry of expected.entries) {
      const index = [...remaining].find(
        (i) => nestedDifference(entries[i][0], entry.key, path) === undefined,
      );
      if (index === undefined) return fail();
      remaining.delete(index);
      const difference = nestedDifference(
        entries[index][1],
        entry.value,
        `${path}.map[${diagnostic(entry.key)}]`,
      );
      if (difference) return difference;
    }
    return;
  }
  if (expected instanceof DuckDBListValue) return nestedDifference(actual, expected.items, path);
  if (expected instanceof DuckDBStructValue)
    return nestedDifference(actual, expected.entries, path);
  if (typeof expected === "bigint") {
    const integer =
      typeof actual === "bigint"
        ? actual
        : Number.isSafeInteger(actual)
          ? BigInt(actual)
          : undefined;
    return integer === expected ? undefined : fail();
  }
  if (typeof expected !== "object") return Object.is(actual, expected) ? undefined : fail();
  if (typeof actual !== "object" || Array.isArray(actual) !== Array.isArray(expected))
    return fail();
  if (!Array.isArray(expected) && Object.getPrototypeOf(expected) !== Object.prototype)
    throw new Error(`Unsupported reference value: ${expected.constructor.name}`);
  if (Array.isArray(expected) && actual.length !== expected.length) return fail();
  const keys = Object.keys(expected);
  if (Object.keys(actual).length !== keys.length) return fail();
  for (const key of keys) {
    if (!Object.hasOwn(actual, key))
      return { path: `${path}.${key}`, actual: "missing", expected: diagnostic(expected[key]) };
    const difference = nestedDifference(actual[key], expected[key], `${path}.${key}`);
    if (difference) return difference;
  }
}
function diagnostic(value) {
  if (typeof value === "bigint") return `${value}n`;
  if (value === undefined) return "undefined";
  return JSON.stringify(value, (_, item) => (typeof item === "bigint" ? `${item}n` : item));
}
