import { isDecimalValue, isTimestampValue, type Row } from "lakeql-core";

export function normalizeDecodedRows(rows: Row[]): Row[] {
  return rows.map((row) => normalizeDecodedValue(row) as Row);
}

function normalizeDecodedValue(value: unknown): unknown {
  if (value === undefined) return null;
  if (
    value instanceof Uint8Array ||
    value instanceof Date ||
    isTimestampValue(value) ||
    isDecimalValue(value)
  )
    return value;
  if (Array.isArray(value)) return value.map(normalizeDecodedValue);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, nested] of value instanceof Map ? value : Object.entries(value)) {
      Object.defineProperty(out, key, {
        value: normalizeDecodedValue(nested),
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    return out;
  }
  return value;
}
