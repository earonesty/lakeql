import { jsonStringUtf8Bytes } from "lakeql-core";
export const metadataTag = "__lakeql_parquet_metadata_type";
const byteTagOverhead = JSON.stringify({ [metadataTag]: "bytes", value: "" }).length;
const bigintTagOverhead = JSON.stringify({ [metadataTag]: "bigint", value: "" }).length;

/** Exact tagged JSON UTF8 cache cost without building the complete JSON or byte buffer. */
export function parquetMetadataCacheBytes(value: unknown): number {
  const active = new Set<object>();
  const visit = (input: unknown, key: string): number | undefined => {
    let value = input;
    if (
      typeof value === "object" &&
      value !== null &&
      "toJSON" in value &&
      typeof value.toJSON === "function"
    ) {
      value = value.toJSON(key);
    }
    if (value === null) return 4;
    switch (typeof value) {
      case "string":
        return jsonStringUtf8Bytes(value);
      case "number":
        return Number.isFinite(value) ? String(value).length : 4;
      case "boolean":
        return value ? 4 : 5;
      case "bigint":
        return bigintTagOverhead + String(value).length;
      case "undefined":
      case "function":
      case "symbol":
        return undefined;
    }
    if (value instanceof Uint8Array || value instanceof ArrayBuffer)
      return byteTagOverhead + value.byteLength * 2;
    if (value instanceof Number || value instanceof String || value instanceof Boolean)
      return visit(value.valueOf(), key);
    if (active.has(value)) throw new TypeError("Converting circular structure to JSON");
    active.add(value);
    try {
      let bytes = 2;
      let count = 0;
      if (Array.isArray(value)) {
        for (let index = 0; index < value.length; index++) {
          bytes += visit(value[index], String(index)) ?? 4;
          count++;
        }
      } else {
        for (const key of Object.keys(value)) {
          const size = visit((value as Record<string, unknown>)[key], key);
          if (size === undefined) continue;
          bytes += jsonStringUtf8Bytes(key) + 1 + size;
          count++;
        }
      }
      return bytes + Math.max(0, count - 1);
    } finally {
      active.delete(value);
    }
  };
  return visit(value, "") ?? 0;
}
