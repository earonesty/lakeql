import { expect, it } from "vitest";
import type { Vector } from "./batch.js";
import { registerCompactionMetadata, retainsCompactionMetadata } from "./compaction-metadata.js";

it("keeps known column directories reused as struct fields and reachable null domains", () => {
  const nulls: Vector = { type: "null", length: 1 };
  const columns = { field: nulls };
  const struct: Vector = { type: "struct", fields: columns, length: 1 };
  registerCompactionMetadata(columns, "columns");
  registerCompactionMetadata(nulls, "vector");
  const reachable = new Set([nulls, struct]);
  expect(retainsCompactionMetadata(columns, reachable)).toBe(true);
  expect(retainsCompactionMetadata(nulls, reachable)).toBe(true);
  expect(retainsCompactionMetadata(columns, new Set())).toBe(false);
  expect(retainsCompactionMetadata(nulls, new Set())).toBe(false);
});

it("discards only registered batch metadata and preserves unknown allocations", () => {
  const batch = { rowCount: 0, columns: {} };
  expect(retainsCompactionMetadata(batch, new Set())).toBe(true);
  registerCompactionMetadata(batch, "batch");
  expect(retainsCompactionMetadata(batch, new Set())).toBe(false);
  expect(retainsCompactionMetadata({ rowCount: 0, columns: {} }, new Set())).toBe(true);
});

it("preserves child directories reused by another union descriptor", () => {
  const children: Vector[] = [{ type: "null", length: 1 }];
  registerCompactionMetadata(children, "children");
  const alias: Vector = {
    type: "union",
    tags: Uint8Array.of(0),
    offsets: Uint32Array.of(0),
    children,
  };
  expect(retainsCompactionMetadata(children, new Set([alias]))).toBe(true);
  expect(retainsCompactionMetadata(children, new Set(children))).toBe(false);
});
