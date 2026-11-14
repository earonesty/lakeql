import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { scopedParquetCompressors } from "./compressors.js";
import { GZIP_WORKSPACE_BYTES } from "./gzip.js";

it("decodes GZIP within exact output and workspace admission in workerd", () => {
  const expected = new TextEncoder().encode("bounded gzip in workers");
  const bytes = Uint8Array.from([
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 202, 47, 205, 75, 73, 77, 81, 72, 175, 202, 44, 80, 200,
    204, 83, 40, 207, 47, 202, 78, 45, 42, 6, 0, 216, 112, 244, 172, 23, 0, 0, 0,
  ]);
  const scope = new MemoryScope(new MemoryBudget(expected.length + GZIP_WORKSPACE_BYTES));
  scope.admit(expected.length + GZIP_WORKSPACE_BYTES, "workerd gzip");
  const codec = scopedParquetCompressors(scope).GZIP;
  if (!codec) throw new Error("Missing GZIP codec");
  expect(codec(bytes, expected.length)).toEqual(expected);
  expect(scope.budget.usedBytes).toBe(expected.length);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
