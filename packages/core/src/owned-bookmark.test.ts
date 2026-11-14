import { expect, it } from "vitest";
import { col, eq, lit } from "./expr.js";
import { type BookmarkInit, createBookmark } from "./manifest.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { createOwnedBookmark } from "./owned-bookmark.js";

function fixture(): BookmarkInit {
  return {
    planFingerprint: "fp",
    snapshot: "snapshot",
    position: { fileIndex: 0, rowGroup: 1, rowOffset: 2, taskId: "task", outputManifestCursor: 4 },
    query: {
      source: "file",
      select: ["key"],
      projections: { z: lit(new Uint8Array([1, 2])), a: col("key") },
      where: eq("key", 2),
      qualify: eq("rank", 1),
      distinct: true,
      limit: 10,
      offset: 2,
      batchSize: 3,
      hive: false,
      windows: {
        rank: {
          fn: "row_number",
          args: [],
          over: { partitionBy: [], orderBy: [{ expr: col("key") }] },
        },
      },
      orderBy: [{ column: "key", direction: "desc", nulls: "first" }],
    },
    writeState: {
      taskState: "running",
      idempotencyKey: "retry",
      multipart: {
        path: "out",
        uploadId: "upload",
        parts: [
          { partNumber: 2, etag: "b", byteSize: 20 },
          { partNumber: 1, etag: "a", byteSize: 10 },
        ],
      },
    },
    operatorState: {
      limitEmitted: 3,
      groupBy: new Uint8Array([3]),
      topK: { spillRef: "topk" },
      sort: new Uint8Array([4]),
      sketches: { z: new Uint8Array([5]), a: new Uint8Array([6]) },
    },
  };
}

it("matches bookmark normalization while independently owning nested expressions and bytes", () => {
  const init = fixture();
  const expected = createBookmark(fixture());
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const owner = createOwnedBookmark(init, scope);
  scope.dispose();
  expect(owner.value).toEqual(expected);
  expect(Object.keys(owner.value.query?.projections ?? {})).toEqual(["a", "z"]);
  init.query?.select?.push("changed");
  if (init.query?.projections?.z?.kind === "literal")
    (init.query.projections.z.value as Uint8Array).fill(9);
  if (init.operatorState?.groupBy instanceof Uint8Array) init.operatorState.groupBy.fill(8);
  expect(owner.value).toEqual(expected);
  expect(budget.usedBytes).toBeGreaterThan(0);
  owner.release();
  expect(budget.usedBytes).toBe(0);
});

it("admits exact normalization/output overlap and releases failure without consuming siblings", () => {
  const measure = new MemoryScope(new MemoryBudget(65536));
  const owner = createOwnedBookmark(fixture(), measure);
  const peak = measure.budget.peakBytes;
  owner.release();
  measure.dispose();
  for (const cap of [peak - 1, peak]) {
    const budget = new MemoryBudget(cap + 37);
    const sibling = budget.reserve(37, "sibling");
    const scope = new MemoryScope(budget);
    if (cap < peak)
      expect(() => createOwnedBookmark(fixture(), scope)).toThrow(
        expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
      );
    else createOwnedBookmark(fixture(), scope).release();
    scope.dispose();
    expect(budget.usedBytes).toBe(37);
    sibling.release();
  }
});

it("releases normalization scratch when window validation fails", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const init = fixture();
  if (init.query) init.query.windows = { bad: { fn: "row_number" } as never };
  expect(() => createOwnedBookmark(init, scope)).toThrow(/windows are invalid/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
