import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { gte, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { footerRowGroupView, indexFooterRowGroups } from "./footer-directory.js";
import { decodeRowGroupMetadata } from "./row-group-metadata.js";
import { planRowGroupsFromMetadata, planRowGroupsFromSource } from "./row-group-plan.js";

it("plans directory-decoded groups in one pass with identical offsets and pruning", () => {
  const bytes = parquetWriteBuffer({
    rowGroupSize: Array(20).fill(3),
    columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 60 }, (_, i) => i) }],
  });
  const metadata = parquetMetadata(bytes);
  const length = new DataView(bytes).getUint32(bytes.byteLength - 8, true);
  const view = new DataView(bytes, bytes.byteLength - 8 - length, length);
  const scope = new MemoryScope(new MemoryBudget(160));
  const directory = indexFooterRowGroups(view, scope);
  const leaves = metadata.schema.filter((element) => element.type);
  for (const where of [undefined, gte("id", 28), gte("id", 100)]) {
    let visited = 0,
      closed = false;
    function* groups() {
      try {
        for (let index = 0; index < 20; index++) {
          visited++;
          yield decodeRowGroupMetadata(footerRowGroupView(view, directory, index), leaves);
        }
      } finally {
        closed = true;
      }
    }
    const actual = planRowGroupsFromSource({ schema: metadata.schema }, groups(), where);
    expect(actual).toEqual(planRowGroupsFromMetadata(metadata, where));
    expect(visited).toBe(20);
    expect(closed).toBe(true);
  }
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("stops decoding groups when a streaming plan consumer closes", async () => {
  const { iteratePlannedRowGroups } = await import("./row-group-plan.js");
  const metadata = parquetMetadata(
    parquetWriteBuffer({
      rowGroupSize: [2, 2, 2],
      columnData: [{ name: "id", type: "INT32", data: [0, 1, 2, 3, 4, 5] }],
    }),
  );
  let visits = 0;
  let closed = false;
  function* groups() {
    try {
      for (const group of metadata.row_groups) {
        visits++;
        yield group;
      }
    } finally {
      closed = true;
    }
  }
  const plan = iteratePlannedRowGroups(metadata, groups(), gte("id", 2));
  expect(plan.next().value).toEqual(planRowGroupsFromMetadata(metadata, gte("id", 2)).rowGroups[0]);
  expect(visits).toBe(2);
  plan.return();
  expect(visits).toBe(2);
  expect(closed).toBe(true);
});

it("summarizes streaming plans without retaining per-group output entries", async () => {
  const { iteratePlannedRowGroups, summarizePlannedRowGroups } = await import(
    "./row-group-plan.js"
  );
  const metadata = parquetMetadata(
    parquetWriteBuffer({
      rowGroupSize: [2, 2, 2, 2],
      columnData: [{ name: "id", type: "INT32", data: [0, 1, 9, 10, 2, 3, 11, 12] }],
    }),
  );
  for (const where of [undefined, gte("id", 9), gte("id", 99)]) {
    const oracle = planRowGroupsFromMetadata(metadata, where);
    const summary = summarizePlannedRowGroups(
      iteratePlannedRowGroups(metadata, metadata.row_groups, where),
    );
    expect(summary).toEqual({
      plannedCount: oracle.rowGroups.length,
      rowGroupRanges: oracle.rowGroupRanges,
    });
    expect(summary).not.toHaveProperty("rowGroups");
  }
});

it("omits physical range inference when the requested plan shape does not need it", async () => {
  const { iteratePlannedRowGroups } = await import("./row-group-plan.js");
  const metadata = parquetMetadata(
    parquetWriteBuffer({
      rowGroupSize: [2],
      columnData: [{ name: "id", type: "INT32", data: [0, 1] }],
    }),
  );
  const group = metadata.row_groups[0];
  if (!group) throw new Error("Missing fixture group");
  let offsetReads = 0;
  const observed = {
    ...group,
    get file_offset() {
      offsetReads++;
      return group.file_offset;
    },
  };
  const entries = [
    ...iteratePlannedRowGroups(metadata, [observed], undefined, { includeByteRanges: false }),
  ];
  expect(entries).toEqual([{ index: 0, rowStart: 0, rowCount: 2 }]);
  expect(offsetReads).toBe(0);
  const full = [...iteratePlannedRowGroups(metadata, [observed], undefined)];
  expect(full[0]?.byteRange).toBeDefined();
  expect(offsetReads).toBe(1);
});
