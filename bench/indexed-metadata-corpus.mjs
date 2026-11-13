import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { gte, MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { openIndexedParquetFooter } from "../packages/parquet/dist/indexed-footer.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import {
  planRowGroupsFromMetadata,
  planRowGroupsFromSource,
} from "../packages/parquet/dist/row-group-plan.js";

const root = new URL("../fixtures/external/parquet-testing/", import.meta.url);
const results = [];
for (const path of (await readdir(root, { recursive: true }))
  .filter((p) => p.endsWith(".parquet"))
  .sort()) {
  const input = await readFile(new URL(path, root));
  const bytes = input.buffer.slice(input.byteOffset, input.byteOffset + input.length);
  const entry = { path, sha256: createHash("sha256").update(input).digest("hex") };
  let scope;
  let phase = "ordinary metadata";
  try {
    const oracle = await readParquetMetadataFromFile({
      byteLength: bytes.byteLength,
      slice: async (start, end) => bytes.slice(start, end),
    });
    phase = "indexed metadata parity";
    scope = new MemoryScope(new MemoryBudget(oracle.row_groups.length * 8));
    const source = openIndexedParquetFooter(bytes, scope);
    const { row_groups, ...envelope } = oracle;
    assert.deepEqual(source.metadata, envelope);
    assert.equal(source.rowGroupCount, row_groups.length);
    for (let i = 0; i < row_groups.length; i++)
      assert.deepEqual(source.readRowGroup(i), row_groups[i]);
    phase = "plan parity";
    const predicates = [undefined];
    for (const column of row_groups[0]?.columns ?? []) {
      const name = column.meta_data?.path_in_schema;
      const maximum = column.meta_data?.statistics?.max_value;
      if (name?.length === 1 && typeof maximum === "number" && Number.isFinite(maximum))
        predicates.push(gte(name[0], maximum));
    }
    let plans = 0,
      unsupportedPlans = 0;
    for (const where of predicates) {
      const outcome = (fn) => {
        try {
          return { plan: fn() };
        } catch (error) {
          return { error: { code: error.code, message: error.message } };
        }
      };
      function* groups() {
        for (let i = 0; i < source.rowGroupCount; i++) yield source.readRowGroup(i);
      }
      const expected = outcome(() => planRowGroupsFromMetadata(oracle, where));
      const actual = outcome(() => planRowGroupsFromSource(source.metadata, groups(), where));
      assert.deepEqual(actual, expected);
      if (expected.error) unsupportedPlans++;
      else plans++;
    }
    results.push({
      ...entry,
      status: "validated",
      groups: source.rowGroupCount,
      plans,
      unsupportedPlans,
    });
  } catch (error) {
    results.push({
      ...entry,
      status: "rejected",
      phase,
      error: String(error.message).slice(0, 1000),
    });
  } finally {
    scope?.dispose();
    if (scope) assert.equal(scope.budget.usedBytes, 0);
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local real external Parquet files; indexed envelope/group equality with ordinary normalized reader, planner outcomes compared including unsupported schemas. Exact directory capacity only; no full-file data decode or whole-query heap claim.",
      results,
    },
    null,
    2,
  ),
);
// Oracle rejection is reported separately; any indexed mismatch fails this check.
if (results.some((result) => result.status === "rejected" && result.phase !== "ordinary metadata"))
  process.exitCode = 1;
// Optional prior artifact guards ordinary-reader regressions as well as parity.
// Compare only identical fixture hashes; changed fixture bytes need separate review.
if (process.argv[2]) {
  const baseline = JSON.parse(await readFile(process.argv[2], "utf8"));
  const current = new Map(results.map((result) => [result.path, result]));
  for (const previous of baseline.results) {
    const result = current.get(previous.path);
    if (
      previous.status === "validated" &&
      (!result || (result.sha256 === previous.sha256 && result.status !== "validated"))
    ) {
      console.error(`Previously validated fixture regressed: ${previous.path}`);
      process.exitCode = 1;
    }
  }
}
