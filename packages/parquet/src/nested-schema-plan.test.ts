import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { SchemaTree } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope, vectorFromValues, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { executeNestedSchema } from "./nested-schema-execute.js";
import { compileNestedProjection, compileNestedSchema } from "./nested-schema-plan.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/nested_maps.snappy.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(fixture))(
  "compiles a real nested map schema to generic operations and leaf sources",
  () => {
    const bytes = readFileSync(fixture);
    const metadata = parquetMetadata(
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
    );
    const paths = (metadata.row_groups[0]?.columns ?? []).map((chunk) => {
      if (!chunk.meta_data) throw Error("Missing column");
      return getSchemaPath(metadata.schema, chunk.meta_data.path_in_schema);
    });
    const root = paths[0]?.[0];
    if (!root) throw Error("Missing root");
    const scope = new MemoryScope(new MemoryBudget(65536));
    const selected = root.children[1];
    if (!selected) throw Error("Missing projection field");
    const projectedPaths = paths.filter((path) => path[1]?.element.name === selected.element.name);
    const projectionScope = new MemoryScope(new MemoryBudget(65536));
    const projected = compileNestedProjection(
      root,
      projectedPaths,
      [selected.element.name],
      projectionScope,
    );
    expect(projected.children).toHaveLength(1);
    expect(projected.children[0]?.node).toBe(selected);
    expect(projected.children[0]?.source).toBe(0);
    const projectionPeak = projectionScope.budget.peakBytes;
    projectionScope.dispose();
    const projectionExact = new MemoryScope(new MemoryBudget(projectionPeak));
    compileNestedProjection(root, projectedPaths, [selected.element.name], projectionExact);
    projectionExact.dispose();
    const projectionTight = new MemoryScope(new MemoryBudget(projectionPeak - 1));
    expect(() =>
      compileNestedProjection(root, projectedPaths, [selected.element.name], projectionTight),
    ).toThrow(/budget/);
    expect(projectionTight.budget.usedBytes).toBe(0);
    projectionTight.dispose();
    const rejected = new MemoryScope(new MemoryBudget(65536));
    expect(() =>
      compileNestedProjection(root, projectedPaths, [selected.element.name, "missing"], rejected),
    ).toThrow(/Missing/);
    expect(rejected.budget.usedBytes).toBe(0);
    expect(() =>
      compileNestedProjection(
        root,
        projectedPaths,
        [selected.element.name, selected.element.name],
        rejected,
      ),
    ).toThrow(/Duplicate/);
    expect(rejected.budget.usedBytes).toBe(0);
    rejected.dispose();
    const plan = compileNestedSchema(root, paths, scope);
    expect(plan.layout.kind).toBe("struct");
    expect(plan.children.map((child) => child.source)).toEqual([0, 3, 4]);
    const map = plan.children[0];
    expect(map?.layout.kind).toBe("map");
    expect(map?.children[1]?.layout.kind).toBe("map");
    expect(map?.children[1]?.children.map((child) => child.source)).toEqual([1, 2]);
    const peak = scope.budget.peakBytes;
    scope.dispose();
    const exact = new MemoryScope(new MemoryBudget(peak));
    compileNestedSchema(root, paths, exact);
    exact.dispose();
    const tight = new MemoryScope(new MemoryBudget(peak - 1));
    expect(() => compileNestedSchema(root, paths, tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
    tight.dispose();
  },
);

it("compiles deep schemas with a budgeted explicit stack and rolls back missing leaves", () => {
  const depth = 2000;
  let pathReads = 0;
  const names = Array.from({ length: depth }, (_, i) => `n${i}`);
  const path = new Array<SchemaTree>(depth + 1);
  let child: SchemaTree | undefined;
  for (let i = depth; i >= 0; i--) {
    const node: SchemaTree = {
      element:
        i === depth
          ? { name: names[i - 1] ?? "value", type: "INT32", repetition_type: "REQUIRED" }
          : {
              name: i === 0 ? "root" : (names[i - 1] ?? "group"),
              num_children: 1,
              repetition_type: "REQUIRED",
            },
      get path() {
        pathReads++;
        return names.slice(0, i);
      },
      count: depth - i + 1,
      children: child ? [child] : [],
    };
    path[i] = node;
    child = node;
  }
  const root = path[0];
  if (!root) throw Error("Missing root");
  const scope = new MemoryScope(new MemoryBudget(4 * 1024 * 1024));
  const compiled = compileNestedSchema(root, [path], scope);
  expect(pathReads).toBe(1);
  let plan = compiled;
  let seen = 0;
  while (plan.children[0]) {
    plan = plan.children[0];
    seen++;
  }
  expect(seen).toBe(depth);
  expect(plan.layout.kind).toBe("leaf");
  const inputs = [
    {
      schemaPath: path,
      values: vectorFromValues([42], scope),
      levels: {
        repetition: undefined,
        definition: undefined,
        rowsStarted: 1,
        presentCount: 1,
        valueOffset: 0,
      },
    },
  ];
  const execute = (target: MemoryScope) => {
    let vector = executeNestedSchema(compiled, inputs, target);
    for (let i = 0; i < depth; i++) {
      if (vector.type !== "struct") throw Error("Missing struct");
      const name = names[i];
      const field = name === undefined ? undefined : vector.fields[name];
      if (!field) throw Error("Missing field");
      vector = field;
    }
    expect(vectorValue(vector, 0)).toBe(42);
  };
  const execution = new MemoryScope(new MemoryBudget(4 * 1024 * 1024));
  execute(execution);
  const peak = execution.budget.peakBytes;
  execution.dispose();
  expect(execution.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  execute(exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => execute(tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const missing = new MemoryScope(new MemoryBudget(4096));
  expect(() => compileNestedSchema(root, [], missing)).toThrow(/Missing/);
  expect(missing.budget.usedBytes).toBe(0);
  missing.dispose();
});
