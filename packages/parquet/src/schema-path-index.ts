import type { SchemaElement } from "hyparquet";
import type { SchemaTree } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";

/** One validated tree per consumer. Optional scope admits the tree and maximum
 * traversal workspace before construction. Returned lookup/path arrays belong to
 * the caller and must be admitted separately when used in a bounded pipeline.
 * Charges are reservation units, not a measurement of VM Map/closure overhead.
 */
export function schemaPathIndex(
  schema: readonly SchemaElement[],
  scope?: MemoryScope,
): (path: readonly string[]) => SchemaTree[] {
  if (!scope) return buildSchemaPathIndex(schema);
  const construction = scope.childScope();
  const scratch = scope.childScope();
  try {
    // Two directory Maps, one child Map per node, ancestry records, tree nodes,
    // child slots and three Map entries per non-root node (rounded up at root).
    const retained =
      2 * c.object +
      schema.length * (3 * c.object + 6 * c.property + c.array + c.arraySlot + 6 * c.property);
    scratch.allocate(
      c.array + schema.length * (c.arraySlot + c.object + 2 * c.property),
      "schema traversal workspace",
      () => undefined,
    );
    const lookup = construction.allocate(retained, "schema path index", () =>
      buildSchemaPathIndex(schema),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return lookup;
  } finally {
    scratch.dispose();
    construction.dispose();
  }
}

function buildSchemaPathIndex(
  schema: readonly SchemaElement[],
): (path: readonly string[]) => SchemaTree[] {
  const fail = (message: string): never => {
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", message);
  };
  const childrenByNode = new Map<SchemaTree, Map<string, SchemaTree>>();
  const ancestry = new Map<SchemaTree, { parent: SchemaTree | undefined; depth: number }>();
  const make = (element: SchemaElement, parent?: SchemaTree): SchemaTree => {
    const depth = parent === undefined ? 0 : (ancestry.get(parent)?.depth ?? 0) + 1;
    const node: SchemaTree = {
      element,
      // Most decoding decisions need nodes/levels, not materialized names.
      // Keep parent links instead of retaining every ancestor prefix (quadratic
      // for a chain). A caller receives an independent path only when requested.
      get path() {
        const names = new Array<string>(depth);
        let current: SchemaTree | undefined = node;
        for (let index = depth - 1; index >= 0; index--) {
          if (current === undefined) throw new Error("Missing schema ancestor");
          names[index] = current.element.name;
          current = ancestry.get(current)?.parent;
        }
        return names;
      },
      count: 1,
      children: [],
    };
    ancestry.set(node, { parent, depth });
    childrenByNode.set(node, new Map());
    return node;
  };
  const childCount = (element: SchemaElement): number => {
    const count = Number(element.num_children ?? 0);
    if (!Number.isSafeInteger(count) || count < 0)
      return fail("Invalid Parquet schema child count");
    return count;
  };
  const first = schema[0];
  if (first === undefined) return fail("Missing Parquet schema root");
  const root = make(first);
  const stack = [{ node: root, remaining: childCount(first) }];
  const finish = () => {
    const done = stack.pop();
    const parent = stack.at(-1);
    if (done !== undefined && parent !== undefined) parent.node.count += done.node.count;
  };
  for (let index = 1; index < schema.length; index++) {
    while (stack.at(-1)?.remaining === 0) finish();
    const parent = stack.at(-1);
    const element = schema[index];
    if (parent === undefined || element === undefined) return fail("Excess Parquet schema entries");
    const siblings = childrenByNode.get(parent.node);
    if (siblings?.has(element.name)) return fail("Duplicate Parquet schema child name");
    parent.remaining--;
    const node = make(element, parent.node);
    parent.node.children.push(node);
    siblings?.set(element.name, node);
    stack.push({ node, remaining: childCount(element) });
  }
  while (stack.at(-1)?.remaining === 0) finish();
  if (stack.length !== 0) return fail("Incomplete Parquet schema tree");
  return (names) => {
    const path = [root];
    let node = root;
    for (const name of names) {
      const child = childrenByNode.get(node)?.get(name);
      if (child === undefined)
        return fail(`Parquet schema element not found: ${JSON.stringify(names)}`);
      path.push(child);
      node = child;
    }
    return path;
  };
}
