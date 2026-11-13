import type { ColumnDecoder, SchemaTree } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type OwnedMemory,
} from "lakeql-core";
import { type NestedSchemaLayout, nestedSchemaLayout } from "./nested-schema-layout.js";

export interface NestedSchemaPlan {
  node: SchemaTree;
  depth: number;
  source: number;
  layout: NestedSchemaLayout | { kind: "repeated" };
  children: NestedSchemaPlan[];
}
interface Frame {
  plan: NestedSchemaPlan;
  next: number;
  parent: OwnedMemory<Frame> | undefined;
}

/** Compile generic container operations once, with explicit admitted traversal frames.
 * Input schema trees/paths are borrowed. Unsupported layouts fail before any row reads.
 */
export function compileNestedSchema(
  node: SchemaTree,
  paths: readonly ColumnDecoder["schemaPath"][],
  scope: MemoryScope,
): NestedSchemaPlan {
  const construction = scope.childScope();
  const scratch = construction.childScope();
  let rootPath: readonly string[] = [];
  const make = (
    node: SchemaTree,
    parent: SchemaTree | undefined,
    depth: number,
    unwrapped: boolean,
    prefix?: ColumnDecoder["schemaPath"],
  ): NestedSchemaPlan => {
    let source = -1;
    for (let i = 0; i < paths.length; i++) {
      const path = paths[i];
      if (!path || path.length <= depth) continue;
      let matches = true;
      for (let j = 1; j <= depth; j++) {
        const name =
          prefix === undefined
            ? rootPath[j - 1]
            : j === depth
              ? node.element.name
              : prefix[j]?.element.name;
        if (path[j]?.element.name !== name) {
          matches = false;
          break;
        }
      }
      if (matches) {
        source = i;
        break;
      }
    }
    if (source < 0)
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
        "Missing nested schema leaf source",
      );
    const layout =
      node.element.repetition_type === "REPEATED" && !unwrapped
        ? construction.allocate(c.object + c.property, "repeated schema layout", () => ({
            kind: "repeated" as const,
          })).value
        : nestedSchemaLayout(node, parent, construction);
    if (!layout)
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
        "Unsupported nested schema layout",
      );
    const count =
      layout.kind === "leaf"
        ? 0
        : layout.kind === "struct"
          ? layout.children.length
          : layout.kind === "map" && layout.value
            ? 2
            : 1;
    const children = construction.allocate(
      c.array + count * c.arraySlot,
      "nested plan children",
      () => new Array<NestedSchemaPlan>(count),
    ).value;
    return construction.allocate(c.object + 5 * c.property, "nested schema plan node", () => ({
      node,
      depth,
      source,
      layout,
      children,
    })).value;
  };
  const frame = (plan: NestedSchemaPlan, parent: OwnedMemory<Frame> | undefined) =>
    scratch.allocate(
      c.object + 3 * c.property,
      "nested schema traversal frame",
      (): Frame => ({ plan, next: 0, parent }),
    );
  try {
    rootPath = node.path;
    const depth = rootPath.length;
    const parent = paths.find(
      (path) =>
        path.length > depth && rootPath.every((name, i) => path[i + 1]?.element.name === name),
    )?.[depth - 1];
    const root = make(node, parent, depth, false);
    let current: OwnedMemory<Frame> | undefined = frame(root, undefined);
    while (current) {
      const state: Frame = current.value;
      if (state.next === state.plan.children.length) {
        const parent = state.parent;
        current.release();
        current = parent;
        continue;
      }
      const { plan } = state;
      const index = state.next++;
      const layout = plan.layout;
      let child: NestedSchemaPlan;
      if (layout.kind === "repeated")
        child = make(
          plan.node,
          paths[plan.source]?.[plan.depth - 1],
          plan.depth,
          true,
          paths[plan.source],
        );
      else if (layout.kind === "list")
        child = make(
          layout.element,
          layout.element === layout.repeated ? plan.node : layout.repeated,
          plan.depth + (layout.element === layout.repeated ? 1 : 2),
          layout.element === layout.repeated,
          paths[plan.source],
        );
      else if (layout.kind === "map") {
        const node = index === 0 ? layout.key : layout.value;
        if (!node) throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Missing map plan child");
        child = make(node, layout.entries, plan.depth + 2, false, paths[plan.source]);
      } else if (layout.kind === "struct") {
        const node = layout.children[index];
        if (!node) throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Missing struct plan child");
        child = make(node, plan.node, plan.depth + 1, false, paths[plan.source]);
      } else throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Unexpected leaf plan child");
      plan.children[index] = child;
      current = frame(child, current);
    }
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return root;
  } finally {
    scratch.dispose();
    construction.dispose();
  }
}

/** Compile selected top-level fields in requested order. Each selected subtree
 * still requires all its leaf sources; unselected fields require no data reads.
 */
export function compileNestedProjection(
  root: SchemaTree,
  paths: readonly ColumnDecoder["schemaPath"][],
  names: readonly string[],
  scope: MemoryScope,
): NestedSchemaPlan {
  const construction = scope.childScope();
  try {
    if (root.path.length !== 0 || names.length === 0)
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
        "Nested projection requires root fields",
      );
    const children = construction.allocate(
      c.array + names.length * c.arraySlot,
      "nested projected plans",
      () => new Array<NestedSchemaPlan>(names.length),
    ).value;
    const nodes = construction.allocate(
      c.array + names.length * c.arraySlot,
      "nested projected schema fields",
      () => new Array<SchemaTree>(names.length),
    ).value;
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      if (names.indexOf(name ?? "") !== i)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Duplicate nested projected field");
      const node = root.children.find((child) => child.element.name === name);
      if (!node)
        throw new LakeqlError(
          "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
          "Missing nested projected field",
        );
      nodes[i] = node;
      children[i] = compileNestedSchema(node, paths, construction);
    }
    const layout = construction.allocate(
      c.object + 2 * c.property,
      "projected struct layout",
      () => ({
        kind: "struct" as const,
        children: nodes,
      }),
    ).value;
    const plan = construction.allocate(
      c.object + 5 * c.property,
      "nested projected root",
      (): NestedSchemaPlan => ({
        node: root,
        depth: 0,
        source: children[0]?.source ?? 0,
        layout,
        children,
      }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return plan;
  } finally {
    construction.dispose();
  }
}
