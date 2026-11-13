import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type OwnedMemory,
  type Vector,
  vectorLength,
} from "lakeql-core";
import { nestedLeafSlots } from "./nested-leaf-slots.js";
import { nestedListVector } from "./nested-list-vector.js";
import { nestedMapVector } from "./nested-map-vector.js";
import { type NestedNodeSource, validateNestedNodeAlignment } from "./nested-node-alignment.js";
import { indexNestedNode } from "./nested-node-index.js";
import type { NestedSchemaPlan } from "./nested-schema-plan.js";
import { type NestedStructField, nestedStructVector } from "./nested-struct-vector.js";

export interface NestedSchemaInput extends NestedNodeSource {
  values: Vector;
}
interface Frame {
  plan: NestedSchemaPlan;
  next: number;
  children: Vector[];
  parent: OwnedMemory<Frame> | undefined;
}

/** Execute a compiled schema over aligned, normalized column rows. Payload and schema
 * ownership stay with the caller; new containers/routing transfer atomically on success.
 */
export function executeNestedSchema(
  plan: NestedSchemaPlan,
  inputs: readonly NestedSchemaInput[],
  scope: MemoryScope,
): Vector {
  const construction = scope.childScope();
  const scratch = construction.childScope();
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Incomplete nested schema input");
  const frame = (plan: NestedSchemaPlan, parent: OwnedMemory<Frame> | undefined) =>
    scratch.allocate(
      c.object + 4 * c.property + c.array + plan.children.length * c.arraySlot,
      "nested execution frame",
      (): Frame => ({ plan, parent, next: 0, children: new Array<Vector>(plan.children.length) }),
    );
  const build = (plan: NestedSchemaPlan, children: readonly Vector[]): Vector => {
    const source = inputs[plan.source];
    if (!source) throw invalid();
    for (const child of plan.children)
      if (child.source !== plan.source) {
        const other = inputs[child.source];
        if (!other) throw invalid();
        validateNestedNodeAlignment(source, other, plan.depth, construction);
        if (plan.layout.kind === "map")
          validateNestedNodeAlignment(source, other, plan.depth + 1, construction);
      }
    const workspace = scratch.childScope();
    try {
      const node = (depth: number, retainValidity = true) => {
        const index = indexNestedNode(
          source.levels,
          source.schemaPath,
          depth,
          workspace,
          retainValidity ? construction : undefined,
        );
        return index;
      };
      const child = (i: number): Vector => {
        const value = children[i];
        if (!value) throw invalid();
        return value;
      };
      switch (plan.layout.kind) {
        case "leaf":
          return nestedLeafSlots(source.levels, source.schemaPath, source.values, construction);
        case "repeated":
          return nestedListVector(
            node(plan.depth - 1),
            node(plan.depth, false),
            child(0),
            construction,
          );
        case "list":
          return nestedListVector(
            node(plan.depth),
            node(plan.depth + 1, false),
            child(0),
            construction,
          );
        case "map": {
          // Legacy Presto/Trino files mark keys optional. Accept the physical
          // shape while validating actual key presence without materializing keys.
          const keyPlan = plan.children[0];
          if (keyPlan?.node.element.repetition_type === "OPTIONAL") {
            const keyInput = inputs[keyPlan.source];
            if (!keyInput) throw invalid();
            const keyIndex = indexNestedNode(
              keyInput.levels,
              keyInput.schemaPath,
              keyPlan.depth,
              workspace,
            );
            if (keyIndex.valid.some((valid) => valid === 0))
              throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Null Parquet map key");
          }
          const keys = child(0);
          const values =
            children[1] ??
            construction.allocate(
              c.object + 2 * c.property,
              "omitted map values",
              (): Vector => ({
                type: "null",
                length: vectorLength(keys),
              }),
            ).value;
          return nestedMapVector(
            node(plan.depth),
            node(plan.depth + 1, false),
            keys,
            values,
            construction,
          );
        }
        case "struct": {
          const temporary = scratch.childScope();
          try {
            const fields = temporary.allocate(
              c.object + children.length * (c.property + c.object + 2 * c.property),
              "nested struct assembly fields",
              (): Record<string, NestedStructField> => Object.create(null),
            ).value;
            for (let i = 0; i < plan.children.length; i++) {
              const spec = plan.children[i];
              if (!spec) throw invalid();
              fields[spec.node.element.name] = {
                values: child(i),
                alignment: spec.layout.kind === "repeated" ? "all-parents" : "present-parents",
              };
            }
            return nestedStructVector(node(plan.depth).valid, fields, construction);
          } finally {
            temporary.dispose();
          }
        }
      }
    } finally {
      workspace.dispose();
    }
  };
  try {
    let current: OwnedMemory<Frame> | undefined = frame(plan, undefined);
    let result: Vector | undefined;
    while (current) {
      const state: Frame = current.value;
      if (state.next < state.plan.children.length) {
        const child = state.plan.children[state.next++];
        if (!child) throw invalid();
        current = frame(child, current);
        continue;
      }
      const value = build(state.plan, state.children);
      const parent = state.parent;
      current.release();
      if (parent) parent.value.children[parent.value.next - 1] = value;
      else result = value;
      current = parent;
    }
    if (!result) throw invalid();
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    scratch.dispose();
    construction.dispose();
  }
}
