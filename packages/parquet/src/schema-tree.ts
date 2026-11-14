import type { parquetSchema } from "hyparquet";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";

export interface SchemaShapeNode {
  element: ReturnType<typeof parquetSchema>["element"];
  children: SchemaShapeNode[];
}

/** Planning needs element/child topology, not a copied root-to-node path per node. */
export function schemaShapeTree(
  schema: SchemaShapeNode["element"][],
  parent?: MemoryScope,
): SchemaShapeNode {
  const output = parent?.childScope();
  const scratch = output?.childScope();
  try {
    const create = (element: SchemaShapeNode["element"]): SchemaShapeNode => {
      const build = () => ({ element, children: [] as SchemaShapeNode[] });
      return output === undefined
        ? build()
        : output.allocate(c.object + 2 * c.property + c.array, "schema topology node", build).value;
    };
    const count = (element: SchemaShapeNode["element"]) => {
      const value = element.num_children ?? 0;
      if (!Number.isSafeInteger(value) || value < 0)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid schema child count");
      return value;
    };
    const first = schema[0];
    if (first === undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing schema root");
    const root = create(first);
    const stack: { node: SchemaShapeNode; remaining: number }[] = [];
    scratch?.allocate(c.array, "schema topology stack", () => undefined);
    const push = (node: SchemaShapeNode) => {
      const remaining = count(node.element);
      if (remaining === 0) return;
      scratch?.allocate(
        c.object + 2 * c.property + c.arraySlot,
        "schema topology frame",
        () => undefined,
      );
      stack.push({ node, remaining });
    };
    push(root);
    for (let index = 1; index < schema.length; index++) {
      while (stack.length > 0 && stack[stack.length - 1]?.remaining === 0) stack.pop();
      const frame = stack[stack.length - 1];
      const element = schema[index];
      if (frame === undefined || element === undefined)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Extra schema elements");
      output?.allocate(c.arraySlot, "schema topology child slot", () => undefined);
      const node = create(element);
      frame.node.children.push(node);
      frame.remaining--;
      push(node);
    }
    if (stack.some((frame) => frame.remaining !== 0))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Truncated schema topology");
    if (output !== undefined && parent !== undefined) {
      const owner = output.retainAllocations(root);
      try {
        return parent.adopt(owner).value;
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return root;
  } finally {
    scratch?.dispose();
    output?.dispose();
  }
}
