import type { parquetSchema } from "hyparquet";
import { MATERIALIZATION_CHARGES as c, type MemoryScope } from "lakeql-core";

type SchemaNode = ReturnType<typeof parquetSchema>;

/** Build one shape record per node without recursive calls or spread copies.
 * Element values are borrowed from metadata; callers retain that backing. */
export function parquetColumnShape(
  root: SchemaNode,
  parent?: MemoryScope,
): Record<string, unknown> {
  const outputScope = parent?.childScope();
  const scratch = outputScope?.childScope();
  try {
    const create = (node: SchemaNode, includeName: boolean): Record<string, unknown> => {
      const element = node.element as unknown as Record<string, unknown>;
      const build = (): Record<string, unknown> => {
        const value: Record<string, unknown> = {
          type: element.type,
          converted_type: element.converted_type,
          logical_type: element.logical_type,
          repetition_type: element.repetition_type,
          type_length: element.type_length,
          precision: element.precision,
          scale: element.scale,
          children: [] as Record<string, unknown>[],
        };
        if (includeName) value.name = element.name;
        return value;
      };
      return outputScope === undefined
        ? build()
        : outputScope.allocate(
            c.object + (includeName ? 9 : 8) * c.property + c.array,
            "schema shape record",
            build,
          ).value;
    };
    const result = create(root, false);
    const stack: {
      node: SchemaNode;
      output: Record<string, unknown>;
      next: number;
      release?: () => void;
    }[] = [];
    scratch?.allocate(c.array, "schema shape stack", () => undefined);
    let capacity = 0;
    const push = (node: SchemaNode, output: Record<string, unknown>) => {
      if (stack.length === capacity) {
        scratch?.allocate(c.arraySlot, "schema shape stack slot", () => undefined);
        capacity++;
      }
      const frame = scratch?.allocate(
        c.object + 4 * c.property,
        "schema shape frame",
        () => undefined,
      );
      stack.push({
        node,
        output,
        next: 0,
        ...(frame === undefined ? {} : { release: () => frame.release() }),
      });
    };
    push(root, result);
    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      if (frame === undefined) break;
      const child = frame.node.children[frame.next++];
      if (child === undefined) {
        stack.pop();
        frame.release?.();
        continue;
      }
      const output = create(child, true);
      outputScope?.allocate(c.arraySlot, "schema shape child slot", () => undefined);
      (frame.output.children as Record<string, unknown>[]).push(output);
      push(child, output);
    }
    if (outputScope !== undefined && parent !== undefined) {
      const owner = outputScope.retainAllocations(result);
      try {
        return parent.adopt(owner).value;
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return result;
  } finally {
    scratch?.dispose();
    outputScope?.dispose();
  }
}
