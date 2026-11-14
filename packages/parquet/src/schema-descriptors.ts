import { MATERIALIZATION_CHARGES as c, type MemoryScope } from "lakeql-core";
import { parquetColumnShape } from "./schema-shape.js";
import { stableSchemaSignature } from "./schema-signature.js";
import { schemaShapeTree } from "./schema-tree.js";
import type { ParquetMetadata } from "./types.js";

export interface ParquetColumnDescriptor {
  path: string;
  signature: string;
  physicalType?: string;
}

export function parquetColumnDescriptors(
  metadata: Pick<ParquetMetadata, "schema">,
  path: string,
  parent?: MemoryScope,
): Map<string, ParquetColumnDescriptor> {
  const output = parent?.childScope();
  const scratch = output?.childScope();
  try {
    const create = () => new Map<string, ParquetColumnDescriptor>();
    const columns =
      output === undefined
        ? create()
        : output.allocate(c.object, "schema descriptor directory", create).value;
    output?.allocate(
      c.string + path.length * c.stringCodeUnit,
      "schema descriptor path",
      () => undefined,
    );
    for (const child of schemaShapeTree(metadata.schema, scratch).children) {
      const element = child.element as unknown as Record<string, unknown>;
      const name = String(element.name ?? "");
      if (name.length === 0) continue;
      const temporary = scratch?.childScope();
      try {
        const shape = parquetColumnShape(child, temporary);
        const signature = stableSchemaSignature(shape, output);
        output?.allocate(
          c.property + c.string + name.length * c.stringCodeUnit,
          "schema descriptor name",
          () => undefined,
        );
        const build = (): ParquetColumnDescriptor => {
          const result: ParquetColumnDescriptor = { path, signature };
          if (typeof element.type === "string") result.physicalType = element.type;
          return result;
        };
        columns.set(
          name,
          output === undefined
            ? build()
            : output.allocate(c.object + 3 * c.property, "schema descriptor", build).value,
        );
      } finally {
        temporary?.dispose();
      }
    }
    if (output !== undefined && parent !== undefined) {
      const owner = output.retainAllocations(columns);
      try {
        return parent.adopt(owner).value;
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return columns;
  } finally {
    scratch?.dispose();
    output?.dispose();
  }
}
