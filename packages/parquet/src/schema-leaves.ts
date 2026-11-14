import type { SchemaElement } from "hyparquet";
import { LakeqlError } from "lakeql-core";

/** Index physical leaves once, preserving path components (names may contain dots). */
export function schemaLeafIndex(schema: readonly SchemaElement[]): Map<string, SchemaElement> {
  const fail = (): never => {
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid Parquet schema tree");
  };
  const count = (element: SchemaElement): number => {
    const value = Number(element.num_children ?? 0);
    if (!Number.isSafeInteger(value) || value < 0) return fail();
    return value;
  };
  const root = schema[0];
  if (root === undefined) return fail();
  const stack = [{ remaining: count(root) }];
  const path: string[] = [];
  const result = new Map<string, SchemaElement>();
  for (let index = 1; index < schema.length; index++) {
    while (stack.length > 0 && stack[stack.length - 1]?.remaining === 0) {
      stack.pop();
      path.pop();
    }
    const parent = stack[stack.length - 1];
    const element = schema[index];
    if (parent === undefined || element === undefined) return fail();
    parent.remaining--;
    const children = count(element);
    path.push(element.name);
    if (children > 0) {
      if (children > schema.length - index - 1) return fail();
      stack.push({ remaining: children });
    } else {
      const key = JSON.stringify(path);
      if (result.has(key)) return fail();
      result.set(key, element);
      path.pop();
    }
  }
  if (stack.some((entry) => entry.remaining !== 0)) return fail();
  return result;
}
