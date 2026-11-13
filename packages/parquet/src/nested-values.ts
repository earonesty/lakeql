import { isListLike } from "hyparquet/src/schema.js";
import type { SchemaTree } from "hyparquet/src/types.js";

type Normalize = (value: unknown) => unknown;

/** Preserve schema-declared maps before generic vector inference sees objects. */
export function nestedValueNormalizer(
  schema: SchemaTree,
  repeatedHandled = false,
): Normalize | undefined {
  const element = schema.element;
  const logical = logicalTypeName(element.logical_type);
  // A decoded variant no longer has the shape of its physical shredded fields.
  if (logical === "VARIANT") return undefined;
  let normalize: Normalize | undefined;
  if (
    element.converted_type === "MAP" ||
    element.converted_type === "MAP_KEY_VALUE" ||
    logical === "MAP"
  ) {
    const fields = schema.children.some((child) => child.element.name === "key")
      ? schema.children
      : (schema.children[0]?.children ?? []);
    const key = fields.find((child) => child.element.name === "key");
    const item = fields.find((child) => child.element.name === "value");
    const normalizeKey = key === undefined ? undefined : nestedValueNormalizer(key);
    const normalizeItem = item === undefined ? undefined : nestedValueNormalizer(item);
    normalize = (value) => {
      if (value === null || value === undefined || typeof value !== "object") return value;
      if (value instanceof Map && normalizeKey === undefined && normalizeItem === undefined)
        return value;
      const entries =
        value instanceof Map
          ? value
          : Array.isArray(value)
            ? (value as [unknown, unknown][])
            : Object.entries(value);
      if (normalizeKey === undefined && normalizeItem === undefined) return new Map(entries);
      return new Map(
        (function* () {
          for (const [key, item] of entries)
            yield [
              normalizeKey === undefined ? key : normalizeKey(key),
              normalizeItem === undefined ? item : normalizeItem(item),
            ] as const;
        })(),
      );
    };
  } else if (isListLike(schema)) {
    let child = schema.children[0];
    if (child?.children.length === 1) child = child.children[0];
    const convert = child === undefined ? undefined : nestedValueNormalizer(child, true);
    if (convert !== undefined)
      normalize = (value) => (Array.isArray(value) ? value.map(convert) : value);
  } else {
    const fields = schema.children.flatMap((child) => {
      const convert = nestedValueNormalizer(child);
      return convert === undefined ? [] : [{ name: child.element.name, convert }];
    });
    if (fields.length > 0)
      normalize = (value) => {
        if (value === null || typeof value !== "object" || Array.isArray(value)) return value;
        const record = value as Record<string, unknown>;
        let output: Record<string, unknown> | undefined;
        for (const { name, convert } of fields) {
          if (!Object.hasOwn(record, name)) continue;
          const converted = convert(record[name]);
          if (converted !== record[name]) {
            output ??= { ...record };
            output[name] = converted;
          }
        }
        return output ?? value;
      };
  }
  if (normalize !== undefined && element.repetition_type === "REPEATED" && !repeatedHandled) {
    const convert = normalize;
    return (value) => (Array.isArray(value) ? value.map(convert) : value);
  }
  return normalize;
}

function logicalTypeName(type: unknown): string | undefined {
  if (typeof type === "string") return type;
  if (typeof type !== "object" || type === null) return undefined;
  if ("type" in type && typeof type.type === "string") return type.type;
  for (const key of ["LIST", "MAP", "VARIANT"]) if (key in type) return key;
  return undefined;
}
