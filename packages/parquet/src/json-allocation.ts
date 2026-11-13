import { LakeqlError, MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";

/** Conservative parsed-tree model. Scans encoded text without building tokens,
 * arrays, or a recursion stack. JSON.parse remains responsible for syntax checks.
 * Input text and VM parser workspace are separate from this tree model.
 */
export function jsonAllocationBytes(text: string): number {
  const c = MATERIALIZATION_CHARGES;
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      const start = ++i;
      for (; i < text.length; i++) {
        if (text[i] === "\\") i++;
        else if (text[i] === '"') break;
      }
      bytes += c.string + 2 * (i - start) + c.arraySlot;
    } else if (ch === "{") bytes += c.object + c.arraySlot;
    else if (ch === "[") bytes += c.array + c.arraySlot;
    else if (ch === ":") bytes += c.property;
    else if (
      ch !== "}" &&
      ch !== "]" &&
      ch !== "," &&
      ch !== " " &&
      ch !== "\n" &&
      ch !== "\r" &&
      ch !== "\t"
    ) {
      bytes += c.number + c.arraySlot;
      while (i + 1 < text.length) {
        const next = text.charCodeAt(i + 1);
        if (
          next === 32 ||
          next === 9 ||
          next === 10 ||
          next === 13 ||
          next === 44 ||
          next === 93 ||
          next === 125 ||
          next === 58
        )
          break;
        i++;
      }
    }
  }
  if (!Number.isSafeInteger(bytes))
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "JSON allocation extent exceeds safe capacity",
    );
  return bytes;
}

export function parseAdmittedJson(text: string, scope: MemoryScope): unknown {
  return scope.allocate(jsonAllocationBytes(text), "parsed metadata JSON", () => JSON.parse(text))
    .value;
}

/** Dynamic JSON object keys are map entries, not fixed-schema struct fields.
 * The parsed tree reservation covers parser-created records; additional map
 * directories are charged before replacing those records in the reviver.
 */
export function parseJsonMapObjects(text: string, scope?: MemoryScope): unknown {
  const parse = () =>
    JSON.parse(text, (_key, value: unknown) => {
      if (value === null || typeof value !== "object" || Array.isArray(value)) return value;
      return jsonRecordMap(value as Record<string, unknown>, scope, false);
    });
  return scope === undefined
    ? parse()
    : scope.allocate(jsonAllocationBytes(text), "parsed JSON tree", parse).value;
}

/** Adapt an already parsed JSON tree before generic vector shape inference.
 * Caller owns the source tree and the conversion scope lifetime.
 */
export function jsonMapObjects(value: unknown, scope?: MemoryScope): unknown {
  if (value === null || typeof value !== "object" || value instanceof Map) return value;
  if (Array.isArray(value)) {
    const build = () => value.map((item) => jsonMapObjects(item, scope));
    return scope === undefined
      ? build()
      : scope.allocate(
          MATERIALIZATION_CHARGES.array + value.length * MATERIALIZATION_CHARGES.arraySlot,
          "JSON array normalization",
          build,
        ).value;
  }
  return jsonRecordMap(value as Record<string, unknown>, scope, true);
}

function jsonRecordMap(
  record: Record<string, unknown>,
  scope: MemoryScope | undefined,
  recursive: boolean,
) {
  let count = 0;
  for (const key in record) if (Object.hasOwn(record, key)) count++;
  const build = () => {
    const map = new Map<string, unknown>();
    for (const key in record)
      if (Object.hasOwn(record, key))
        map.set(key, recursive ? jsonMapObjects(record[key], scope) : record[key]);
    return map;
  };
  return scope === undefined
    ? build()
    : scope.allocate(
        MATERIALIZATION_CHARGES.object + count * MATERIALIZATION_CHARGES.property,
        "JSON object normalization",
        build,
      ).value;
}
