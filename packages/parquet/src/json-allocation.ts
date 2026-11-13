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
