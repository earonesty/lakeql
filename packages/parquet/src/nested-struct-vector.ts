import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type Vector,
  vectorLength,
} from "lakeql-core";

export interface NestedStructField {
  values: Vector;
  /** A scalar subtree may omit null parents; a repeated subtree may already include them. */
  alignment: "present-parents" | "all-parents";
}

/** Assemble aligned struct fields, borrowing payload/validity and sharing one null-parent
 * routing array across all compact fields. Cross-column row alignment is caller-owned.
 */
export function nestedStructVector(
  valid: Uint8Array,
  fields: Readonly<Record<string, NestedStructField>>,
  scope: MemoryScope,
): Vector {
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Unaligned nested struct fields");
  let present = 0;
  for (const flag of valid) {
    if (flag !== 0 && flag !== 1) throw invalid();
    present += flag;
  }
  let fieldCount = 0;
  let needsRouting = false;
  for (const key in fields)
    if (Object.hasOwn(fields, key)) {
      const field = fields[key];
      if (
        field === undefined ||
        (field.alignment !== "present-parents" && field.alignment !== "all-parents") ||
        vectorLength(field.values) !==
          (field.alignment === "present-parents" ? present : valid.length)
      )
        throw invalid();
      fieldCount++;
      if (field.alignment === "present-parents" && present !== valid.length) needsRouting = true;
    }
  const construction = scope.childScope();
  try {
    const indices = needsRouting
      ? construction.allocate(valid.length * 4 + c.view, "nested struct parent routing", () => {
          const indices = new Uint32Array(valid.length);
          let value = 0;
          for (let row = 0; row < valid.length; row++) if (valid[row]) indices[row] = value++;
          return indices;
        }).value
      : undefined;
    const output = construction.allocate(
      c.object + fieldCount * c.property,
      "nested struct field directory",
      (): Record<string, Vector> => Object.create(null),
    ).value;
    for (const key in fields)
      if (Object.hasOwn(fields, key)) {
        const field = fields[key];
        if (field === undefined) throw invalid();
        output[key] =
          indices !== undefined && field.alignment === "present-parents"
            ? construction.allocate(
                c.object + 4 * c.property,
                "nested struct field routing",
                (): Vector => ({
                  type: "dict",
                  indices,
                  valid,
                  dictionary: field.values,
                }),
              ).value
            : field.values;
      }
    const result = construction.allocate(
      c.object + 4 * c.property,
      "nested struct vector",
      (): Vector => ({
        type: "struct",
        fields: output,
        length: valid.length,
        valid,
      }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}
