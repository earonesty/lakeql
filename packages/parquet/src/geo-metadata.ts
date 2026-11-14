import type { SchemaElement } from "hyparquet";
import { LakeqlError, MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";
import { parseAdmittedJson } from "./json-allocation.js";

type KeyValue = { key: string; value?: string | undefined };
const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" ? (value as Record<string, unknown>) : undefined;

/** Parse under temporary ownership, retaining only annotations on eligible leaves. */
export function markGeoMetadata(
  schema: SchemaElement[],
  entries?: readonly KeyValue[],
  scope?: MemoryScope,
): void {
  const text = entries?.find((entry) => entry.key === "geo")?.value;
  if (!text) return;
  const scratch = scope?.childScope();
  const allocate = <T>(bytes: number, create: () => T): T =>
    scope === undefined ? create() : scope.allocate(bytes, "GeoParquet annotation", create).value;
  try {
    const decoded = scratch === undefined ? JSON.parse(text) : parseAdmittedJson(text, scratch);
    const columns = record(record(decoded)?.columns);
    if (columns === undefined) return;
    const c = MATERIALIZATION_CHARGES;
    for (let i = 1; i < schema.length; i++) {
      const element = schema[i];
      if (element === undefined) continue;
      // Skip the entire flattened subtree, not just its immediate children.
      let pending = element.num_children ?? 0;
      if (pending > 0) {
        while (pending > 0 && ++i < schema.length) pending += (schema[i]?.num_children ?? 0) - 1;
        continue;
      }
      if (
        element.type !== "BYTE_ARRAY" ||
        element.logical_type ||
        !Object.hasOwn(columns, element.name)
      )
        continue;
      const column = record(columns[element.name]);
      if (column?.encoding !== "WKB") continue;
      const crs = record(column.crs);
      const id = record(crs?.id ?? (Array.isArray(crs?.ids) ? crs.ids[0] : undefined));
      let reference: string | undefined;
      if (id !== undefined) {
        const authority = id.authority,
          code = id.code;
        if (
          typeof authority !== "string" ||
          (typeof code !== "string" && typeof code !== "number") ||
          (typeof code === "number" && !Number.isFinite(code))
        )
          throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid GeoParquet CRS identifier");
        const codeUnits = typeof code === "string" ? code.length : 24;
        reference = allocate(
          c.string + (authority.length + 1 + codeUnits) * c.stringCodeUnit,
          () => `${authority}:${code}`,
        );
      }
      element.logical_type = allocate(c.object + 2 * c.property, () => ({
        type: column.edges === "spherical" ? ("GEOGRAPHY" as const) : ("GEOMETRY" as const),
        ...(reference === undefined ? {} : { crs: reference }),
      }));
    }
  } finally {
    scratch?.dispose();
  }
}
