import type { ThriftObject, ThriftType } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c } from "lakeql-core";

/** Raw parser values retained by conversion. Input is a validated compact tree:
 * bounded depth, no cycles, binary payloads borrowed from separately owned footer.
 * Duplicate aliases are conservatively charged more than once.
 */
export function rawMetadataValueBytes(value: ThriftType | undefined): number {
  if (value === undefined) return 0;
  if (value instanceof Uint8Array) return c.view;
  if (Array.isArray(value)) {
    let bytes = c.array + value.length * c.arraySlot;
    for (const entry of value) bytes += rawMetadataValueBytes(entry);
    return bytes;
  }
  if (typeof value === "object" && value !== null) {
    let bytes = c.object;
    for (const key in value)
      if (Object.hasOwn(value, key))
        bytes +=
          c.property +
          c.string +
          key.length * c.stringCodeUnit +
          rawMetadataValueBytes(value[key as keyof ThriftObject]);
    return bytes;
  }
  return c.bigint;
}

export function columnRawAliasBytes(chunk: ThriftObject): number {
  let bytes = 0;
  // All raw chunk fields except decoded path and converted metadata.
  for (const key in chunk)
    if (Object.hasOwn(chunk, key) && key !== "field_1" && key !== "field_3")
      bytes += rawMetadataValueBytes(chunk[key as keyof ThriftObject]);
  const data = chunk.field_3 as ThriftObject | undefined;
  if (data)
    for (const key in data) {
      if (!Object.hasOwn(data, key)) continue;
      // Enums and decoded path/key-value strings have independent output admission.
      if (
        key === "field_1" ||
        key === "field_2" ||
        key === "field_3" ||
        key === "field_4" ||
        key === "field_8"
      )
        continue;
      // Keep whole statistics/size/geo/encoding-stat trees conservatively, since
      // conversion forwards selected values and may retain opaque legacy shapes.
      bytes += rawMetadataValueBytes(data[key as keyof ThriftObject]);
    }
  return bytes;
}

export function rowGroupRawAliasBytes(raw: ThriftObject): number {
  let bytes = 0;
  for (const key in raw)
    if (Object.hasOwn(raw, key) && key !== "field_1")
      bytes += rawMetadataValueBytes(raw[key as keyof ThriftObject]);
  return bytes;
}
