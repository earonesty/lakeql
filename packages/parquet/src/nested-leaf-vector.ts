import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type Vector,
  vectorFromSource,
  vectorLength,
} from "lakeql-core";
import { convertDecodedValues } from "./logical-values.js";
import { nestedPackedCapability } from "./nested-packed-capability.js";
import type { readNestedPage } from "./nested-page.js";

type PhysicalPage = NonNullable<ReturnType<typeof readNestedPage>>;

/** Compact present values only; repetition/definition levels remain on the physical page.
 * Borrowed physical and dictionary backing must outlive this vector. Unsupported packed
 * logical conversions decline before allocation so the caller can select another decoder.
 */
export function nestedLeafVector(
  page: PhysicalPage,
  decoder: ColumnDecoder,
  scope: MemoryScope,
  dictionary?: Vector,
): Vector | undefined {
  const packed = page.packedBinary;
  const capability = nestedPackedCapability(decoder.element);
  const text = capability === "text";
  const packedLogical = capability === "converted";
  if (packed && capability === undefined) return undefined;
  const invalid = () => new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid compact nested leaf");
  const count = page.levels.presentCount;
  if ((packed?.count ?? page.values?.length) !== count) throw invalid();
  const construction = scope.childScope();
  try {
    const output = construction.allocate(
      c.object + 5 * c.property + c.view,
      "nested leaf vector",
      (): Vector => {
        if (page.dictionaryEncoded) {
          if (dictionary === undefined || !(page.values instanceof Uint32Array)) throw invalid();
          const length = vectorLength(dictionary);
          for (const index of page.values) if (index >= length) throw invalid();
          return { type: "dict", indices: page.values, dictionary };
        }
        if (packed && !packedLogical)
          return {
            type: text ? "utf8-view" : "binary-view",
            data: packed.payload,
            length: count,
            ...(packed.ranges === undefined ? {} : { ranges: packed.ranges }),
            ...(packed.fixedLength === undefined ? {} : { fixedLength: packed.fixedLength }),
          };
        const values = (() => {
          if (!packed) {
            if (page.values === undefined) throw invalid();
            return convertDecodedValues(page.values, decoder, construction);
          }
          const scratch = construction.childScope();
          try {
            const physical = scratch.allocate(
              c.array + count * (c.arraySlot + c.view),
              "packed logical byte views",
              () => Array.from({ length: count }, (_, index) => packed.value(index)),
            ).value;
            return convertDecodedValues(physical, decoder, construction);
          } finally {
            scratch.dispose();
          }
        })();
        if (values instanceof Float64Array) return { type: "f64", values };
        if (values instanceof Float32Array) return { type: "f32", values };
        if (values instanceof Int32Array) return { type: "i32", values };
        if (values instanceof Uint32Array) return { type: "u32", values };
        if (values instanceof BigInt64Array) return { type: "i64", values };
        if (values instanceof BigUint64Array) return { type: "u64", values };
        if (values instanceof Uint8Array)
          return { type: decoder.type === "BOOLEAN" ? "bool" : "u8", values };
        return vectorFromSource({ length: count, get: (index) => values[index] }, construction);
      },
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return output;
  } finally {
    construction.dispose();
  }
}
