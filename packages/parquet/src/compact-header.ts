import type { DataReader, ThriftObject, ThriftType } from "hyparquet/src/types.js";
import { LakeqlError } from "lakeql-core";

/** More probe bytes may resolve this error; other parse errors are permanent. */
export class IncompleteCompactHeaderError extends LakeqlError {
  constructor() {
    super("LAKEQL_PARQUET_READ_ERROR", "Truncated compact-protocol header");
  }
}

export interface CompactProjection {
  readonly [field: number]:
    | true
    | "i32"
    | "i64"
    | "bool"
    | "binary"
    | CompactProjection
    | CompactStructListProjection;
}

export interface CompactStructListProjection {
  readonly list: boolean | CompactProjection | ((index: number) => CompactProjection | false);
  /** Consume projected entries without retaining a list-sized result array. */
  readonly visit?: (value: ThriftType, index: number) => void;
  /** Validated entry offsets relative to reader.view; emits only complete structs. */
  readonly visitRange?: (start: number, end: number, index: number) => void;
}

export const PAGE_HEADER_PROJECTION: CompactProjection = {
  1: "i32",
  2: "i32",
  3: "i32",
  4: "i32",
  5: { 1: "i32", 2: "i32", 3: "i32", 4: "i32" },
  7: { 1: "i32", 2: "i32", 3: "bool" },
  8: { 1: "i32", 2: "i32", 3: "i32", 4: "i32", 5: "i32", 6: "i32", 7: "bool" },
};

/** FileMetaData.row_groups -> columns -> meta_data -> statistics.
 * Binary bounds borrow footer backing; unrelated metadata fields are skipped.
 */
export const FOOTER_STATISTICS_PROJECTION: CompactProjection = {
  4: {
    list: {
      1: {
        list: {
          3: {
            12: {
              1: "binary",
              2: "binary",
              5: "binary",
              6: "binary",
            },
          },
        },
      },
    },
  },
};

/** Structural preflight counters; these are format counts, not JS heap bytes. */
export interface CompactStructureCounts {
  structs: number;
  fields: number;
  lists: number;
  listItems: number;
  binaryValues: number;
  binaryBytes: number;
  maxDepth: number;
}

/** Validate and count the input while retaining no decoded metadata tree. */
export function inspectCompactStructure(reader: DataReader): CompactStructureCounts {
  const counts: CompactStructureCounts = {
    structs: 0,
    fields: 0,
    lists: 0,
    listItems: 0,
    binaryValues: 0,
    binaryBytes: 0,
    maxDepth: 0,
  };
  readCompactHeader(reader, {}, counts);
  return counts;
}

/** Allocation events describe parser-owned structures, not engine-specific heap bytes.
 * Throwing from admission prevents the corresponding construction. Borrowed binary
 * payload bytes are reported separately from the view object and must not be
 * charged as a second copy. Conversion callbacks require their own admission.
 */
export type CompactAllocation =
  | { kind: "struct" }
  | { kind: "field" }
  | { kind: "list"; length: number }
  | { kind: "binary-view"; borrowedBytes: number };

/** Count selected raw allocations without constructing the decoded tree or invoking
 * conversion visitors. Projection factories still select each list entry.
 * Counts are structural units; callers choose and validate a heap cost model.
 */
export function inspectCompactAllocations(reader: DataReader, projection?: CompactProjection) {
  const result = {
    structs: 0,
    fields: 0,
    lists: 0,
    listSlots: 0,
    binaryViews: 0,
    borrowedBytes: 0,
  };
  parseCompactHeader(
    reader,
    projection,
    undefined,
    (event) => {
      switch (event.kind) {
        case "struct":
          result.structs++;
          break;
        case "field":
          result.fields++;
          break;
        case "list":
          result.lists++;
          result.listSlots += event.length;
          break;
        case "binary-view":
          result.binaryViews++;
          result.borrowedBytes += event.borrowedBytes;
          break;
      }
    },
    false,
  );
  return result;
}

/** Compact-protocol header parsing with input-bounded allocations.
 * 128 nesting levels bound recursive parser stack use; ordinary Parquet page
 * headers need only a few. This is not whole-heap accounting for metadata.
 */
export function readCompactHeader(
  reader: DataReader,
  projection?: CompactProjection,
  counts?: CompactStructureCounts,
  admit?: (allocation: CompactAllocation) => void,
): ThriftObject {
  return parseCompactHeader(reader, projection, counts, admit, true);
}

function parseCompactHeader(
  reader: DataReader,
  projection: CompactProjection | undefined,
  counts: CompactStructureCounts | undefined,
  admit: ((allocation: CompactAllocation) => void) | undefined,
  materialize: boolean,
): ThriftObject {
  const fail = (message: string): never => {
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", message);
  };
  const requireBytes = (length: number) => {
    if (
      !Number.isSafeInteger(length) ||
      length < 0 ||
      length > reader.view.byteLength - reader.offset
    )
      throw new IncompleteCompactHeaderError();
  };
  // DataView performs the single-byte bounds check itself.
  const byte = () => {
    try {
      return reader.view.getUint8(reader.offset++);
    } catch (cause) {
      if (cause instanceof RangeError) throw new IncompleteCompactHeaderError();
      throw cause;
    }
  };
  const unsigned = () => {
    let result = 0;
    for (let index = 0; index < 5; index++) {
      const next = byte();
      if (index === 4 && next > 15) fail("Compact integer exceeds 32 bits");
      result |= (next & 127) << (7 * index);
      if ((next & 128) === 0) return result >>> 0;
    }
    return fail("Invalid compact integer");
  };
  const signed = () => {
    const value = unsigned();
    return (value >>> 1) ^ -(value & 1);
  };
  const struct = (
    depth: number,
    selection: boolean | CompactProjection,
  ): ThriftObject | undefined => {
    if (depth > 128) fail("Compact header nesting exceeds parser limit");
    if (counts !== undefined) {
      counts.structs++;
      counts.maxDepth = Math.max(counts.maxDepth, depth);
    }
    if (selection !== false) admit?.({ kind: "struct" });
    const output: ThriftObject | undefined = selection === false || !materialize ? undefined : {};
    let field = 0;
    for (;;) {
      const tag = byte();
      const type = tag & 15;
      if (type === 0) return output;
      if (counts !== undefined) counts.fields++;
      field = tag >>> 4 ? field + (tag >>> 4) : signed();
      const selected = typeof selection === "boolean" ? selection : (selection[field] ?? false);
      // Validate projected field shape before parsing an attacker-controlled
      // container length or constructing a borrowed binary view.
      if (
        (selected === "i32" && type !== 5) ||
        (selected === "i64" && type !== 6) ||
        (selected === "bool" && type !== 1 && type !== 2) ||
        (selected === "binary" && type !== 8) ||
        (typeof selected === "object" && type !== ("list" in selected ? 9 : 12))
      )
        fail(`Unexpected compact type for field ${field}`);
      if (selection !== false && selected !== false) admit?.({ kind: "field" });
      const value = element(type, depth, typeof selected === "string" ? true : selected);
      if (output !== undefined && selected !== false) output[`field_${field}`] = value;
    }
  };
  const element = (
    type: number,
    depth: number,
    selection: boolean | CompactProjection | CompactStructListProjection,
  ): ThriftType => {
    switch (type) {
      case 1:
        return true;
      case 2:
        return false;
      case 3: {
        const value = byte();
        return value < 128 ? value : value - 256;
      }
      case 4:
      case 5:
        return signed();
      case 6: {
        let small = 0;
        let wide = 0n;
        const decode = materialize && selection !== false;
        for (let index = 0; index < 10; index++) {
          const next = byte();
          if (index === 9 && next > 1) fail("Compact integer exceeds 64 bits");
          if (decode) {
            // Seven base-128 digits use only 49 bits, so accumulation and
            // signed zigzag conversion remain exact in Number arithmetic.
            if (index < 7) small += (next & 127) * 2 ** (index * 7);
            else {
              if (index === 7) wide = BigInt(small);
              wide |= BigInt(next & 127) << BigInt(index * 7);
            }
          }
          if ((next & 128) === 0) {
            if (!decode) return 0n;
            if (index < 7) return BigInt(small % 2 === 0 ? small / 2 : -(small + 1) / 2);
            return (wide >> 1n) ^ -(wide & 1n);
          }
        }
        return fail("Invalid compact integer");
      }
      case 7: {
        requireBytes(8);
        const value = reader.view.getFloat64(reader.offset, true);
        reader.offset += 8;
        return value;
      }
      case 8: {
        const length = unsigned();
        requireBytes(length);
        if (counts !== undefined) {
          counts.binaryValues++;
          counts.binaryBytes += length;
        }
        if (selection !== false) admit?.({ kind: "binary-view", borrowedBytes: length });
        const value =
          selection === false || !materialize
            ? 0
            : new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length);
        reader.offset += length;
        return value;
      }
      case 9: {
        if (depth >= 128) fail("Compact header nesting exceeds parser limit");
        const tag = byte();
        const itemType = tag & 15;
        const listProjection =
          typeof selection === "object" && "list" in selection ? selection.list : undefined;
        if (listProjection !== undefined && itemType !== 12)
          fail("Unexpected compact list element type");
        const length = tag >>> 4 === 15 ? unsigned() : tag >>> 4;
        // Every supported list element consumes at least one byte, including
        // boolean values and STOP-terminated empty structs. Check before Array.
        requireBytes(length);
        if (counts !== undefined) {
          counts.lists++;
          counts.listItems += length;
          counts.maxDepth = Math.max(counts.maxDepth, depth + 1);
        }
        const visit =
          typeof selection === "object" && "list" in selection ? selection.visit : undefined;
        const visitRange =
          typeof selection === "object" && "list" in selection ? selection.visitRange : undefined;
        if (selection !== false && visit === undefined && visitRange === undefined)
          admit?.({ kind: "list", length });
        const values: ThriftType[] | undefined =
          !materialize || selection === false || visit !== undefined || visitRange !== undefined
            ? undefined
            : new Array(length);
        for (let index = 0; index < length; index++) {
          const start = reader.offset;
          const value =
            itemType === 1 || itemType === 2
              ? byte() === 1
              : element(
                  itemType,
                  depth + 1,
                  typeof listProjection === "function"
                    ? listProjection(index)
                    : (listProjection ?? selection),
                );
          if (values !== undefined) values[index] = value;
          if (materialize) {
            visit?.(value, index);
            visitRange?.(start, reader.offset, index);
          }
        }
        return values ?? 0;
      }
      case 12:
        if (typeof selection === "object" && "list" in selection)
          return fail("Unexpected compact struct projection");
        return struct(depth + 1, selection) ?? 0;
      default:
        return fail(`Unsupported compact header type ${type}`);
    }
  };
  return struct(0, projection ?? true) ?? {};
}
