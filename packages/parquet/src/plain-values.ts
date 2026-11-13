import { readPlain } from "hyparquet/src/plain.js";
import type { DataReader, DecodedArray, ParquetType } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import { readPackedBinaryValues } from "./binary-values.js";

/** Admit numeric alignment copies, INT96 carriers, and binary view arrays.
 * Aligned numeric views retain input backing; other representations use upstream.
 */
export function readPlainValues(
  reader: DataReader,
  type: ParquetType,
  count: number,
  fixedLength: number | undefined,
  scope: MemoryScope | undefined,
): DecodedArray {
  if (scope !== undefined && type === "FIXED_LEN_BYTE_ARRAY" && fixedLength === undefined)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing PLAIN fixed binary width");
  if (scope !== undefined && (type === "BYTE_ARRAY" || type === "FIXED_LEN_BYTE_ARRAY"))
    return binaryValues(
      reader,
      count,
      type === "FIXED_LEN_BYTE_ARRAY" ? fixedLength : undefined,
      scope,
    );
  if (type === "INT96") return int96Values(reader, count, scope);
  if (scope === undefined || count === 0) return readPlain(reader, type, count, fixedLength);
  switch (type) {
    case "INT32":
      return numericValues(Int32Array, reader, count, scope);
    case "INT64":
      return numericValues(BigInt64Array, reader, count, scope);
    case "FLOAT":
      return numericValues(Float32Array, reader, count, scope);
    case "DOUBLE":
      return numericValues(Float64Array, reader, count, scope);
    default:
      return readPlain(reader, type, count, fixedLength);
  }
}

function numericValues<T extends Int32Array | BigInt64Array | Float32Array | Float64Array>(
  Constructor: {
    new (buffer: ArrayBufferLike, byteOffset: number, length: number): T;
    readonly BYTES_PER_ELEMENT: number;
  },
  reader: DataReader,
  count: number,
  scope: MemoryScope,
): T {
  const size = count * Constructor.BYTES_PER_ELEMENT;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    !Number.isSafeInteger(size) ||
    !Number.isSafeInteger(reader.offset) ||
    reader.offset < 0 ||
    size > reader.view.byteLength - reader.offset
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid PLAIN numeric value range");
  }
  const offset = reader.view.byteOffset + reader.offset;
  let result: T;
  if (offset % Constructor.BYTES_PER_ELEMENT === 0) {
    result = new Constructor(reader.view.buffer, offset, count);
  } else {
    result = scope.allocate(size, "Parquet PLAIN alignment", () => {
      const buffer = new ArrayBuffer(size);
      new Uint8Array(buffer).set(new Uint8Array(reader.view.buffer, offset, size));
      return new Constructor(buffer, 0, count);
    }).value;
  }
  reader.offset += size;
  return result;
}

/** Transpose numeric byte lanes into one admitted backing allocation. */
export function readByteStreamSplitValues(
  reader: DataReader,
  type: ParquetType,
  count: number,
  scope: MemoryScope,
): DecodedArray {
  const width =
    type === "INT32" || type === "FLOAT" ? 4 : type === "INT64" || type === "DOUBLE" ? 8 : 0;
  const size = count * width;
  if (
    width === 0 ||
    !Number.isSafeInteger(count) ||
    count < 0 ||
    !Number.isSafeInteger(size) ||
    !Number.isSafeInteger(reader.offset) ||
    reader.offset < 0 ||
    size > reader.view.byteLength - reader.offset
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid numeric byte-stream-split range");
  }
  const output = scope.allocate(size, "Parquet byte-stream-split values", () => {
    const bytes = new Uint8Array(size);
    for (let lane = 0; lane < width; lane += 1) {
      for (let index = 0; index < count; index += 1) {
        bytes[index * width + lane] = reader.view.getUint8(reader.offset + lane * count + index);
      }
    }
    // Track the returned numeric value as the exact owner of this allocation.
    // The temporary byte view adds no backing storage and does not escape.
    switch (type) {
      case "INT32":
        return new Int32Array(bytes.buffer);
      case "INT64":
        return new BigInt64Array(bytes.buffer);
      case "FLOAT":
        return new Float32Array(bytes.buffer);
      default:
        return new Float64Array(bytes.buffer);
    }
  }).value;
  reader.offset += size;
  return output;
}

function int96Values(reader: DataReader, count: number, scope: MemoryScope | undefined): bigint[] {
  const size = count * 12;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    !Number.isSafeInteger(size) ||
    !Number.isSafeInteger(reader.offset) ||
    reader.offset < 0 ||
    size > reader.view.byteLength - reader.offset
  )
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid PLAIN INT96 extent");
  const decode = () => {
    const values = new Array<bigint>(count);
    for (let i = 0; i < count; i++) {
      const offset = reader.offset + i * 12;
      const low = reader.view.getBigUint64(offset, true);
      const high = BigInt(reader.view.getInt32(offset + 8, true));
      values[i] = (high << 64n) | low;
    }
    return values;
  };
  const retained = c.array + count * (c.arraySlot + c.bigint);
  const stage = scope?.childStage(
    retained + (count === 0 ? 0 : 3 * c.bigint),
    "PLAIN INT96 conversion",
  );
  try {
    const values =
      stage === undefined || scope === undefined
        ? decode()
        : scope.adopt(stage.allocate(retained, "PLAIN INT96 carriers", decode).retain()).value;
    reader.offset += size;
    return values;
  } finally {
    stage?.dispose();
  }
}

function binaryValues(
  reader: DataReader,
  count: number,
  fixedLength: number | undefined,
  scope: MemoryScope,
): Uint8Array[] {
  const scratch = scope.childScope();
  const input = { view: reader.view, offset: reader.offset };
  try {
    const packed = readPackedBinaryValues(input, count, fixedLength, scratch);
    const output = scope.allocate(
      c.array + count * (c.arraySlot + c.view),
      "PLAIN binary value views",
      () => {
        const values = new Array<Uint8Array>(count);
        for (let index = 0; index < count; index++) values[index] = packed.value(index);
        return values;
      },
    ).value;
    reader.offset = input.offset;
    return output;
  } finally {
    scratch.dispose();
  }
}
