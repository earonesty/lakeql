import { decompressPage } from "hyparquet/src/datapage.js";
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from "hyparquet/src/schema.js";
import type { ColumnDecoder, DecodedArray, PageHeader } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type Vector,
} from "lakeql-core";
import { readAlpValues } from "./alp-values.js";
import { readPackedBinaryValues } from "./binary-values.js";
import { scopedParquetCompressors, ZSTD_REUSABLE_WORKSPACE_BYTES } from "./compressors.js";
import { readPackedDeltaBinaryValues } from "./delta-binary-values.js";
import { readDeltaValues } from "./delta-values.js";
import { readDictionaryIndices } from "./dictionary-indices.js";
import { GZIP_WORKSPACE_BYTES } from "./gzip.js";
import { visitLevelRuns } from "./level-runs.js";
import { readByteStreamSplitValues, readPlainValues } from "./plain-values.js";
import { planZstdWorkspaceBytes } from "./zstd-workspace-plan.js";

export interface NumericPage {
  values: DecodedArray;
  packedVector?: Vector;
  dictionaryEncoded?: boolean;
  booleanValues?: boolean;
  definitionLevels: Uint8Array | Uint32Array | undefined;
}

/** Flat numeric and dictionary encodings with explicit typed-buffer admission. */
export function readNumericPage(
  bytes: Uint8Array,
  header: PageHeader,
  decoder: ColumnDecoder,
  scope: MemoryScope,
  dictionaryLength?: number,
): NumericPage | undefined {
  if (getMaxRepetitionLevel(decoder.schemaPath) !== 0) return undefined;
  const v1 = header.type === "DATA_PAGE" ? header.data_page_header : undefined;
  const v2 = header.type === "DATA_PAGE_V2" ? header.data_page_header_v2 : undefined;
  const encoding = (v1 ?? v2)?.encoding;
  const dictionaryEncoded = encoding === "RLE_DICTIONARY" || encoding === "PLAIN_DICTIONARY";
  const booleanEncoded = decoder.type === "BOOLEAN" && (encoding === "PLAIN" || encoding === "RLE");
  const leaf = decoder.element;
  const text = leaf.converted_type === "UTF8" || leaf.logical_type?.type === "STRING";
  const deltaBinaryEncoded =
    decoder.type === "BYTE_ARRAY" &&
    (encoding === "DELTA_BYTE_ARRAY" || encoding === "DELTA_LENGTH_BYTE_ARRAY");
  const binaryEncoded =
    ((encoding === "PLAIN" &&
      (decoder.type === "BYTE_ARRAY" || decoder.type === "FIXED_LEN_BYTE_ARRAY")) ||
      deltaBinaryEncoded) &&
    (text || (leaf.converted_type === undefined && leaf.logical_type === undefined));
  const plainBinaryEncoded =
    encoding === "PLAIN" &&
    (decoder.type === "BYTE_ARRAY" || decoder.type === "FIXED_LEN_BYTE_ARRAY");
  const int96Encoded = decoder.type === "INT96" && encoding === "PLAIN";
  if (
    !plainBinaryEncoded &&
    !int96Encoded &&
    !binaryEncoded &&
    !booleanEncoded &&
    (dictionaryEncoded
      ? dictionaryLength === undefined
      : (encoding !== "PLAIN" &&
          encoding !== "BYTE_STREAM_SPLIT" &&
          !(encoding === "ALP" && (decoder.type === "FLOAT" || decoder.type === "DOUBLE")) &&
          !(
            encoding === "DELTA_BINARY_PACKED" &&
            (decoder.type === "INT32" || decoder.type === "INT64")
          )) ||
        !["INT32", "INT64", "FLOAT", "DOUBLE"].includes(decoder.type))
  )
    return undefined;
  const layout = flatLevelLayout(bytes, header, decoder);
  if (layout === undefined) return undefined;
  const { count, maximum } = layout;
  let page = v1
    ? decompressPage(bytes, header.uncompressed_page_size, decoder.codec, decoder.compressors)
    : bytes;
  const reader = { view: new DataView(page.buffer, page.byteOffset, page.byteLength), offset: 0 };
  if (v2 !== undefined && v2.repetition_levels_byte_length > 0) {
    // Flat columns have a zero-bit repetition domain. Some writers explicitly
    // encode its runs; validate coverage without materializing a level array.
    visitLevelRuns(reader, 0, count, () => {}, v2.repetition_levels_byte_length, true);
  }
  let levels: Uint8Array | Uint32Array | undefined;
  let present = count;
  if (maximum > 0) {
    const width = 32 - Math.clz32(maximum);
    present = 0;
    visitLevelRuns(
      reader,
      width,
      count,
      (level, begin, run) => {
        if (level > maximum)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid Parquet definition level");
        const exists = level === maximum;
        if (exists) present += run;
        else if (levels === undefined) {
          levels = scope.allocate(
            count,
            "Parquet definition levels",
            () => new Uint8Array(count),
          ).value;
          levels.fill(1, 0, begin);
        }
        if (levels !== undefined) {
          if (run === 1) levels[begin] = exists ? 1 : 0;
          else levels.fill(exists ? 1 : 0, begin, begin + run);
        }
      },
      v2?.definition_levels_byte_length,
    );
  }
  if (v2) {
    if (v2.num_rows !== count || v2.num_nulls !== count - present) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Inconsistent flat numeric page counts");
    }
    reader.offset = v2.repetition_levels_byte_length + v2.definition_levels_byte_length;
    page = bytes.subarray(reader.offset);
    if (v2.is_compressed !== false)
      page = decompressPage(
        page,
        header.uncompressed_page_size - reader.offset,
        decoder.codec,
        decoder.compressors,
      );
    reader.view = new DataView(page.buffer, page.byteOffset, page.byteLength);
    reader.offset = 0;
  }
  if (binaryEncoded) {
    const fixedLength = decoder.type === "FIXED_LEN_BYTE_ARRAY" ? leaf.type_length : undefined;
    if (decoder.type === "FIXED_LEN_BYTE_ARRAY" && fixedLength === undefined) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing fixed binary width");
    }
    const packed = deltaBinaryEncoded
      ? readPackedDeltaBinaryValues(reader, present, encoding === "DELTA_BYTE_ARRAY", scope)
      : readPackedBinaryValues(reader, present, fixedLength, scope);
    releaseDecodedScratch(page, bytes, packed.payload, scope);
    const dictionary: Vector = {
      type: text ? "utf8-view" : "binary-view",
      data: packed.payload,
      ...(packed.fixedLength === undefined ? {} : { fixedLength: packed.fixedLength }),
      ...(packed.ranges === undefined ? {} : { ranges: packed.ranges }),
      length: present,
    };
    if (present === count)
      return { values: [], packedVector: dictionary, definitionLevels: undefined };
    const indices = scope.allocate(
      count * 4,
      "Parquet binary presence indices",
      () => new Uint32Array(count),
    ).value;
    // Definition levels are already normalized to 0/1. A byte-wide level
    // buffer is the final validity buffer, not additional decode workspace.
    const valid =
      levels instanceof Uint8Array
        ? levels
        : scope.allocate(count, "Parquet binary validity", () => new Uint8Array(count)).value;
    let valueIndex = 0;
    for (let index = 0; index < count; index += 1) {
      if (levels?.[index] === 1) {
        indices[index] = valueIndex++;
        valid[index] = 1;
      }
    }
    if (levels !== undefined && levels !== valid) scope.releaseAllocation(levels);
    return {
      values: [],
      packedVector: { type: "dict", dictionary, indices, valid },
      definitionLevels: undefined,
    };
  }
  if (dictionaryEncoded) {
    const values = readDictionaryIndices(reader, present, dictionaryLength ?? 0, scope);
    releaseDecodedScratch(page, bytes, values, scope);
    return {
      values,
      dictionaryEncoded: true,
      definitionLevels: present === count ? undefined : levels,
    };
  }
  if (booleanEncoded) {
    if (encoding === "PLAIN" && Math.ceil(present / 8) > reader.view.byteLength - reader.offset) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Truncated PLAIN boolean values");
    }
    const values = scope.allocate(
      present,
      "Parquet boolean values",
      () => new Uint8Array(present),
    ).value;
    if (encoding === "PLAIN") {
      for (let index = 0; index < present; index += 1)
        values[index] =
          (reader.view.getUint8(reader.offset + Math.floor(index / 8)) >> (index % 8)) & 1;
    } else if (present > 0) {
      readLevelRuns(reader, 1, values);
      for (const value of values)
        if (value > 1)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid boolean RLE value");
    }
    releaseDecodedScratch(page, bytes, values, scope);
    return {
      values,
      booleanValues: true,
      definitionLevels: present === count ? undefined : levels,
    };
  }
  const values =
    encoding === "ALP"
      ? readAlpValues(reader, decoder.type, present, scope)
      : encoding === "DELTA_BINARY_PACKED"
        ? readDeltaValues(reader, present, decoder.type === "INT32", scope)
        : encoding === "BYTE_STREAM_SPLIT"
          ? readByteStreamSplitValues(reader, decoder.type, present, scope)
          : readPlainValues(reader, decoder.type, present, decoder.element.type_length, scope);
  // Binary carrier arrays borrow page backing through their element views.
  // Keep the page owner until the enclosing decode scope releases those views.
  if (!plainBinaryEncoded) releaseDecodedScratch(page, bytes, values, scope);
  return { values, definitionLevels: present === count ? undefined : levels };
}

function releaseDecodedScratch(
  page: Uint8Array,
  input: Uint8Array,
  values: DecodedArray,
  scope: MemoryScope,
): void {
  // PLAIN and delta-length values may alias decompression output. Reconstructed
  // values have independent admitted storage and no longer need that scratch.
  // Never release caller input or an untracked custom compressor's output.
  if (
    page.buffer !== input.buffer &&
    (!ArrayBuffer.isView(values) || values.buffer !== page.buffer)
  )
    scope.tryReleaseAllocation(page);
}

/** Decode into the admitted output without allowing encoded runs to grow it. */
function readLevelRuns(
  reader: { view: DataView; offset: number },
  width: number,
  output: Uint8Array | Uint32Array,
  encodedLength?: number,
): void {
  visitLevelRuns(
    reader,
    width,
    output.length,
    (value, start, count) => {
      if (count === 1) output[start] = value;
      else output.fill(value, start, start + count);
    },
    encodedLength,
  );
}

/** Complete new backing for uncompressed flat fixed-width pages and V2 Snappy/ZSTD.
 * V2 exposes level bytes outside compression; other compressed layouts need
 * a separate decompression/level-planning stage. Custom codecs need their own
 * allocation contract and are not substituted by this planner. Input page
 * backing must remain pinned separately, including when aligned PLAIN output
 * aliases it. Logical conversion and multi-column alignment are separate stages.
 */
interface FlatNumericPageExtent {
  bytes: number;
  borrowsInput: boolean;
  rowCount: number;
  presentCount: number;
  valueWidth: number;
  compressed: boolean;
  zstd: boolean;
  mayHaveNulls?: boolean;
}

/** Conservative numeric decode allocation extent before transport. Input backing is
 * separate. V1 null counts use a worst-case bound. Unknown alignment may require
 * copying; ZSTD reserves its bounded pool.
 * Unsupported shapes/codecs return undefined, never an assumed zero extent.
 */
export function planFlatNumericPageHeader(
  header: PageHeader,
  decoder: ColumnDecoder,
): FlatNumericPageExtent | undefined {
  if (header.type === "DATA_PAGE") {
    const page = header.data_page_header;
    if (
      page === undefined ||
      getMaxRepetitionLevel(decoder.schemaPath) !== 0 ||
      !supportsFlatValueEncoding(decoder, page.encoding) ||
      !["UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP", "LZ4_RAW", "LZ4"].includes(decoder.codec) ||
      (decoder.codec !== "UNCOMPRESSED" && decoder.compressors?.[decoder.codec] !== undefined)
    )
      return undefined;
    const width = numericValueWidth(decoder);
    if (width === 0) return undefined;
    const layout = flatLevelLayout({ byteLength: header.compressed_page_size }, header, decoder);
    if (layout === undefined) return undefined;
    const { count, maximum } = layout;
    if (
      !Number.isSafeInteger(header.compressed_page_size) ||
      header.compressed_page_size < 0 ||
      !Number.isSafeInteger(header.uncompressed_page_size) ||
      header.uncompressed_page_size < 0
    )
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid V1 page extent");
    const compressed = decoder.codec !== "UNCOMPRESSED";
    const zstd = decoder.codec === "ZSTD";
    const bytes =
      (maximum > 0 ? count : 0) +
      count * width +
      (page.encoding === "ALP" ? 2 * c.view : 0) +
      (compressed ? header.uncompressed_page_size : 0) +
      (zstd ? ZSTD_REUSABLE_WORKSPACE_BYTES : 0) +
      (decoder.codec === "GZIP" ? GZIP_WORKSPACE_BYTES : 0);
    if (!Number.isSafeInteger(bytes))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "V1 page extent exceeds safe capacity");
    return {
      bytes,
      borrowsInput: false,
      rowCount: count,
      presentCount: count,
      valueWidth: width,
      compressed,
      zstd,
      mayHaveNulls: maximum > 0,
    };
  }
  return describeFlatNumericPage(undefined, header, decoder);
}

export function planFlatNumericPage(
  bytes: Uint8Array,
  header: PageHeader,
  decoder: ColumnDecoder,
): (FlatNumericPageExtent & { execute(scope: MemoryScope): NumericPage }) | undefined {
  const extent = describeFlatNumericPage(bytes, header, decoder);
  if (extent === undefined) return undefined;
  return {
    ...extent,
    execute(scope) {
      const scopedDecoder = extent.compressed
        ? { ...decoder, compressors: scopedParquetCompressors(scope, extent.zstd) }
        : decoder;
      const result = readNumericPage(bytes, header, scopedDecoder, scope);
      if (result === undefined) throw new Error("Numeric page changed after planning");
      return result;
    },
  };
}

function describeFlatNumericPage(
  bytes: Uint8Array | undefined,
  header: PageHeader,
  decoder: ColumnDecoder,
): FlatNumericPageExtent | undefined {
  const v1 = header.type === "DATA_PAGE" ? header.data_page_header : undefined;
  const v2 = header.type === "DATA_PAGE_V2" ? header.data_page_header_v2 : undefined;
  if (bytes === undefined && v2 === undefined) return undefined;
  const encoding = (v1 ?? v2)?.encoding;
  const compressed =
    v2 !== undefined &&
    v2.is_compressed !== false &&
    (decoder.codec === "SNAPPY" ||
      decoder.codec === "ZSTD" ||
      decoder.codec === "GZIP" ||
      decoder.codec === "LZ4_RAW" ||
      decoder.codec === "LZ4");
  const zstd = compressed && decoder.codec === "ZSTD";
  if (compressed && decoder.compressors?.[decoder.codec] !== undefined) return undefined;
  if (
    getMaxRepetitionLevel(decoder.schemaPath) !== 0 ||
    (decoder.codec !== "UNCOMPRESSED" && !(v2?.is_compressed === false) && !compressed) ||
    !supportsFlatValueEncoding(decoder, encoding)
  )
    return undefined;
  const width = numericValueWidth(decoder);
  if (width === 0) return undefined;
  const inputLength = bytes?.byteLength ?? header.compressed_page_size;
  if (!Number.isSafeInteger(inputLength) || inputLength < 0)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid numeric input extent");
  const layout = flatLevelLayout({ byteLength: inputLength }, header, decoder);
  if (layout === undefined) return undefined;
  const { count, maximum } = layout;
  let valueOffset = 0;
  let present = maximum === 0 ? count : 0;
  if (v2 !== undefined) {
    // V2 exposes counts and level extents in its header. Plan from those without
    // replaying level runs; execution validates actual counts before value decode.
    if (
      v2.num_rows !== count ||
      !Number.isSafeInteger(v2.num_nulls) ||
      v2.num_nulls < 0 ||
      v2.num_nulls > count ||
      (maximum === 0 && v2.num_nulls !== 0)
    )
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Inconsistent flat numeric page counts");
    present = count - v2.num_nulls;
    valueOffset = v2.repetition_levels_byte_length + v2.definition_levels_byte_length;
  } else if (maximum > 0 && bytes !== undefined) {
    const reader = {
      view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
      offset: 0,
    };
    visitLevelRuns(reader, 32 - Math.clz32(maximum), count, (level, _start, run) => {
      if (level > maximum)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid Parquet definition level");
      if (level === maximum) present += run;
    });
    valueOffset = reader.offset;
  }
  const booleanValues = decoder.type === "BOOLEAN";
  const payload = present * width;
  const encodedPayload =
    encoding === "ALP"
      ? 0
      : booleanValues
        ? encoding === "PLAIN"
          ? Math.ceil(present / 8)
          : 0
        : payload;
  const decodedBytes = compressed ? header.uncompressed_page_size - valueOffset : 0;
  const available = compressed ? decodedBytes : inputLength - valueOffset;
  if (
    !Number.isSafeInteger(available) ||
    available < 0 ||
    !Number.isSafeInteger(payload) ||
    encodedPayload > available
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid numeric value range");
  const borrowsInput =
    !booleanValues &&
    !compressed &&
    encoding === "PLAIN" &&
    present > 0 &&
    bytes !== undefined &&
    (bytes.byteOffset + valueOffset) % width === 0;
  const copiesValues =
    booleanValues ||
    encoding === "ALP" ||
    encoding === "BYTE_STREAM_SPLIT" ||
    (!compressed && !borrowsInput);
  const extent =
    (present < count ? count : 0) +
    decodedBytes +
    (copiesValues ? payload : 0) +
    (encoding === "ALP" ? 2 * c.view : 0) +
    (compressed && decoder.codec === "GZIP" ? GZIP_WORKSPACE_BYTES : 0) +
    (zstd
      ? bytes === undefined
        ? ZSTD_REUSABLE_WORKSPACE_BYTES
        : planZstdWorkspaceBytes(bytes.subarray(valueOffset))
      : 0);
  if (!Number.isSafeInteger(extent))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Numeric page extent exceeds safe capacity");
  return {
    bytes: extent,
    rowCount: count,
    presentCount: present,
    valueWidth: width,
    borrowsInput,
    compressed,
    zstd,
  };
}

function flatLevelLayout(
  bytes: { byteLength: number },
  header: PageHeader,
  decoder: ColumnDecoder,
) {
  const v1 = header.type === "DATA_PAGE" ? header.data_page_header : undefined;
  const v2 = header.type === "DATA_PAGE_V2" ? header.data_page_header_v2 : undefined;
  const count = (v1 ?? v2)?.num_values;
  if (count === undefined || !Number.isSafeInteger(count) || count < 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid numeric page count");
  }
  const maximum = getMaxDefinitionLevel(decoder.schemaPath);
  if (v1 && maximum > 0 && v1.definition_level_encoding !== "RLE") return undefined;
  if (
    v2 &&
    (!Number.isSafeInteger(v2.definition_levels_byte_length) ||
      v2.definition_levels_byte_length < 0 ||
      !Number.isSafeInteger(v2.repetition_levels_byte_length) ||
      v2.repetition_levels_byte_length < 0 ||
      v2.definition_levels_byte_length + v2.repetition_levels_byte_length > bytes.byteLength ||
      (maximum === 0 && v2.definition_levels_byte_length !== 0))
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid flat numeric level lengths");
  }
  return { count, maximum };
}

function supportsFlatValueEncoding(decoder: ColumnDecoder, encoding: unknown): boolean {
  return decoder.type === "BOOLEAN"
    ? encoding === "PLAIN" || encoding === "RLE"
    : encoding === "PLAIN" ||
        encoding === "BYTE_STREAM_SPLIT" ||
        (encoding === "ALP" && (decoder.type === "FLOAT" || decoder.type === "DOUBLE"));
}

function numericValueWidth(decoder: ColumnDecoder): number {
  if (decoder.type === "BOOLEAN") return 1;
  return decoder.type === "INT32" || decoder.type === "FLOAT"
    ? 4
    : decoder.type === "INT64" || decoder.type === "DOUBLE"
      ? 8
      : 0;
}
