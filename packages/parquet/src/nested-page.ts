import { decompressPage } from "hyparquet/src/datapage.js";
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from "hyparquet/src/schema.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import { readPackedBinaryValues } from "./binary-values.js";
import { scopedParquetCompressors } from "./compressors.js";
import { readPackedDeltaBinaryValues } from "./delta-binary-values.js";
import { readDeltaValues } from "./delta-values.js";
import { readDictionaryIndices } from "./dictionary-indices.js";
import { readNestedLevels } from "./nested-levels.js";
import { supportsNestedValueEncoding } from "./nested-value-capability.js";
import { readByteStreamSplitValues, readPlainValues } from "./plain-values.js";

/** Physical numeric/PLAIN/dictionary capabilities for nested leaves. Logical conversion/reconstruction
 * are separate stages. Caller must retain input backing for borrowed PLAIN views.
 * Unsupported encodings return undefined before allocating or reading payload.
 */
export function readNestedPage(
  bytes: Uint8Array,
  header: PageHeader,
  decoder: ColumnDecoder,
  startsColumn: boolean,
  scope: MemoryScope,
  dictionaryLength?: number,
) {
  const v1 = header.type === "DATA_PAGE" ? header.data_page_header : undefined;
  const v2 = header.type === "DATA_PAGE_V2" ? header.data_page_header_v2 : undefined;
  const encoding = (v1 ?? v2)?.encoding;
  const dictionaryEncoded = encoding === "RLE_DICTIONARY" || encoding === "PLAIN_DICTIONARY";
  const delta =
    encoding === "DELTA_BINARY_PACKED" && (decoder.type === "INT32" || decoder.type === "INT64");
  const binaryDelta =
    decoder.type === "BYTE_ARRAY" &&
    (encoding === "DELTA_BYTE_ARRAY" || encoding === "DELTA_LENGTH_BYTE_ARRAY");
  const binaryPlain =
    encoding === "PLAIN" &&
    (decoder.type === "BYTE_ARRAY" || decoder.type === "FIXED_LEN_BYTE_ARRAY");
  const split =
    encoding === "BYTE_STREAM_SPLIT" &&
    ["INT32", "INT64", "FLOAT", "DOUBLE"].includes(decoder.type);
  if (
    !supportsNestedValueEncoding(decoder.type, encoding) ||
    (dictionaryEncoded && dictionaryLength === undefined)
  )
    return undefined;
  const maxRepetition = getMaxRepetitionLevel(decoder.schemaPath);
  const maxDefinition = getMaxDefinitionLevel(decoder.schemaPath);
  if (
    v1 &&
    ((maxRepetition > 0 && v1.repetition_level_encoding !== "RLE") ||
      (maxDefinition > 0 && v1.definition_level_encoding !== "RLE"))
  )
    return undefined;
  const invalid = () => new LakeqlError("LAKEQL_VALIDATION_ERROR", "Malformed nested Parquet page");
  if (
    bytes.length !== header.compressed_page_size ||
    !Number.isSafeInteger(header.uncompressed_page_size) ||
    header.uncompressed_page_size < 0
  )
    throw invalid();
  const construction = scope.childScope();
  const decompression = construction.childScope();
  try {
    const compressors = scopedParquetCompressors(decompression);
    let page = v1
      ? decompressPage(bytes, header.uncompressed_page_size, decoder.codec, compressors)
      : bytes;
    if (v1 && page.length !== header.uncompressed_page_size) throw invalid();
    if (v2) {
      for (const length of [v2.repetition_levels_byte_length, v2.definition_levels_byte_length])
        if (!Number.isSafeInteger(length) || length < 0) throw invalid();
      const levelBytes = v2.repetition_levels_byte_length + v2.definition_levels_byte_length;
      if (levelBytes > bytes.length || levelBytes > header.uncompressed_page_size) throw invalid();
    }
    const levels = readNestedLevels(
      page,
      {
        count: (v1 ?? v2)?.num_values ?? 0,
        maxRepetition,
        maxDefinition,
        startsColumn,
        ...(v2
          ? {
              repetitionBytes: v2.repetition_levels_byte_length,
              definitionBytes: v2.definition_levels_byte_length,
            }
          : {}),
      },
      construction,
    );
    let offset = levels.valueOffset;
    if (v2) {
      if (
        v2.num_nulls !== v2.num_values - levels.presentCount ||
        v2.num_rows !== levels.rowsStarted
      )
        throw invalid();
      page = bytes.subarray(offset);
      const expected = header.uncompressed_page_size - offset;
      if (v2.is_compressed !== false)
        page = decompressPage(page, expected, decoder.codec, compressors);
      if (page.length !== expected) throw invalid();
      offset = 0;
    }
    const reader = { view: new DataView(page.buffer, page.byteOffset, page.byteLength), offset };
    if (
      binaryPlain &&
      decoder.type === "FIXED_LEN_BYTE_ARRAY" &&
      decoder.element.type_length === undefined
    )
      throw invalid();
    const packedBinary =
      binaryPlain || binaryDelta
        ? construction.allocate(
            c.object + 4 * c.property + 2 * c.view,
            "nested packed binary descriptor",
            () =>
              binaryDelta
                ? readPackedDeltaBinaryValues(
                    reader,
                    levels.presentCount,
                    encoding === "DELTA_BYTE_ARRAY",
                    construction,
                  )
                : readPackedBinaryValues(
                    reader,
                    levels.presentCount,
                    decoder.type === "FIXED_LEN_BYTE_ARRAY"
                      ? decoder.element.type_length
                      : undefined,
                    construction,
                  ),
          ).value
        : undefined;
    const values =
      packedBinary !== undefined
        ? undefined
        : delta
          ? readDeltaValues(reader, levels.presentCount, decoder.type === "INT32", construction)
          : split
            ? readByteStreamSplitValues(reader, decoder.type, levels.presentCount, construction)
            : dictionaryEncoded
              ? readDictionaryIndices(
                  reader,
                  levels.presentCount,
                  dictionaryLength ?? 0,
                  construction,
                )
              : decoder.type === "BOOLEAN"
                ? (() => {
                    const size = Math.ceil(levels.presentCount / 8);
                    if (reader.offset + size > page.length) throw invalid();
                    const output = construction.allocate(
                      levels.presentCount + c.view,
                      "nested boolean values",
                      () => new Uint8Array(levels.presentCount),
                    ).value;
                    for (let i = 0; i < output.length; i++)
                      output[i] =
                        (reader.view.getUint8(reader.offset + Math.floor(i / 8)) >> (i % 8)) & 1;
                    reader.offset += size;
                    return output;
                  })()
                : readPlainValues(
                    reader,
                    decoder.type,
                    levels.presentCount,
                    decoder.element.type_length,
                    construction,
                  );
    if (reader.offset !== page.length) throw invalid();
    // Levels and reconstructed values own their storage. Keep decompressed bytes
    // only for packed payloads or numeric views that actually alias that buffer.
    const borrowsPage =
      packedBinary?.payload.buffer === page.buffer ||
      (ArrayBuffer.isView(values) && values.buffer === page.buffer);
    if (borrowsPage && page.buffer !== bytes.buffer)
      construction.adopt(decompression.retainAllocations(undefined));
    decompression.dispose();
    const result = construction.allocate(c.object + 4 * c.property, "nested physical page", () => ({
      levels,
      values,
      dictionaryEncoded,
      packedBinary,
    })).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    decompression.dispose();
    construction.dispose();
  }
}
