import { decompressPage } from "hyparquet/src/datapage.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type Vector,
} from "lakeql-core";
import { readPackedBinaryValues } from "./binary-values.js";
import { scopedParquetCompressors } from "./compressors.js";
import { nestedLeafVector } from "./nested-leaf-vector.js";
import { readPlainValues } from "./plain-values.js";

/** Decode one logical dictionary atomically. Legacy PLAIN_DICTIONARY dictionary
 * headers encode PLAIN payloads. Caller retains input backing for borrowed views.
 */
export function decodeNestedDictionaryPage(
  bytes: Uint8Array,
  header: PageHeader,
  decoder: ColumnDecoder,
  scope: MemoryScope,
): Vector | undefined {
  const dictionary = header.type === "DICTIONARY_PAGE" ? header.dictionary_page_header : undefined;
  if (
    !dictionary ||
    (dictionary.encoding !== "PLAIN" && dictionary.encoding !== "PLAIN_DICTIONARY") ||
    decoder.type === "BOOLEAN"
  )
    return undefined;
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Malformed nested dictionary page");
  const count = dictionary.num_values;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > 0xffffffff ||
    !Number.isSafeInteger(header.uncompressed_page_size) ||
    header.uncompressed_page_size < 0 ||
    bytes.length !== header.compressed_page_size
  )
    throw invalid();
  const construction = scope.childScope();
  const scratch = construction.childScope();
  try {
    const payload = decompressPage(
      bytes,
      header.uncompressed_page_size,
      decoder.codec,
      scopedParquetCompressors(construction),
    );
    if (payload.length !== header.uncompressed_page_size) throw invalid();
    const reader = scratch.allocate(
      c.object + 2 * c.property + c.view,
      "dictionary reader",
      () => ({
        view: new DataView(payload.buffer, payload.byteOffset, payload.byteLength),
        offset: 0,
      }),
    ).value;
    const binary = decoder.type === "BYTE_ARRAY" || decoder.type === "FIXED_LEN_BYTE_ARRAY";
    if (decoder.type === "FIXED_LEN_BYTE_ARRAY" && decoder.element.type_length === undefined)
      throw invalid();
    const packedBinary = binary
      ? construction.allocate(
          c.object + 4 * c.property + 2 * c.view,
          "packed dictionary descriptor",
          () =>
            readPackedBinaryValues(
              reader,
              count,
              decoder.type === "FIXED_LEN_BYTE_ARRAY" ? decoder.element.type_length : undefined,
              construction,
            ),
        ).value
      : undefined;
    const values = binary
      ? undefined
      : readPlainValues(reader, decoder.type, count, decoder.element.type_length, construction);
    if (reader.offset !== payload.length) throw invalid();
    const physical = scratch.allocate(
      2 * c.object + 9 * c.property,
      "dictionary conversion descriptor",
      () => ({
        values,
        packedBinary,
        dictionaryEncoded: false,
        levels: {
          presentCount: count,
          rowsStarted: count,
          valueOffset: 0,
          definition: undefined,
          repetition: undefined,
        },
      }),
    ).value;
    const result = nestedLeafVector(physical, decoder, construction);
    if (result === undefined) return undefined;
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    scratch.dispose();
    construction.dispose();
  }
}
