import type { Encoding, PageHeader } from "hyparquet";
import { Encodings, PageTypes } from "hyparquet/src/constants.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import {
  IncompleteCompactHeaderError,
  PAGE_HEADER_PROJECTION,
  readCompactHeader,
} from "./compact-header.js";
import type { StoreAsyncBuffer } from "./types.js";

export const INITIAL_PAGE_HEADER_BYTES = 256;

export interface PreparedPageWindow {
  header: PageHeader;
  headerBytes: number;
  bodyOffset: number;
  bodyEnd: number;
  /** Borrowed only by the scope that retained this complete header-probe body. */
  inlineBody?: { scope: MemoryScope; bytes: Uint8Array; offset: number };
}

/** Inspect and validate a page extent without issuing a body read. Probe backing
 * is released by default for cross-column planning. Same-scope execution may opt
 * to retain a complete body already present in the bounded probe.
 */
export async function preparePageWindow(
  file: StoreAsyncBuffer,
  chunkStart: number,
  chunkEnd: number,
  offset: number,
  scope?: MemoryScope,
  reuseCompleteProbeBody = false,
): Promise<PreparedPageWindow | undefined> {
  if (scope?.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Page inspection scope disposed");
  const absoluteOffset = chunkStart + offset;
  if (
    !Number.isSafeInteger(chunkStart) ||
    chunkStart < 0 ||
    !Number.isSafeInteger(chunkEnd) ||
    chunkEnd < chunkStart ||
    chunkEnd > file.byteLength ||
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    !Number.isSafeInteger(absoluteOffset)
  )
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid Parquet column extent");
  if (absoluteOffset >= chunkEnd) return undefined;
  const header = await readPageHeader(
    file,
    absoluteOffset,
    chunkEnd,
    scope,
    reuseCompleteProbeBody,
  );
  const bodyOffset = absoluteOffset + header.headerBytes;
  const bodyEnd = bodyOffset + header.header.compressed_page_size;
  if (
    !Number.isSafeInteger(header.header.compressed_page_size) ||
    header.header.compressed_page_size < 0 ||
    !Number.isSafeInteger(bodyEnd) ||
    bodyEnd > chunkEnd
  ) {
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Parquet page body exceeds column chunk", {
      bodyOffset,
      bodyEnd,
      chunkEnd,
    });
  }
  return {
    ...header,
    bodyOffset,
    bodyEnd,
  };
}

/** Load a prepared extent into the execution scope chosen after inspection. */
export async function readPreparedPageBody(
  file: StoreAsyncBuffer,
  prepared: PreparedPageWindow,
  scope?: MemoryScope,
): Promise<Uint8Array> {
  if (scope?.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Page execution scope disposed");
  validatePreparedPageBody(file, prepared);
  const { bodyOffset, bodyEnd, inlineBody } = prepared;
  if (
    scope !== undefined &&
    inlineBody?.scope === scope &&
    inlineBody.offset === bodyOffset &&
    inlineBody.bytes.byteLength === bodyEnd - bodyOffset
  )
    return inlineBody.bytes;
  const owner =
    scope !== undefined && file.sliceOwned !== undefined
      ? await file.sliceOwned(bodyOffset, bodyEnd, scope)
      : undefined;
  const body = owner?.value ?? (await file.slice(bodyOffset, bodyEnd));
  if (scope?.disposed) {
    owner?.release();
    throw new LakeqlError("LAKEQL_ABORTED", "Page execution scope disposed");
  }
  if (body.byteLength !== bodyEnd - bodyOffset) {
    owner?.release();
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Incomplete Parquet page body", {
      expected: bodyEnd - bodyOffset,
      actual: body.byteLength,
    });
  }
  return new Uint8Array(body);
}

export function validatePreparedPageBody(
  file: StoreAsyncBuffer,
  prepared: PreparedPageWindow,
): void {
  const { bodyOffset, bodyEnd } = prepared;
  if (
    !Number.isSafeInteger(bodyOffset) ||
    bodyOffset < 0 ||
    !Number.isSafeInteger(bodyEnd) ||
    bodyEnd < bodyOffset ||
    bodyEnd > file.byteLength ||
    bodyEnd - bodyOffset !== prepared.header.compressed_page_size
  )
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid prepared Parquet body extent");
}

async function readPageHeader(
  file: StoreAsyncBuffer,
  absoluteOffset: number,
  chunkEnd: number,
  scope?: MemoryScope,
  reuseCompleteProbeBody = false,
): Promise<{
  header: PageHeader;
  headerBytes: number;
  inlineBody?: NonNullable<PreparedPageWindow["inlineBody"]>;
}> {
  let size = INITIAL_PAGE_HEADER_BYTES;
  let lastError: unknown;
  while (absoluteOffset < chunkEnd) {
    const end = Math.min(chunkEnd, absoluteOffset + size);
    const probe =
      scope !== undefined && file.sliceOwned !== undefined
        ? await file.sliceOwned(absoluteOffset, end, scope)
        : undefined;
    const bytes = probe?.value ?? (await file.slice(absoluteOffset, end));
    if (scope?.disposed) {
      probe?.release();
      throw new LakeqlError("LAKEQL_ABORTED", "Page inspection scope disposed");
    }
    const reader = { view: new DataView(bytes), offset: 0 };
    try {
      const header = parquetHeader(reader);
      const descriptorBytes = c.object + 3 * c.property + c.view;
      if (
        reuseCompleteProbeBody &&
        scope &&
        probe &&
        header.compressed_page_size >= 0 &&
        header.compressed_page_size <= bytes.byteLength - reader.offset &&
        descriptorBytes <= scope.budget.availableBytes
      ) {
        const inlineBody = scope.allocate(descriptorBytes, "retained header-probe body", () => ({
          scope,
          bytes: new Uint8Array(bytes, reader.offset, header.compressed_page_size),
          offset: absoluteOffset + reader.offset,
        })).value;
        return { header, headerBytes: reader.offset, inlineBody };
      }
      // The mapped page header contains only validated scalars and nested scalar
      // records; projected statistics/binary fields cannot retain probe backing.
      probe?.release();
      return { header, headerBytes: reader.offset };
    } catch (cause) {
      probe?.release();
      if (!(cause instanceof IncompleteCompactHeaderError)) throw cause;
      lastError = cause;
      if (end === chunkEnd) break;
      size *= 4;
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Unable to read Parquet page header");
}

export function parquetHeader(reader: { view: DataView; offset: number }): PageHeader {
  const header = readCompactHeader(reader, PAGE_HEADER_PROJECTION);
  type Fields = ReturnType<typeof readCompactHeader>;
  const number = (fields: Fields, key: string): number => {
    const value = fields[key as `field_${number}`];
    if (typeof value !== "number" || !Number.isSafeInteger(value))
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid numeric page header field", {
        key,
      });
    return value;
  };
  const optionalBoolean = (fields: Fields, key: string): boolean | undefined => {
    const value = fields[key as `field_${number}`];
    if (value !== undefined && typeof value !== "boolean")
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid boolean page header field", {
        key,
      });
    return value;
  };
  const struct = (key: string): Fields | undefined => {
    const value = header[key as `field_${number}`];
    if (value === undefined) return undefined;
    if (
      typeof value !== "object" ||
      value === null ||
      Array.isArray(value) ||
      ArrayBuffer.isView(value)
    )
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested page header field", {
        key,
      });
    return value;
  };
  const v1 = struct("field_5");
  const dictionary = struct("field_7");
  const v2 = struct("field_8");
  const sorted = dictionary === undefined ? undefined : optionalBoolean(dictionary, "field_3");
  return {
    type: PageTypes[number(header, "field_1")] as PageHeader["type"],
    uncompressed_page_size: number(header, "field_2"),
    compressed_page_size: number(header, "field_3"),
    ...(header.field_4 === undefined ? {} : { crc: number(header, "field_4") }),
    ...(v1 === undefined
      ? {}
      : {
          data_page_header: {
            num_values: number(v1, "field_1"),
            encoding: Encodings[number(v1, "field_2")] as Encoding,
            definition_level_encoding: Encodings[number(v1, "field_3")] as Encoding,
            repetition_level_encoding: Encodings[number(v1, "field_4")] as Encoding,
          },
        }),
    ...(dictionary === undefined
      ? {}
      : {
          dictionary_page_header: {
            num_values: number(dictionary, "field_1"),
            encoding: Encodings[number(dictionary, "field_2")] as Encoding,
            ...(sorted === undefined ? {} : { is_sorted: sorted }),
          },
        }),
    ...(v2 === undefined
      ? {}
      : {
          data_page_header_v2: {
            num_values: number(v2, "field_1"),
            num_nulls: number(v2, "field_2"),
            num_rows: number(v2, "field_3"),
            encoding: Encodings[number(v2, "field_4")] as Encoding,
            definition_levels_byte_length: number(v2, "field_5"),
            repetition_levels_byte_length: number(v2, "field_6"),
            is_compressed: optionalBoolean(v2, "field_7") ?? true,
          },
        }),
  };
}
