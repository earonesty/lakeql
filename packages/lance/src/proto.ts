// Lance protobuf message shapes implemented from the Apache-2.0 format
// specifications in lance-format/lance v8.0.0. This deliberately decodes only
// table/file structures needed by the supported read-only storage 2.0 contract.

import { LakeqlError } from "lakeql";

export interface LanceField {
  id: number;
  name: string;
  parentId: number;
  logicalType: string;
  nullable: boolean;
}

export interface LanceDataFile {
  path: string;
  fields: number[];
  columnIndices: number[];
  fileMajorVersion: number;
  fileMinorVersion: number;
  fileSizeBytes: number;
}

export interface LanceDeletionFile {
  fileType: number;
  readVersion: bigint;
  id: bigint;
  numDeletedRows: bigint;
}

export interface LanceExternalFile {
  path: string;
  offset: number;
  size: number;
}

export interface LanceFragment {
  id: bigint;
  files: LanceDataFile[];
  deletionFile?: LanceDeletionFile;
  physicalRows: number;
  inlineRowIds?: Uint8Array;
  externalRowIds?: LanceExternalFile;
}

export interface LanceManifest {
  fields: LanceField[];
  fragments: LanceFragment[];
  version: bigint;
  readerFeatureFlags: bigint;
  writerLibrary: string;
  writerVersion: string;
  dataFileFormat: string;
  dataStorageVersion: string;
  indexSectionOffset?: number;
}

export interface LanceIndexFile {
  path: string;
  sizeBytes: number;
}

export interface LanceIndexMetadata {
  uuid: string;
  fields: number[];
  name: string;
  datasetVersion: bigint;
  detailsTypeUrl: string;
  details?: Uint8Array;
  indexVersion?: number;
  files: LanceIndexFile[];
}

export interface LanceFileDescriptor {
  fields: LanceField[];
  metadata: Record<string, Uint8Array>;
  length: number;
}

export interface LanceIvfMetadata {
  offsets: number[];
  lengths: number[];
  centroids: Float32Array;
  numPartitions: number;
  dimension: number;
}

export interface LancePage {
  bufferOffsets: number[];
  bufferSizes: number[];
  length: number;
  priority: number;
  encoding: LanceArrayEncoding;
}

export interface LanceColumnMetadata {
  pages: LancePage[];
  bufferOffsets: number[];
  bufferSizes: number[];
}

export type LanceArrayEncoding =
  | LanceFlatEncoding
  | LanceNullableEncoding
  | LanceFixedSizeListEncoding
  | LanceBinaryEncoding
  | LanceDictionaryEncoding
  | LanceConstantEncoding;

export interface LanceFlatEncoding {
  kind: "flat";
  bitsPerValue: number;
  bufferIndex: number;
  bufferType: number;
  compression?: string;
}

export type LanceNullableEncoding =
  | {
      kind: "nullable";
      mode: "no_nulls";
      values: LanceArrayEncoding;
    }
  | {
      kind: "nullable";
      mode: "some_nulls";
      validity: LanceArrayEncoding;
      values: LanceArrayEncoding;
    }
  | {
      kind: "nullable";
      mode: "all_nulls";
    };

export interface LanceFixedSizeListEncoding {
  kind: "fixed_size_list";
  dimension: number;
  hasValidity: boolean;
  items: LanceArrayEncoding;
}

export interface LanceBinaryEncoding {
  kind: "binary";
  indices: LanceArrayEncoding;
  bytes: LanceArrayEncoding;
  nullAdjustment: bigint;
}

export interface LanceDictionaryEncoding {
  kind: "dictionary";
  indices: LanceArrayEncoding;
  items: LanceArrayEncoding;
  numItems: number;
}

export interface LanceConstantEncoding {
  kind: "constant";
  value: Uint8Array;
}

export interface LanceRowIdSegment {
  kind: "range" | "range_with_holes" | "range_with_bitmap" | "sorted_array" | "array";
  start?: bigint;
  end?: bigint;
  holes?: bigint[];
  bitmap?: Uint8Array;
  values?: bigint[];
}

export function parseManifest(bytes: Uint8Array): LanceManifest {
  const reader = new ProtoReader(bytes);
  const fields: LanceField[] = [];
  const fragments: LanceFragment[] = [];
  let version = 0n;
  let readerFeatureFlags = 0n;
  let writerLibrary = "";
  let writerVersion = "";
  let dataFileFormat = "";
  let dataStorageVersion = "";
  let indexSectionOffset: number | undefined;
  while (!reader.done) {
    const { field, wire } = reader.key();
    switch (field) {
      case 1:
        fields.push(parseField(reader.message(wire)));
        break;
      case 2:
        fragments.push(parseFragment(reader.message(wire)));
        break;
      case 3:
        version = reader.varint(wire);
        break;
      case 6:
        indexSectionOffset = reader.safeInteger(wire, "index section offset");
        break;
      case 9:
        readerFeatureFlags = reader.varint(wire);
        break;
      case 13: {
        const writer = parseWriterVersion(reader.message(wire));
        writerLibrary = writer.library;
        writerVersion = writer.version;
        break;
      }
      case 15: {
        const format = parseDataFormat(reader.message(wire));
        dataFileFormat = format.fileFormat;
        dataStorageVersion = format.version;
        break;
      }
      default:
        reader.skip(wire);
    }
  }
  return {
    fields,
    fragments,
    version,
    readerFeatureFlags,
    writerLibrary,
    writerVersion,
    dataFileFormat,
    dataStorageVersion,
    ...(indexSectionOffset === undefined ? {} : { indexSectionOffset }),
  };
}

export function parseIndexSection(bytes: Uint8Array): LanceIndexMetadata[] {
  const reader = new ProtoReader(bytes);
  const indices: LanceIndexMetadata[] = [];
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) indices.push(parseIndexMetadata(reader.message(key.wire)));
    else reader.skip(key.wire);
  }
  return indices;
}

export function parseFileDescriptor(bytes: Uint8Array): LanceFileDescriptor {
  const reader = new ProtoReader(bytes);
  let fields: LanceField[] = [];
  let metadata: Record<string, Uint8Array> = {};
  let length = 0;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) {
      const schema = parseSchema(reader.message(key.wire));
      fields = schema.fields;
      metadata = schema.metadata;
    } else if (key.field === 2) {
      length = reader.safeInteger(key.wire, "file descriptor row count");
    } else {
      reader.skip(key.wire);
    }
  }
  return { fields, metadata, length };
}

export function parseIvfMetadata(bytes: Uint8Array): LanceIvfMetadata {
  const reader = new ProtoReader(bytes);
  const offsets: number[] = [];
  const lengths: number[] = [];
  let tensor: { dataType: number; shape: number[]; data: Uint8Array } | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 2) offsets.push(...reader.packedSafeIntegers(key.wire, "IVF offset"));
    else if (key.field === 3) lengths.push(...reader.packedSafeIntegers(key.wire, "IVF length"));
    else if (key.field === 4) tensor = parseTensor(reader.message(key.wire));
    else reader.skip(key.wire);
  }
  if (offsets.length === 0 || offsets.length !== lengths.length) {
    corrupt("Lance IVF partition tables are empty or inconsistent", {
      offsets: offsets.length,
      lengths: lengths.length,
    });
  }
  if (tensor === undefined) {
    return {
      offsets,
      lengths,
      centroids: new Float32Array(),
      numPartitions: offsets.length,
      dimension: 0,
    };
  }
  if (tensor.dataType !== 2 || tensor.shape.length !== 2) {
    unsupported("Unsupported Lance IVF centroid tensor");
  }
  const numPartitions = tensor.shape[0] as number;
  const dimension = tensor.shape[1] as number;
  if (
    numPartitions !== offsets.length ||
    dimension <= 0 ||
    tensor.data.byteLength !== numPartitions * dimension * 4
  ) {
    corrupt("Lance IVF centroid tensor shape is inconsistent", {
      numPartitions,
      dimension,
      bytes: tensor.data.byteLength,
    });
  }
  const centroids = new Float32Array(numPartitions * dimension);
  const view = dataView(tensor.data);
  for (let index = 0; index < centroids.length; index += 1) {
    centroids[index] = view.getFloat32(index * 4, true);
  }
  return { offsets, lengths, centroids, numPartitions, dimension };
}

function parseTensor(bytes: Uint8Array): {
  dataType: number;
  shape: number[];
  data: Uint8Array;
} {
  const reader = new ProtoReader(bytes);
  let dataType = 0;
  const shape: number[] = [];
  let data: Uint8Array<ArrayBufferLike> = new Uint8Array();
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) dataType = reader.safeInteger(key.wire, "tensor data type");
    else if (key.field === 2) shape.push(...reader.packedSafeIntegers(key.wire, "tensor shape"));
    else if (key.field === 3) data = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  return { dataType, shape, data };
}

export function parseRowIdSequence(bytes: Uint8Array): LanceRowIdSegment[] {
  const reader = new ProtoReader(bytes);
  const segments: LanceRowIdSegment[] = [];
  while (!reader.done) {
    const { field, wire } = reader.key();
    if (field === 1) segments.push(parseU64Segment(reader.message(wire)));
    else reader.skip(wire);
  }
  return segments;
}

export function parseColumnMetadata(bytes: Uint8Array): LanceColumnMetadata {
  const reader = new ProtoReader(bytes);
  const pages: LancePage[] = [];
  const bufferOffsets: number[] = [];
  const bufferSizes: number[] = [];
  while (!reader.done) {
    const { field, wire } = reader.key();
    switch (field) {
      case 2:
        pages.push(parsePage(reader.message(wire)));
        break;
      case 3:
        bufferOffsets.push(...reader.packedSafeIntegers(wire, "column buffer offset"));
        break;
      case 4:
        bufferSizes.push(...reader.packedSafeIntegers(wire, "column buffer size"));
        break;
      default:
        reader.skip(wire);
    }
  }
  if (bufferOffsets.length !== bufferSizes.length) {
    corrupt("Lance column metadata buffer tables have different lengths", {
      offsets: bufferOffsets.length,
      sizes: bufferSizes.length,
    });
  }
  return { pages, bufferOffsets, bufferSizes };
}

function parseIndexMetadata(bytes: Uint8Array): LanceIndexMetadata {
  const reader = new ProtoReader(bytes);
  let uuid = "";
  const fields: number[] = [];
  let name = "";
  let datasetVersion = 0n;
  let detailsTypeUrl = "";
  let details: Uint8Array | undefined;
  let indexVersion: number | undefined;
  const files: LanceIndexFile[] = [];
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1:
        uuid = parseUuid(reader.message(key.wire));
        break;
      case 2:
        fields.push(...reader.packedInt32(key.wire));
        break;
      case 3:
        name = reader.string(key.wire);
        break;
      case 4:
        datasetVersion = reader.varint(key.wire);
        break;
      case 6:
        ({ typeUrl: detailsTypeUrl, value: details } = parseAny(reader.message(key.wire)));
        break;
      case 7:
        indexVersion = reader.int32(key.wire);
        break;
      case 10:
        files.push(parseIndexFile(reader.message(key.wire)));
        break;
      default:
        reader.skip(key.wire);
    }
  }
  return {
    uuid,
    fields,
    name,
    datasetVersion,
    detailsTypeUrl,
    ...(details === undefined ? {} : { details }),
    ...(indexVersion === undefined ? {} : { indexVersion }),
    files,
  };
}

function parseUuid(bytes: Uint8Array): string {
  const reader = new ProtoReader(bytes);
  let raw: Uint8Array | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) raw = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  if (raw?.byteLength !== 16) corrupt("Lance index UUID must contain 16 bytes");
  const hex = [...raw].map((value) => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(
    16,
    20,
  )}-${hex.slice(20)}`;
}

function parseAny(bytes: Uint8Array): { typeUrl: string; value?: Uint8Array } {
  const reader = new ProtoReader(bytes);
  let typeUrl = "";
  let value: Uint8Array | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) typeUrl = reader.string(key.wire);
    else if (key.field === 2) value = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  return { typeUrl, ...(value === undefined ? {} : { value }) };
}

function parseIndexFile(bytes: Uint8Array): LanceIndexFile {
  const reader = new ProtoReader(bytes);
  let path = "";
  let sizeBytes = 0;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) path = reader.string(key.wire);
    else if (key.field === 2) sizeBytes = reader.safeInteger(key.wire, "index file size");
    else reader.skip(key.wire);
  }
  return { path, sizeBytes };
}

function parseSchema(bytes: Uint8Array): {
  fields: LanceField[];
  metadata: Record<string, Uint8Array>;
} {
  const reader = new ProtoReader(bytes);
  const fields: LanceField[] = [];
  const metadata: Record<string, Uint8Array> = {};
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) fields.push(parseField(reader.message(key.wire)));
    else if (key.field === 5) {
      const entry = parseBytesMapEntry(reader.message(key.wire));
      metadata[entry.key] = entry.value;
    } else reader.skip(key.wire);
  }
  return { fields, metadata };
}

function parseBytesMapEntry(bytes: Uint8Array): { key: string; value: Uint8Array } {
  const reader = new ProtoReader(bytes);
  let key = "";
  let value: Uint8Array<ArrayBufferLike> = new Uint8Array();
  while (!reader.done) {
    const field = reader.key();
    if (field.field === 1) key = reader.string(field.wire);
    else if (field.field === 2) value = reader.bytesValue(field.wire);
    else reader.skip(field.wire);
  }
  return { key, value };
}

function parseField(bytes: Uint8Array): LanceField {
  const reader = new ProtoReader(bytes);
  let id = 0;
  let name = "";
  let parentId = 0;
  let logicalType = "";
  let nullable = false;
  while (!reader.done) {
    const { field, wire } = reader.key();
    switch (field) {
      case 2:
        name = reader.string(wire);
        break;
      case 3:
        id = reader.safeInteger(wire, "field id");
        break;
      case 4:
        parentId = reader.int32(wire);
        break;
      case 5:
        logicalType = reader.string(wire);
        break;
      case 6:
        nullable = reader.varint(wire) !== 0n;
        break;
      default:
        reader.skip(wire);
    }
  }
  return { id, name, parentId, logicalType, nullable };
}

function parseFragment(bytes: Uint8Array): LanceFragment {
  const reader = new ProtoReader(bytes);
  const files: LanceDataFile[] = [];
  let id = 0n;
  let deletionFile: LanceDeletionFile | undefined;
  let physicalRows = 0;
  let inlineRowIds: Uint8Array | undefined;
  let externalRowIds: LanceExternalFile | undefined;
  while (!reader.done) {
    const { field, wire } = reader.key();
    switch (field) {
      case 1:
        id = reader.varint(wire);
        break;
      case 2:
        files.push(parseDataFile(reader.message(wire)));
        break;
      case 3:
        deletionFile = parseDeletionFile(reader.message(wire));
        break;
      case 4:
        physicalRows = reader.safeInteger(wire, "fragment physical rows");
        break;
      case 5:
        inlineRowIds = reader.bytesValue(wire);
        break;
      case 6:
        externalRowIds = parseExternalFile(reader.message(wire));
        break;
      default:
        reader.skip(wire);
    }
  }
  return {
    id,
    files,
    physicalRows,
    ...(deletionFile === undefined ? {} : { deletionFile }),
    ...(inlineRowIds === undefined ? {} : { inlineRowIds }),
    ...(externalRowIds === undefined ? {} : { externalRowIds }),
  };
}

function parseDataFile(bytes: Uint8Array): LanceDataFile {
  const reader = new ProtoReader(bytes);
  let path = "";
  const fields: number[] = [];
  const columnIndices: number[] = [];
  let fileMajorVersion = 0;
  let fileMinorVersion = 0;
  let fileSizeBytes = 0;
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1:
        path = reader.string(key.wire);
        break;
      case 2:
        fields.push(...reader.packedInt32(key.wire));
        break;
      case 3:
        columnIndices.push(...reader.packedInt32(key.wire));
        break;
      case 4:
        fileMajorVersion = reader.safeInteger(key.wire, "data file major version");
        break;
      case 5:
        fileMinorVersion = reader.safeInteger(key.wire, "data file minor version");
        break;
      case 6:
        fileSizeBytes = reader.safeInteger(key.wire, "data file size");
        break;
      default:
        reader.skip(key.wire);
    }
  }
  return {
    path,
    fields,
    columnIndices,
    fileMajorVersion,
    fileMinorVersion,
    fileSizeBytes,
  };
}

function parseDeletionFile(bytes: Uint8Array): LanceDeletionFile {
  const reader = new ProtoReader(bytes);
  let fileType = 0;
  let readVersion = 0n;
  let id = 0n;
  let numDeletedRows = 0n;
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1:
        fileType = reader.safeInteger(key.wire, "deletion file type");
        break;
      case 2:
        readVersion = reader.varint(key.wire);
        break;
      case 3:
        id = reader.varint(key.wire);
        break;
      case 4:
        numDeletedRows = reader.varint(key.wire);
        break;
      default:
        reader.skip(key.wire);
    }
  }
  return { fileType, readVersion, id, numDeletedRows };
}

function parseExternalFile(bytes: Uint8Array): LanceExternalFile {
  const reader = new ProtoReader(bytes);
  let path = "";
  let offset = 0;
  let size = 0;
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1:
        path = reader.string(key.wire);
        break;
      case 2:
        offset = reader.safeInteger(key.wire, "external row-id offset");
        break;
      case 3:
        size = reader.safeInteger(key.wire, "external row-id size");
        break;
      default:
        reader.skip(key.wire);
    }
  }
  return { path, offset, size };
}

function parseWriterVersion(bytes: Uint8Array): { library: string; version: string } {
  const reader = new ProtoReader(bytes);
  let library = "";
  let version = "";
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) library = reader.string(key.wire);
    else if (key.field === 2) version = reader.string(key.wire);
    else reader.skip(key.wire);
  }
  return { library, version };
}

function parseDataFormat(bytes: Uint8Array): { fileFormat: string; version: string } {
  const reader = new ProtoReader(bytes);
  let fileFormat = "";
  let version = "";
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) fileFormat = reader.string(key.wire);
    else if (key.field === 2) version = reader.string(key.wire);
    else reader.skip(key.wire);
  }
  return { fileFormat, version };
}

function parsePage(bytes: Uint8Array): LancePage {
  const reader = new ProtoReader(bytes);
  const bufferOffsets: number[] = [];
  const bufferSizes: number[] = [];
  let length = 0;
  let priority = 0;
  let encoding: LanceArrayEncoding | undefined;
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1:
        bufferOffsets.push(...reader.packedSafeIntegers(key.wire, "page buffer offset"));
        break;
      case 2:
        bufferSizes.push(...reader.packedSafeIntegers(key.wire, "page buffer size"));
        break;
      case 3:
        length = reader.safeInteger(key.wire, "page length");
        break;
      case 4:
        encoding = parseFileEncoding(reader.message(key.wire));
        break;
      case 5:
        priority = reader.safeInteger(key.wire, "page priority");
        break;
      default:
        reader.skip(key.wire);
    }
  }
  if (bufferOffsets.length !== bufferSizes.length) {
    corrupt("Lance page buffer tables have different lengths", {
      offsets: bufferOffsets.length,
      sizes: bufferSizes.length,
    });
  }
  if (encoding === undefined) corrupt("Lance page has no direct encoding");
  return { bufferOffsets, bufferSizes, length, priority, encoding };
}

function parseFileEncoding(bytes: Uint8Array): LanceArrayEncoding {
  const reader = new ProtoReader(bytes);
  let direct: Uint8Array | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 2) direct = parseDirectEncoding(reader.message(key.wire));
    else if (key.field === 1) {
      unsupported("Deferred Lance page encodings are not supported");
    } else {
      reader.skip(key.wire);
    }
  }
  if (direct === undefined) corrupt("Lance page encoding is absent");
  return parseEncodingAny(direct);
}

function parseDirectEncoding(bytes: Uint8Array): Uint8Array {
  const reader = new ProtoReader(bytes);
  let encoding: Uint8Array | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) encoding = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  if (encoding === undefined) corrupt("Lance direct encoding has no payload");
  return encoding;
}

function parseEncodingAny(bytes: Uint8Array): LanceArrayEncoding {
  const reader = new ProtoReader(bytes);
  let typeUrl = "";
  let value: Uint8Array | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) typeUrl = reader.string(key.wire);
    else if (key.field === 2) value = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  if (typeUrl !== "/lance.encodings.ArrayEncoding") {
    unsupported("Unsupported Lance page encoding type", { typeUrl });
  }
  if (value === undefined) corrupt("Lance ArrayEncoding payload is absent");
  return parseArrayEncoding(value);
}

function parseArrayEncoding(bytes: Uint8Array): LanceArrayEncoding {
  const reader = new ProtoReader(bytes);
  let encoding: LanceArrayEncoding | undefined;
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1:
        encoding = parseFlat(reader.message(key.wire));
        break;
      case 2:
        encoding = parseNullable(reader.message(key.wire));
        break;
      case 3:
        encoding = parseFixedSizeList(reader.message(key.wire));
        break;
      case 6:
        encoding = parseBinary(reader.message(key.wire));
        break;
      case 7:
        encoding = parseDictionary(reader.message(key.wire));
        break;
      case 13:
        encoding = parseConstant(reader.message(key.wire));
        break;
      default:
        unsupported("Unsupported Lance 2.0 array encoding", { encodingField: key.field });
    }
  }
  if (encoding === undefined) corrupt("Lance ArrayEncoding is empty");
  return encoding;
}

function parseFixedSizeList(bytes: Uint8Array): LanceFixedSizeListEncoding {
  const reader = new ProtoReader(bytes);
  let dimension = 0;
  let hasValidity = false;
  let items: LanceArrayEncoding | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) dimension = reader.safeInteger(key.wire, "fixed-size-list dimension");
    else if (key.field === 2) items = parseArrayEncoding(reader.message(key.wire));
    else if (key.field === 3) hasValidity = reader.varint(key.wire) !== 0n;
    else reader.skip(key.wire);
  }
  if (dimension <= 0 || items === undefined) {
    corrupt("Lance fixed-size-list encoding is incomplete", { dimension });
  }
  return { kind: "fixed_size_list", dimension, hasValidity, items };
}

function parseDictionary(bytes: Uint8Array): LanceDictionaryEncoding {
  const reader = new ProtoReader(bytes);
  let indices: LanceArrayEncoding | undefined;
  let items: LanceArrayEncoding | undefined;
  let numItems = 0;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) indices = parseArrayEncoding(reader.message(key.wire));
    else if (key.field === 2) items = parseArrayEncoding(reader.message(key.wire));
    else if (key.field === 3) numItems = reader.safeInteger(key.wire, "dictionary item count");
    else reader.skip(key.wire);
  }
  if (indices === undefined || items === undefined || numItems <= 0) {
    corrupt("Lance dictionary encoding is incomplete", { numItems });
  }
  return { kind: "dictionary", indices, items, numItems };
}

function parseFlat(bytes: Uint8Array): LanceFlatEncoding {
  const reader = new ProtoReader(bytes);
  let bitsPerValue = 0;
  let bufferIndex = 0;
  let bufferType = 0;
  let compression: string | undefined;
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1:
        bitsPerValue = reader.safeInteger(key.wire, "flat bits per value");
        break;
      case 2: {
        const buffer = parseBuffer(reader.message(key.wire));
        bufferIndex = buffer.index;
        bufferType = buffer.type;
        break;
      }
      case 3:
        compression = parseCompression(reader.message(key.wire));
        break;
      default:
        reader.skip(key.wire);
    }
  }
  return {
    kind: "flat",
    bitsPerValue,
    bufferIndex,
    bufferType,
    ...(compression === undefined || compression === "" ? {} : { compression }),
  };
}

function parseBuffer(bytes: Uint8Array): { index: number; type: number } {
  const reader = new ProtoReader(bytes);
  let index = 0;
  let type = 0;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) index = reader.safeInteger(key.wire, "buffer index");
    else if (key.field === 2) type = reader.safeInteger(key.wire, "buffer type");
    else reader.skip(key.wire);
  }
  return { index, type };
}

function parseCompression(bytes: Uint8Array): string {
  const reader = new ProtoReader(bytes);
  let scheme = "";
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) scheme = reader.string(key.wire);
    else reader.skip(key.wire);
  }
  return scheme;
}

function parseNullable(bytes: Uint8Array): LanceNullableEncoding {
  const reader = new ProtoReader(bytes);
  let encoding: LanceNullableEncoding | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) {
      const nested = new ProtoReader(reader.message(key.wire));
      let values: LanceArrayEncoding | undefined;
      while (!nested.done) {
        const inner = nested.key();
        if (inner.field === 1) values = parseArrayEncoding(nested.message(inner.wire));
        else nested.skip(inner.wire);
      }
      if (values === undefined) corrupt("Nullable no-nulls encoding has no values");
      encoding = { kind: "nullable", mode: "no_nulls", values };
    } else if (key.field === 2) {
      const nested = new ProtoReader(reader.message(key.wire));
      let validity: LanceArrayEncoding | undefined;
      let values: LanceArrayEncoding | undefined;
      while (!nested.done) {
        const inner = nested.key();
        if (inner.field === 1) validity = parseArrayEncoding(nested.message(inner.wire));
        else if (inner.field === 2) values = parseArrayEncoding(nested.message(inner.wire));
        else nested.skip(inner.wire);
      }
      if (validity === undefined || values === undefined) {
        corrupt("Nullable some-nulls encoding is incomplete");
      }
      encoding = { kind: "nullable", mode: "some_nulls", validity, values };
    } else if (key.field === 3) {
      reader.message(key.wire);
      encoding = { kind: "nullable", mode: "all_nulls" };
    } else {
      reader.skip(key.wire);
    }
  }
  if (encoding === undefined) corrupt("Nullable Lance encoding has no nullability mode");
  return encoding;
}

function parseBinary(bytes: Uint8Array): LanceBinaryEncoding {
  const reader = new ProtoReader(bytes);
  let indices: LanceArrayEncoding | undefined;
  let values: LanceArrayEncoding | undefined;
  let nullAdjustment = 0n;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) indices = parseArrayEncoding(reader.message(key.wire));
    else if (key.field === 2) values = parseArrayEncoding(reader.message(key.wire));
    else if (key.field === 3) nullAdjustment = reader.varint(key.wire);
    else reader.skip(key.wire);
  }
  if (indices === undefined || values === undefined) corrupt("Lance binary encoding is incomplete");
  return { kind: "binary", indices, bytes: values, nullAdjustment };
}

function parseConstant(bytes: Uint8Array): LanceConstantEncoding {
  const reader = new ProtoReader(bytes);
  let value: Uint8Array<ArrayBufferLike> = new Uint8Array();
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) value = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  return { kind: "constant", value };
}

function parseU64Segment(bytes: Uint8Array): LanceRowIdSegment {
  const reader = new ProtoReader(bytes);
  let segment: LanceRowIdSegment | undefined;
  while (!reader.done) {
    const key = reader.key();
    switch (key.field) {
      case 1: {
        const range = parseRange(reader.message(key.wire));
        segment = { kind: "range", ...range };
        break;
      }
      case 2: {
        const range = parseRangeWithHoles(reader.message(key.wire));
        segment = { kind: "range_with_holes", ...range };
        break;
      }
      case 3: {
        const range = parseRangeWithBitmap(reader.message(key.wire));
        segment = { kind: "range_with_bitmap", ...range };
        break;
      }
      case 4:
        segment = { kind: "sorted_array", values: parseEncodedU64Array(reader.message(key.wire)) };
        break;
      case 5:
        segment = { kind: "array", values: parseEncodedU64Array(reader.message(key.wire)) };
        break;
      default:
        reader.skip(key.wire);
    }
  }
  if (segment === undefined) corrupt("Lance row-id segment is empty");
  return segment;
}

function parseRange(bytes: Uint8Array): { start: bigint; end: bigint } {
  const reader = new ProtoReader(bytes);
  let start = 0n;
  let end = 0n;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) start = reader.varint(key.wire);
    else if (key.field === 2) end = reader.varint(key.wire);
    else reader.skip(key.wire);
  }
  return { start, end };
}

function parseRangeWithHoles(bytes: Uint8Array): { start: bigint; end: bigint; holes: bigint[] } {
  const reader = new ProtoReader(bytes);
  let start = 0n;
  let end = 0n;
  let holes: bigint[] = [];
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) start = reader.varint(key.wire);
    else if (key.field === 2) end = reader.varint(key.wire);
    else if (key.field === 3) holes = parseEncodedU64Array(reader.message(key.wire));
    else reader.skip(key.wire);
  }
  return { start, end, holes };
}

function parseRangeWithBitmap(bytes: Uint8Array): {
  start: bigint;
  end: bigint;
  bitmap: Uint8Array;
} {
  const reader = new ProtoReader(bytes);
  let start = 0n;
  let end = 0n;
  let bitmap: Uint8Array<ArrayBufferLike> = new Uint8Array();
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) start = reader.varint(key.wire);
    else if (key.field === 2) end = reader.varint(key.wire);
    else if (key.field === 3) bitmap = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  return { start, end, bitmap };
}

function parseEncodedU64Array(bytes: Uint8Array): bigint[] {
  const reader = new ProtoReader(bytes);
  let values: bigint[] | undefined;
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) values = parseDeltaArray(reader.message(key.wire), 2);
    else if (key.field === 2) values = parseDeltaArray(reader.message(key.wire), 4);
    else if (key.field === 3) values = parseU64Array(reader.message(key.wire));
    else reader.skip(key.wire);
  }
  if (values === undefined) corrupt("Encoded Lance u64 array is empty");
  return values;
}

function parseDeltaArray(bytes: Uint8Array, width: 2 | 4): bigint[] {
  const reader = new ProtoReader(bytes);
  let base = 0n;
  let offsets: Uint8Array<ArrayBufferLike> = new Uint8Array();
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 1) base = reader.varint(key.wire);
    else if (key.field === 2) offsets = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  if (offsets.byteLength % width !== 0) corrupt("Lance row-id delta array is truncated");
  const view = dataView(offsets);
  const values: bigint[] = [];
  for (let offset = 0; offset < offsets.byteLength; offset += width) {
    const delta = width === 2 ? view.getUint16(offset, true) : view.getUint32(offset, true);
    values.push(base + BigInt(delta));
  }
  return values;
}

function parseU64Array(bytes: Uint8Array): bigint[] {
  const reader = new ProtoReader(bytes);
  let raw: Uint8Array<ArrayBufferLike> = new Uint8Array();
  while (!reader.done) {
    const key = reader.key();
    if (key.field === 2) raw = reader.bytesValue(key.wire);
    else reader.skip(key.wire);
  }
  if (raw.byteLength % 8 !== 0) corrupt("Lance row-id u64 array is truncated");
  const view = dataView(raw);
  const values: bigint[] = [];
  for (let offset = 0; offset < raw.byteLength; offset += 8) {
    values.push(view.getBigUint64(offset, true));
  }
  return values;
}

class ProtoReader {
  private offset = 0;

  constructor(private readonly source: Uint8Array) {}

  get done(): boolean {
    return this.offset === this.source.byteLength;
  }

  key(): { field: number; wire: number } {
    const key = this.rawVarint();
    const field = Number(key >> 3n);
    const wire = Number(key & 7n);
    if (field <= 0) corrupt("Invalid protobuf field number", { field });
    return { field, wire };
  }

  varint(wire: number): bigint {
    this.expectWire(wire, 0);
    return this.rawVarint();
  }

  safeInteger(wire: number, label: string): number {
    const value = this.varint(wire);
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      corrupt(`Lance ${label} exceeds JavaScript's safe integer range`, {
        value: value.toString(),
      });
    }
    return Number(value);
  }

  int32(wire: number): number {
    return Number(BigInt.asIntN(32, this.varint(wire)));
  }

  string(wire: number): string {
    const bytes = this.bytesValue(wire);
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch (cause) {
      corrupt("Invalid UTF-8 in Lance protobuf", { cause: errorMessage(cause) });
    }
  }

  message(wire: number): Uint8Array {
    return this.bytesValue(wire);
  }

  bytesValue(wire: number): Uint8Array {
    this.expectWire(wire, 2);
    const length = this.rawVarint();
    if (length > BigInt(Number.MAX_SAFE_INTEGER)) {
      corrupt("Protobuf field length exceeds JavaScript's safe integer range", {
        length: length.toString(),
      });
    }
    const end = this.offset + Number(length);
    if (end > this.source.byteLength) corrupt("Truncated Lance protobuf field");
    const value = this.source.subarray(this.offset, end);
    this.offset = end;
    return value;
  }

  packedSafeIntegers(wire: number, label: string): number[] {
    if (wire === 0) return [this.safeInteger(wire, label)];
    const packed = new ProtoReader(this.bytesValue(wire));
    const values: number[] = [];
    while (!packed.done) values.push(packed.safeInteger(0, label));
    return values;
  }

  packedInt32(wire: number): number[] {
    if (wire === 0) return [this.int32(wire)];
    const packed = new ProtoReader(this.bytesValue(wire));
    const values: number[] = [];
    while (!packed.done) values.push(packed.int32(0));
    return values;
  }

  skip(wire: number): void {
    switch (wire) {
      case 0:
        this.rawVarint();
        return;
      case 1:
        this.advance(8);
        return;
      case 2: {
        const length = this.rawVarint();
        if (length > BigInt(Number.MAX_SAFE_INTEGER)) corrupt("Protobuf field is too large");
        this.advance(Number(length));
        return;
      }
      case 5:
        this.advance(4);
        return;
      default:
        corrupt("Unsupported protobuf wire type", { wire });
    }
  }

  private rawVarint(): bigint {
    let value = 0n;
    for (let shift = 0n; shift < 70n; shift += 7n) {
      if (this.offset >= this.source.byteLength) corrupt("Truncated Lance protobuf varint");
      const byte = this.source[this.offset];
      this.offset += 1;
      if (byte === undefined) corrupt("Truncated Lance protobuf varint");
      value |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return value;
    }
    corrupt("Lance protobuf varint exceeds 64 bits");
  }

  private advance(length: number): void {
    const end = this.offset + length;
    if (!Number.isSafeInteger(end) || end > this.source.byteLength) {
      corrupt("Truncated Lance protobuf field");
    }
    this.offset = end;
  }

  private expectWire(actual: number, expected: number): void {
    if (actual !== expected) corrupt("Unexpected Lance protobuf wire type", { actual, expected });
  }
}

function dataView(bytes: Uint8Array): DataView {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}

function errorMessage(value: unknown): string {
  return value instanceof Error ? value.message : String(value);
}
