import {
  LakeqlError,
  type ObjectHead,
  type Row,
  type TimestampUnit,
  timestampFromEpoch,
} from "lakeql-core";
import type { ByteRange, LanceReadContext, RangeLease } from "./io.js";
import {
  type LanceArrayEncoding,
  type LanceColumnMetadata,
  type LanceDataFile,
  type LanceField,
  type LanceFlatEncoding,
  type LanceFragment,
  type LancePage,
  parseColumnMetadata,
  parseFileDescriptor,
} from "./proto.js";

const DATA_FILE_FOOTER_BYTES = 40;
const MAGIC = "LANC";

export interface InspectedLanceFile {
  path: string;
  fileSize: number;
  fields: LanceField[];
  schemaMetadata: Record<string, Uint8Array>;
  columns: LanceColumnMetadata[];
  rowCount: number;
}

export async function inspectLanceFile(
  context: LanceReadContext,
  path: string,
  knownFileSize?: number,
): Promise<InspectedLanceFile> {
  const fileSize = knownFileSize ?? (await requiredHead(context, path)).size;
  if (fileSize < DATA_FILE_FOOTER_BYTES) {
    corrupt("Lance file is smaller than its footer", { path });
  }
  const footerRange = {
    offset: fileSize - DATA_FILE_FOOTER_BYTES,
    length: DATA_FILE_FOOTER_BYTES,
  };
  const footerLease = await context.readRange(path, footerRange, "file_metadata", fileSize);
  let footer: FileFooter;
  try {
    footer = parseFooter(footerLease.slice(footerRange), fileSize);
  } finally {
    footerLease.release();
  }
  if (footer.numGlobalBuffers < 1) {
    corrupt("Self-describing Lance file has no descriptor buffer", { path });
  }
  const tableRanges: ByteRange[] = [
    { offset: footer.globalBufferOffsets, length: 16 },
    ...Array.from({ length: footer.numColumns }, (_value, index) => ({
      offset: footer.columnMetadataOffsets + index * 16,
      length: 16,
    })),
  ];
  const tableLease = await context.readRanges(path, tableRanges, "file_metadata", fileSize);
  let descriptorRange: ByteRange;
  const columnRanges: ByteRange[] = [];
  try {
    descriptorRange = readTableRange(
      tableLease,
      tableRanges[0] as ByteRange,
      footer.columnMetadataStart,
      path,
    );
    for (let index = 0; index < footer.numColumns; index += 1) {
      columnRanges.push(
        readTableRange(
          tableLease,
          tableRanges[index + 1] as ByteRange,
          footer.columnMetadataOffsets,
          path,
        ),
      );
    }
  } finally {
    tableLease.release();
  }
  const metadataLease = await context.readRanges(
    path,
    [descriptorRange, ...columnRanges],
    "file_metadata",
    fileSize,
  );
  try {
    const descriptor = parseFileDescriptor(metadataLease.slice(descriptorRange));
    const columns = columnRanges.map((range) => parseColumnMetadata(metadataLease.slice(range)));
    if (descriptor.fields.length !== columns.length) {
      corrupt("Self-describing Lance schema and column tables have different lengths", {
        path,
        fields: descriptor.fields.length,
        columns: columns.length,
      });
    }
    const derivedRows = columns[0]?.pages.reduce(
      (maximum, page) => Math.max(maximum, page.priority + page.length),
      0,
    );
    const rowCount = descriptor.length || derivedRows || 0;
    return {
      path,
      fileSize,
      fields: descriptor.fields,
      schemaMetadata: descriptor.metadata,
      columns,
      rowCount,
    };
  } finally {
    metadataLease.release();
  }
}

export async function materializeInspectedLanceFileRows(options: {
  context: LanceReadContext;
  file: InspectedLanceFile;
  selections: readonly { field: LanceField; columnIndex: number }[];
  rowOffsets: readonly number[];
}): Promise<Map<number, Row>> {
  const plans: CellPlan[] = [];
  for (const selection of options.selections) {
    const column = options.file.columns[selection.columnIndex];
    if (column === undefined) {
      corrupt("Selected Lance self-described column is absent", {
        path: options.file.path,
        columnIndex: selection.columnIndex,
      });
    }
    for (const rowOffset of options.rowOffsets) {
      if (rowOffset < 0 || rowOffset >= options.file.rowCount) {
        corrupt("Selected Lance self-described row is out of bounds", {
          path: options.file.path,
          rowOffset,
          rowCount: options.file.rowCount,
        });
      }
      const location = findPage(column.pages, rowOffset);
      options.context.stats.pages.add(
        `${options.file.path}:${selection.columnIndex}:${location.index}`,
      );
      plans.push({
        field: selection.field,
        rowOffset,
        page: location.page,
        rowInPage: rowOffset - location.page.priority,
      });
    }
  }
  return await executeCellPlans(options.context, options.file.path, options.file.fileSize, plans);
}

export async function materializeFragmentRows(options: {
  context: LanceReadContext;
  root: string;
  fragment: LanceFragment;
  fragmentIndex: number;
  fields: readonly LanceField[];
  select: readonly string[];
  rowOffsets: readonly number[];
}): Promise<Map<number, Row>> {
  const { context, fragment, fragmentIndex, rowOffsets } = options;
  context.stats.fragments.add(fragmentIndex);
  const rows = new Map<number, Row>(rowOffsets.map((offset) => [offset, {}]));
  const fieldByName = new Map(options.fields.map((field) => [field.name, field]));
  const fileSelections = new Map<LanceDataFile, { field: LanceField; columnIndex: number }[]>();
  for (const name of options.select) {
    const field = fieldByName.get(name);
    if (field === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown Lance column ${name}`, {
        column: name,
      });
    }
    if (field.parentId !== -1) {
      unsupported("Nested Lance projections are not supported", {
        column: name,
        logicalType: field.logicalType,
      });
    }
    const location = locateField(fragment, field.id);
    if (location === undefined) {
      for (const row of rows.values()) row[name] = null;
      continue;
    }
    const selections = fileSelections.get(location.file) ?? [];
    selections.push({ field, columnIndex: location.columnIndex });
    fileSelections.set(location.file, selections);
  }
  await Promise.all(
    [...fileSelections].map(async ([file, selections]) => {
      const values = await materializeDataFile({
        context,
        root: options.root,
        file,
        selections,
        rowOffsets,
      });
      for (const [rowOffset, valuesByColumn] of values) {
        const row = rows.get(rowOffset);
        if (row === undefined)
          corrupt("Materialized an unexpected Lance row offset", { rowOffset });
        Object.assign(row, valuesByColumn);
      }
    }),
  );
  return rows;
}

async function materializeDataFile(options: {
  context: LanceReadContext;
  root: string;
  file: LanceDataFile;
  selections: readonly { field: LanceField; columnIndex: number }[];
  rowOffsets: readonly number[];
}): Promise<Map<number, Row>> {
  const path = joinObjectPath(options.root, "data", options.file.path);
  const fileSize = await resolveFileSize(options.context, path, options.file);
  const footerLease = await options.context.readRange(
    path,
    { offset: fileSize - DATA_FILE_FOOTER_BYTES, length: DATA_FILE_FOOTER_BYTES },
    "file_metadata",
    fileSize,
  );
  let footer: FileFooter;
  try {
    footer = parseFooter(
      footerLease.slice({
        offset: fileSize - DATA_FILE_FOOTER_BYTES,
        length: DATA_FILE_FOOTER_BYTES,
      }),
      fileSize,
    );
  } finally {
    footerLease.release();
  }
  const uniqueColumns = [
    ...new Set(options.selections.map((selection) => selection.columnIndex)),
  ].sort((left, right) => left - right);
  for (const columnIndex of uniqueColumns) {
    if (columnIndex < 0 || columnIndex >= footer.numColumns) {
      corrupt("Lance manifest references an invalid data-file column", {
        path,
        columnIndex,
        numColumns: footer.numColumns,
      });
    }
  }
  const tableRanges = uniqueColumns.map((columnIndex) => ({
    offset: footer.columnMetadataOffsets + columnIndex * 16,
    length: 16,
  }));
  const tableLease = await options.context.readRanges(path, tableRanges, "file_metadata", fileSize);
  const metadataRanges = new Map<number, ByteRange>();
  try {
    for (const columnIndex of uniqueColumns) {
      const entryRange = {
        offset: footer.columnMetadataOffsets + columnIndex * 16,
        length: 16,
      };
      const entry = dataView(tableLease.slice(entryRange));
      const offset = safeU64(entry.getBigUint64(0, true), "column metadata offset");
      const length = safeU64(entry.getBigUint64(8, true), "column metadata length");
      validateContainedRange({ offset, length }, footer.columnMetadataOffsets, path);
      metadataRanges.set(columnIndex, { offset, length });
    }
  } finally {
    tableLease.release();
  }
  const metadataLease = await options.context.readRanges(
    path,
    [...metadataRanges.values()],
    "file_metadata",
    fileSize,
  );
  const metadata = new Map<number, LanceColumnMetadata>();
  try {
    for (const [columnIndex, range] of metadataRanges) {
      metadata.set(columnIndex, parseColumnMetadata(metadataLease.slice(range)));
    }
  } finally {
    metadataLease.release();
  }
  const cellPlans: CellPlan[] = [];
  for (const selection of options.selections) {
    const column = metadata.get(selection.columnIndex);
    if (column === undefined) corrupt("Missing selected Lance column metadata");
    for (const rowOffset of options.rowOffsets) {
      const pageLocation = findPage(column.pages, rowOffset);
      options.context.stats.pages.add(`${path}:${selection.columnIndex}:${pageLocation.index}`);
      cellPlans.push({
        field: selection.field,
        rowOffset,
        page: pageLocation.page,
        rowInPage: rowOffset - pageLocation.page.priority,
      });
    }
  }
  return await executeCellPlans(options.context, path, fileSize, cellPlans);
}

interface CellPlan {
  field: LanceField;
  rowOffset: number;
  page: LancePage;
  rowInPage: number;
}

interface FixedCellPlan extends CellPlan {
  flat: LanceFlatEncoding;
  validity?: LanceFlatEncoding;
}

interface BinaryCellPlan extends CellPlan {
  indices: LanceFlatEncoding;
  bytes: LanceFlatEncoding;
  nullAdjustment: bigint;
}

async function executeCellPlans(
  context: LanceReadContext,
  path: string,
  fileSize: number,
  cellPlans: readonly CellPlan[],
): Promise<Map<number, Row>> {
  const output = new Map<number, Row>();
  const fixed: FixedCellPlan[] = [];
  const binary: BinaryCellPlan[] = [];
  const phaseOneRanges: ByteRange[] = [];
  for (const plan of cellPlans) {
    const resolved = resolveCellEncoding(plan.page.encoding);
    if (resolved.kind === "all_nulls") {
      assignCell(output, plan.rowOffset, plan.field.name, null);
      continue;
    }
    if (resolved.kind === "fixed") {
      validateFlat(resolved.flat, plan.page, plan.field.logicalType);
      const fixedPlan: FixedCellPlan = {
        ...plan,
        flat: resolved.flat,
        ...(resolved.validity === undefined ? {} : { validity: resolved.validity }),
      };
      fixed.push(fixedPlan);
      phaseOneRanges.push(flatRange(plan.page, resolved.flat, plan.rowInPage));
      if (resolved.validity !== undefined) {
        validateValidity(resolved.validity, plan.page);
        phaseOneRanges.push(flatRange(plan.page, resolved.validity, plan.rowInPage));
      }
      continue;
    }
    validateFlat(resolved.indices, plan.page, "uint64");
    validateFlat(resolved.bytes, plan.page, "uint8");
    if (resolved.indices.bitsPerValue !== 64 || resolved.bytes.bitsPerValue !== 8) {
      unsupported("Unsupported Lance 2.0 binary physical encoding", {
        indexBits: resolved.indices.bitsPerValue,
        valueBits: resolved.bytes.bitsPerValue,
      });
    }
    const binaryPlan: BinaryCellPlan = {
      ...plan,
      indices: resolved.indices,
      bytes: resolved.bytes,
      nullAdjustment: resolved.nullAdjustment,
    };
    binary.push(binaryPlan);
    phaseOneRanges.push(flatRange(plan.page, resolved.indices, plan.rowInPage));
    if (plan.rowInPage > 0) {
      phaseOneRanges.push(flatRange(plan.page, resolved.indices, plan.rowInPage - 1));
    }
  }
  const phaseOne = await context.readRanges(path, phaseOneRanges, "data", fileSize);
  const binaryValues: {
    plan: BinaryCellPlan;
    range?: ByteRange;
    isNull: boolean;
  }[] = [];
  try {
    for (const plan of fixed) {
      if (
        plan.validity !== undefined &&
        !readBit(phaseOne, flatRange(plan.page, plan.validity, plan.rowInPage), plan.rowInPage)
      ) {
        assignCell(output, plan.rowOffset, plan.field.name, null);
        continue;
      }
      const range = flatRange(plan.page, plan.flat, plan.rowInPage);
      assignCell(
        output,
        plan.rowOffset,
        plan.field.name,
        decodeFixedValue(
          phaseOne.slice(range),
          plan.flat.bitsPerValue,
          plan.rowInPage,
          plan.field.logicalType,
        ),
      );
    }
    for (const plan of binary) {
      const endRaw = readFlatU64(phaseOne, plan.page, plan.indices, plan.rowInPage);
      const isNull = plan.nullAdjustment > 0n && endRaw >= plan.nullAdjustment;
      const end = plan.nullAdjustment > 0n ? endRaw % plan.nullAdjustment : endRaw;
      const start =
        plan.rowInPage === 0
          ? 0n
          : binaryOffset(
              readFlatU64(phaseOne, plan.page, plan.indices, plan.rowInPage - 1),
              plan.nullAdjustment,
            );
      if (end < start) corrupt("Lance binary offsets are not monotonic", { path });
      const valueBuffer = pageBuffer(plan.page, plan.bytes);
      const startNumber = safeU64(start, "binary value start");
      const length = safeU64(end - start, "binary value length");
      if (startNumber + length > valueBuffer.length) {
        corrupt("Lance binary value range exceeds its page buffer", {
          start: startNumber,
          length,
          bufferLength: valueBuffer.length,
        });
      }
      binaryValues.push({
        plan,
        isNull,
        ...(isNull || length === 0
          ? {}
          : { range: { offset: valueBuffer.offset + startNumber, length } }),
      });
    }
  } finally {
    phaseOne.release();
  }
  const valueRanges = binaryValues.flatMap((value) =>
    value.range === undefined ? [] : [value.range],
  );
  const phaseTwo =
    valueRanges.length === 0
      ? undefined
      : await context.readRanges(path, valueRanges, "data", fileSize);
  try {
    for (const value of binaryValues) {
      if (value.isNull) {
        assignCell(output, value.plan.rowOffset, value.plan.field.name, null);
      } else {
        const bytes = value.range === undefined ? new Uint8Array() : phaseTwo?.slice(value.range);
        if (bytes === undefined) corrupt("Missing Lance binary value bytes");
        const decoded =
          value.plan.field.logicalType === "string" ||
          value.plan.field.logicalType === "large_string"
            ? decodeUtf8(bytes)
            : copyBytes(bytes);
        context.accountDecodedMemory(
          typeof decoded === "string"
            ? new TextEncoder().encode(decoded).byteLength
            : decoded.byteLength,
        );
        assignCell(output, value.plan.rowOffset, value.plan.field.name, decoded);
      }
    }
  } finally {
    phaseTwo?.release();
  }
  return output;
}

type ResolvedCellEncoding =
  | { kind: "all_nulls" }
  | { kind: "fixed"; flat: LanceFlatEncoding; validity?: LanceFlatEncoding }
  | {
      kind: "binary";
      indices: LanceFlatEncoding;
      bytes: LanceFlatEncoding;
      nullAdjustment: bigint;
    };

function resolveCellEncoding(
  encoding: LanceArrayEncoding,
  validity?: LanceFlatEncoding,
): ResolvedCellEncoding {
  if (encoding.kind === "nullable") {
    if (encoding.mode === "all_nulls") return { kind: "all_nulls" };
    if (encoding.mode === "no_nulls") return resolveCellEncoding(encoding.values, validity);
    const validityFlat = requireFlat(encoding.validity, "nullable validity");
    return resolveCellEncoding(encoding.values, validityFlat);
  }
  if (encoding.kind === "flat") {
    return {
      kind: "fixed",
      flat: encoding,
      ...(validity === undefined ? {} : { validity }),
    };
  }
  if (encoding.kind === "binary") {
    if (validity !== undefined) {
      unsupported("Outer nullable wrappers on Lance binary arrays are not supported");
    }
    return {
      kind: "binary",
      indices: requireFlat(unwrapNoNulls(encoding.indices), "binary offsets"),
      bytes: requireFlat(encoding.bytes, "binary values"),
      nullAdjustment: encoding.nullAdjustment,
    };
  }
  unsupported("Lance constant encodings are not supported for projected row reads");
}

function unwrapNoNulls(encoding: LanceArrayEncoding): LanceArrayEncoding {
  if (encoding.kind !== "nullable") return encoding;
  if (encoding.mode !== "no_nulls") {
    unsupported("Unsupported nullable encoding for Lance binary offsets");
  }
  return encoding.values;
}

function requireFlat(encoding: LanceArrayEncoding, role: string): LanceFlatEncoding {
  if (encoding.kind !== "flat") {
    unsupported(`Lance ${role} must use flat encoding`, { encoding: encoding.kind });
  }
  return encoding;
}

function validateFlat(flat: LanceFlatEncoding, page: LancePage, logicalType: string): void {
  if (flat.bufferType !== 0) {
    unsupported("Lance column/file metadata buffers are not supported for flat values", {
      bufferType: flat.bufferType,
    });
  }
  if (flat.compression !== undefined) {
    unsupported("Compressed Lance 2.0 flat buffers are not supported", {
      compression: flat.compression,
    });
  }
  const expected = bitsForLogicalType(logicalType);
  if (expected !== undefined && flat.bitsPerValue !== expected) {
    unsupported("Lance physical width does not match the projected logical type", {
      logicalType,
      expectedBits: expected,
      actualBits: flat.bitsPerValue,
    });
  }
  pageBuffer(page, flat);
}

function validateValidity(flat: LanceFlatEncoding, page: LancePage): void {
  if (flat.bitsPerValue !== 1) {
    unsupported("Lance validity buffers must use one bit per value", {
      bitsPerValue: flat.bitsPerValue,
    });
  }
  validateFlat(flat, page, "bool");
}

function flatRange(page: LancePage, flat: LanceFlatEncoding, rowInPage: number): ByteRange {
  const buffer = pageBuffer(page, flat);
  const bitOffset = rowInPage * flat.bitsPerValue;
  if (!Number.isSafeInteger(bitOffset)) corrupt("Lance flat value offset is too large");
  const firstByte = Math.floor(bitOffset / 8);
  const lastByte = Math.ceil((bitOffset + flat.bitsPerValue) / 8);
  if (lastByte > buffer.length) {
    corrupt("Lance flat value exceeds its page buffer", {
      rowInPage,
      bitsPerValue: flat.bitsPerValue,
      bufferLength: buffer.length,
    });
  }
  return { offset: buffer.offset + firstByte, length: lastByte - firstByte };
}

function pageBuffer(page: LancePage, flat: LanceFlatEncoding): ByteRange {
  const offset = page.bufferOffsets[flat.bufferIndex];
  const length = page.bufferSizes[flat.bufferIndex];
  if (offset === undefined || length === undefined) {
    corrupt("Lance encoding references a missing page buffer", {
      bufferIndex: flat.bufferIndex,
      bufferCount: page.bufferOffsets.length,
    });
  }
  return { offset, length };
}

function readFlatU64(
  lease: RangeLease,
  page: LancePage,
  flat: LanceFlatEncoding,
  rowInPage: number,
): bigint {
  const range = flatRange(page, flat, rowInPage);
  return dataView(lease.slice(range)).getBigUint64(0, true);
}

function readBit(lease: RangeLease, range: ByteRange, rowInPage: number): boolean {
  const byte = lease.slice(range)[0] ?? 0;
  return (byte & (1 << (rowInPage % 8))) !== 0;
}

function decodeFixedValue(
  bytes: Uint8Array,
  bitsPerValue: number,
  rowInPage: number,
  logicalType: string,
): unknown {
  if (logicalType === "bool") {
    return ((bytes[0] ?? 0) & (1 << (rowInPage % 8))) !== 0;
  }
  const view = dataView(bytes);
  const temporal = parseTemporalType(logicalType);
  if (temporal?.kind === "date32") {
    return new Date(view.getInt32(0, true) * 86_400_000);
  }
  if (temporal?.kind === "timestamp") {
    const raw = view.getBigInt64(0, true);
    const value = temporal.unit === "seconds" ? raw * 1_000n : raw;
    const unit: TimestampUnit = temporal.unit === "seconds" ? "millis" : temporal.unit;
    return timestampFromEpoch(value, unit, temporal.isAdjustedToUTC);
  }
  switch (logicalType) {
    case "int8":
      return view.getInt8(0);
    case "uint8":
      return view.getUint8(0);
    case "int16":
      return view.getInt16(0, true);
    case "uint16":
      return view.getUint16(0, true);
    case "int32":
      return view.getInt32(0, true);
    case "uint32":
      return view.getUint32(0, true);
    case "int64":
      return numberOrBigInt(view.getBigInt64(0, true));
    case "uint64":
      return numberOrBigInt(view.getBigUint64(0, true));
    case "float":
      return view.getFloat32(0, true);
    case "double":
      return view.getFloat64(0, true);
    default:
      unsupported("Unsupported projected Lance scalar type", {
        logicalType,
        bitsPerValue,
      });
  }
}

function bitsForLogicalType(logicalType: string): number | undefined {
  const temporal = parseTemporalType(logicalType);
  if (temporal?.kind === "date32") return 32;
  if (temporal?.kind === "timestamp") return 64;
  switch (logicalType) {
    case "bool":
      return 1;
    case "int8":
    case "uint8":
      return 8;
    case "int16":
    case "uint16":
      return 16;
    case "int32":
    case "uint32":
    case "float":
      return 32;
    case "int64":
    case "uint64":
    case "double":
      return 64;
    case "string":
    case "large_string":
    case "binary":
    case "large_binary":
      return undefined;
    default:
      unsupported("Unsupported projected Lance logical type", { logicalType });
  }
}

function parseTemporalType(logicalType: string):
  | { kind: "date32" }
  | {
      kind: "timestamp";
      unit: "seconds" | TimestampUnit;
      isAdjustedToUTC: boolean;
    }
  | undefined {
  if (logicalType === "date32:day") return { kind: "date32" };
  const match = /^timestamp:(s|ms|us|ns):(.+)$/u.exec(logicalType);
  if (match === null) return undefined;
  const unit = match[1];
  return {
    kind: "timestamp",
    unit: unit === "s" ? "seconds" : unit === "ms" ? "millis" : unit === "us" ? "micros" : "nanos",
    isAdjustedToUTC: match[2] !== "-",
  };
}

function binaryOffset(value: bigint, nullAdjustment: bigint): bigint {
  return nullAdjustment > 0n ? value % nullAdjustment : value;
}

function findPage(
  pages: readonly LancePage[],
  rowOffset: number,
): { page: LancePage; index: number } {
  for (const [index, page] of pages.entries()) {
    if (rowOffset >= page.priority && rowOffset < page.priority + page.length) {
      return { page, index };
    }
  }
  corrupt("Lance row offset is not covered by a selected column page", { rowOffset });
}

function locateField(
  fragment: LanceFragment,
  fieldId: number,
): { file: LanceDataFile; columnIndex: number } | undefined {
  let found: { file: LanceDataFile; columnIndex: number } | undefined;
  for (const file of fragment.files) {
    if (file.fields.length !== file.columnIndices.length) {
      corrupt("Lance data-file field and column-index tables have different lengths", {
        path: file.path,
      });
    }
    for (const [position, candidate] of file.fields.entries()) {
      if (candidate !== fieldId) continue;
      const columnIndex = file.columnIndices[position];
      if (columnIndex === undefined || columnIndex < 0) continue;
      if (found !== undefined) {
        corrupt("Lance field is supplied by more than one active data file", { fieldId });
      }
      found = { file, columnIndex };
    }
  }
  return found;
}

interface FileFooter {
  columnMetadataStart: number;
  columnMetadataOffsets: number;
  globalBufferOffsets: number;
  numGlobalBuffers: number;
  numColumns: number;
  majorVersion: number;
  minorVersion: number;
}

function parseFooter(bytes: Uint8Array, fileSize: number): FileFooter {
  if (bytes.byteLength !== DATA_FILE_FOOTER_BYTES) corrupt("Truncated Lance data-file footer");
  const view = dataView(bytes);
  const footer = {
    columnMetadataStart: safeU64(view.getBigUint64(0, true), "column metadata start"),
    columnMetadataOffsets: safeU64(view.getBigUint64(8, true), "column metadata table"),
    globalBufferOffsets: safeU64(view.getBigUint64(16, true), "global buffer table"),
    numGlobalBuffers: view.getUint32(24, true),
    numColumns: view.getUint32(28, true),
    majorVersion: view.getUint16(32, true),
    minorVersion: view.getUint16(34, true),
  };
  if (decodeAscii(bytes.subarray(36, 40)) !== MAGIC) corrupt("Invalid Lance data-file magic");
  if (footer.majorVersion !== 0 || footer.minorVersion !== 3) {
    unsupported("Unsupported Lance v2 container version", {
      majorVersion: footer.majorVersion,
      minorVersion: footer.minorVersion,
    });
  }
  const footerOffset = fileSize - DATA_FILE_FOOTER_BYTES;
  if (
    footer.columnMetadataStart > footer.columnMetadataOffsets ||
    footer.columnMetadataOffsets + footer.numColumns * 16 > footer.globalBufferOffsets ||
    footer.globalBufferOffsets + footer.numGlobalBuffers * 16 > footerOffset
  ) {
    corrupt("Lance data-file footer contains out-of-bounds metadata tables");
  }
  return footer;
}

async function resolveFileSize(
  context: LanceReadContext,
  path: string,
  file: LanceDataFile,
): Promise<number> {
  if (file.fileMajorVersion !== 2 || file.fileMinorVersion !== 0) {
    unsupported("Only Lance data storage version 2.0 files are supported", {
      path,
      fileMajorVersion: file.fileMajorVersion,
      fileMinorVersion: file.fileMinorVersion,
    });
  }
  const size = file.fileSizeBytes || (await requiredHead(context, path)).size;
  if (size < DATA_FILE_FOOTER_BYTES)
    corrupt("Lance data file is smaller than its footer", { path });
  return size;
}

async function requiredHead(context: LanceReadContext, path: string): Promise<ObjectHead> {
  context.check();
  const head = await context.store.head(path);
  context.check();
  if (head === null) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Missing Lance object ${path}`, { path });
  }
  return head;
}

function validateContainedRange(range: ByteRange, end: number, path: string): void {
  if (
    range.offset < 0 ||
    range.length < 0 ||
    !Number.isSafeInteger(range.offset + range.length) ||
    range.offset + range.length > end
  ) {
    corrupt("Lance column metadata range is outside the metadata section", {
      path,
      range,
      metadataTableOffset: end,
    });
  }
}

function readTableRange(
  lease: RangeLease,
  tableEntry: ByteRange,
  sectionEnd: number,
  path: string,
): ByteRange {
  const entry = dataView(lease.slice(tableEntry));
  const range = {
    offset: safeU64(entry.getBigUint64(0, true), "metadata offset"),
    length: safeU64(entry.getBigUint64(8, true), "metadata length"),
  };
  validateContainedRange(range, sectionEnd, path);
  return range;
}

function assignCell(
  output: Map<number, Row>,
  rowOffset: number,
  column: string,
  value: unknown,
): void {
  const row = output.get(rowOffset) ?? {};
  row[column] = value;
  output.set(rowOffset, row);
}

function joinObjectPath(...parts: string[]): string {
  return parts
    .flatMap((part) => part.split("/"))
    .filter((part) => part.length > 0)
    .join("/");
}

function numberOrBigInt(value: bigint): number | bigint {
  return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(value)
    : value;
}

function safeU64(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    corrupt(`Lance ${label} exceeds JavaScript's safe integer range`, {
      value: value.toString(),
    });
  }
  return Number(value);
}

function dataView(bytes: Uint8Array): DataView {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function decodeAscii(bytes: Uint8Array): string {
  return String.fromCharCode(...bytes);
}

function decodeUtf8(bytes: Uint8Array): string {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (cause) {
    corrupt("Invalid UTF-8 in Lance string value", {
      cause: cause instanceof Error ? cause.message : String(cause),
    });
  }
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}
