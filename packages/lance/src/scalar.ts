import { LakeqlError, type QueryBudget, type Row, type SharedMemoryCache } from "lakeql";
import {
  type InspectedLanceFile,
  inspectLanceFile,
  materializeInspectedLanceFileRows,
} from "./file.js";
import type { LanceReadContext } from "./io.js";
import {
  type LanceField,
  type LanceIndexMetadata,
  type LanceManifest,
  parseIndexSection,
} from "./proto.js";

export type LanceScalarValue = string | number | bigint | boolean;

export interface LanceScalarIndexInfo {
  name: string;
  uuid: string;
  column: string;
  indexVersion?: number;
}

export interface LanceScalarMatch {
  value: LanceScalarValue;
  rowIds: bigint[];
}

export interface LanceScalarRange {
  lower?: LanceScalarValue;
  lowerInclusive?: boolean;
  upper?: LanceScalarValue;
  upperInclusive?: boolean;
}

export async function loadScalarIndexes(options: {
  context: LanceReadContext;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  indexMetadata?: readonly LanceIndexMetadata[];
}): Promise<{ metadata: LanceIndexMetadata; info: LanceScalarIndexInfo }[]> {
  const indices = options.indexMetadata ?? (await loadLanceIndexMetadata(options));
  const fieldById = new Map(options.manifest.fields.map((field) => [field.id, field]));
  return indices.flatMap((metadata) => {
    if (metadata.detailsTypeUrl.toLowerCase() !== "/lance.table.btreeindexdetails") return [];
    if (metadata.fields.length !== 1) {
      corrupt("Lance BTree index must address exactly one field", {
        index: metadata.name,
        fields: metadata.fields,
      });
    }
    const field = fieldById.get(metadata.fields[0] as number);
    if (field === undefined) {
      corrupt("Lance BTree index references an unknown field", {
        index: metadata.name,
      });
    }
    return [
      {
        metadata,
        info: {
          name: metadata.name,
          uuid: metadata.uuid,
          column: field.name,
          ...(metadata.indexVersion === undefined ? {} : { indexVersion: metadata.indexVersion }),
        },
      },
    ];
  });
}

export async function loadLanceIndexMetadata(options: {
  context: LanceReadContext;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
}): Promise<LanceIndexMetadata[]> {
  const offset = options.manifest.indexSectionOffset;
  if (offset === undefined) return [];
  if (offset < 0 || offset + 4 > options.manifestFileSize) {
    corrupt("Lance manifest index-section offset is out of bounds", { offset });
  }
  const lengthLease = await options.context.readRange(
    options.manifestPath,
    { offset, length: 4 },
    "file_metadata",
    options.manifestFileSize,
  );
  let length: number;
  try {
    length = dataView(lengthLease.slice({ offset, length: 4 })).getUint32(0, true);
  } finally {
    lengthLease.release();
  }
  if (length <= 0 || offset + 4 + length > options.manifestFileSize) {
    corrupt("Lance manifest index section is out of bounds", { offset, length });
  }
  const range = { offset: offset + 4, length };
  const sectionLease = await options.context.readRange(
    options.manifestPath,
    range,
    "file_metadata",
    options.manifestFileSize,
  );
  let indices: LanceIndexMetadata[];
  try {
    indices = parseIndexSection(sectionLease.slice(range));
  } finally {
    sectionLease.release();
  }
  return indices;
}

export async function lookupScalarRowIds(options: {
  context: LanceReadContext;
  root: string;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  indexMetadata?: readonly LanceIndexMetadata[];
  indexCache?: SharedMemoryCache;
  cacheNamespace?: string;
  indexName: string;
  values: readonly LanceScalarValue[];
  budget: QueryBudget;
}): Promise<{ index: LanceScalarIndexInfo; matches: LanceScalarMatch[] }> {
  if (options.values.length === 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Scalar lookup values must not be empty");
  }
  const opened = await openBTreeIndex(options);
  const values = options.values.map((value) => validatedScalarValue(value, opened.field));
  const candidatePages = values.map((value) => {
    const pages: number[] = [];
    for (const row of opened.lookupRows.values()) {
      const minimum = row.min;
      const maximum = row.max;
      const page = row.page_idx;
      if (
        minimum !== null &&
        maximum !== null &&
        typeof page === "number" &&
        compareScalar(minimum as LanceScalarValue, value) <= 0 &&
        compareScalar(maximum as LanceScalarValue, value) >= 0
      ) {
        pages.push(page);
      }
    }
    return pages;
  });
  const pageRows = await materializeCandidatePages(
    options.context,
    opened.pageData,
    opened.batchSize,
    candidatePages.flat(),
    options.indexCache,
    options.cacheNamespace,
    opened.index.uuid,
  );
  const rowOffsetsByValue = values.map(() => [] as number[]);
  for (const [valueIndex, pages] of candidatePages.entries()) {
    const value = values[valueIndex] as LanceScalarValue;
    for (const page of pages) {
      const low = page * opened.batchSize;
      const high = Math.min(opened.pageData.rowCount, low + opened.batchSize);
      const lower = localBound(pageRows, value, "lower", low, high);
      const upper = localBound(pageRows, value, "upper", lower, high);
      for (let offset = lower; offset < upper; offset += 1) {
        rowOffsetsByValue[valueIndex]?.push(offset);
      }
    }
  }
  enforceOutputRows(options.budget, rowOffsetsByValue.flat().length);
  const matches = rowOffsetsByValue.map((offsets, valueIndex) => ({
    value: options.values[valueIndex] as LanceScalarValue,
    rowIds: stableIdsAtOffsets(pageRows, offsets),
  }));
  return { index: opened.index, matches };
}

export async function rangeScalarRowIds(options: {
  context: LanceReadContext;
  root: string;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  indexMetadata?: readonly LanceIndexMetadata[];
  indexCache?: SharedMemoryCache;
  cacheNamespace?: string;
  indexName: string;
  range: LanceScalarRange;
  budget: QueryBudget;
}): Promise<{ index: LanceScalarIndexInfo; rowIds: bigint[] }> {
  if (options.range.lower === undefined && options.range.upper === undefined) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "A Lance scalar range requires a lower or upper bound",
    );
  }
  const opened = await openBTreeIndex(options);
  const lower =
    options.range.lower === undefined
      ? undefined
      : validatedScalarValue(options.range.lower, opened.field);
  const upper =
    options.range.upper === undefined
      ? undefined
      : validatedScalarValue(options.range.upper, opened.field);
  if (lower !== undefined && upper !== undefined && compareScalar(lower, upper) > 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Lance scalar range lower bound exceeds its upper bound",
    );
  }
  const values: LanceScalarValue[] = [];
  const searches: SearchState[] = [];
  if (lower !== undefined) {
    const valueIndex = values.push(lower) - 1;
    searches.push({
      valueIndex,
      page: -1,
      mode: options.range.lowerInclusive === false ? "upper" : "lower",
      low: 0,
      high: opened.pageData.rowCount,
    });
  }
  if (upper !== undefined) {
    const valueIndex = values.push(upper) - 1;
    searches.push({
      valueIndex,
      page: -1,
      mode: options.range.upperInclusive === false ? "lower" : "upper",
      low: 0,
      high: opened.pageData.rowCount,
    });
  }
  await runBinarySearch(options.context, opened.pageData, values, searches);
  const start = lower === undefined ? 0 : (searches[0]?.low ?? 0);
  const end = upper === undefined ? opened.pageData.rowCount : (searches.at(-1)?.low ?? 0);
  const offsets =
    end <= start ? [] : Array.from({ length: end - start }, (_value, index) => start + index);
  enforceOutputRows(options.budget, offsets.length);
  const idRows = await materializeStableIds(options.context, opened.pageData, [offsets]);
  return { index: opened.index, rowIds: stableIdsAtOffsets(idRows, offsets) };
}

interface OpenedBTreeIndex {
  index: LanceScalarIndexInfo;
  field: LanceField;
  batchSize: number;
  pageData: InspectedLanceFile;
  lookupRows: Map<number, Row>;
}

async function openBTreeIndex(options: {
  context: LanceReadContext;
  root: string;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  indexMetadata?: readonly LanceIndexMetadata[];
  indexCache?: SharedMemoryCache;
  cacheNamespace?: string;
  indexName: string;
}): Promise<OpenedBTreeIndex> {
  const indices = await loadScalarIndexes(options);
  const selected = indices.find(({ info }) => info.name === options.indexName);
  if (selected === undefined) {
    throw new LakeqlError(
      "LAKEQL_OBJECT_NOT_FOUND",
      `Lance BTree index ${options.indexName} does not exist`,
      { index: options.indexName },
    );
  }
  if (selected.metadata.indexVersion !== undefined && selected.metadata.indexVersion !== 0) {
    unsupported("Unsupported Lance BTree index version", {
      index: selected.info.name,
      indexVersion: selected.metadata.indexVersion,
    });
  }
  const indexCache = options.indexCache;
  const cacheKey =
    indexCache === undefined || options.cacheNamespace === undefined
      ? undefined
      : `${options.cacheNamespace}:btree:${selected.info.uuid}:state`;
  const cached = cacheKey === undefined ? undefined : indexCache?.get<OpenedBTreeIndex>(cacheKey);
  if (cached !== undefined) {
    options.context.stats.cacheHits += 1;
    return cached.value;
  }
  if (cacheKey !== undefined) options.context.stats.cacheMisses += 1;
  const field = options.manifest.fields.find(
    (candidate) => candidate.id === selected.metadata.fields[0],
  );
  if (field === undefined) corrupt("Selected Lance scalar-index field is absent");
  const fileByName = new Map(selected.metadata.files.map((file) => [file.path, file]));
  const lookupMetadata = fileByName.get("page_lookup.lance");
  const pageMetadata = fileByName.get("page_data.lance");
  if (lookupMetadata === undefined || pageMetadata === undefined) {
    corrupt("Lance BTree index is missing required files", {
      index: selected.info.name,
    });
  }
  const indexRoot = joinObjectPath(options.root, "_indices", selected.metadata.uuid);
  const lookup = await inspectLanceFile(
    options.context,
    joinObjectPath(indexRoot, lookupMetadata.path),
    lookupMetadata.sizeBytes || undefined,
  );
  validateLookupFile(lookup, field);
  const pageData = await inspectLanceFile(
    options.context,
    joinObjectPath(indexRoot, pageMetadata.path),
    pageMetadata.sizeBytes || undefined,
  );
  validatePageDataFile(pageData, field);
  const batchSize = decodeBatchSize(lookup.schemaMetadata.batch_size);
  options.context.reserveDecodedRows(lookup.rowCount);
  const lookupRows = await materializeInspectedLanceFileRows({
    context: options.context,
    file: lookup,
    selections: lookup.fields.map((candidate, columnIndex) => ({
      field: candidate,
      columnIndex,
    })),
    rowOffsets: Array.from({ length: lookup.rowCount }, (_value, index) => index),
  });
  const opened = { index: selected.info, field, batchSize, pageData, lookupRows };
  if (cacheKey !== undefined) {
    indexCache?.set(cacheKey, opened, estimateOpenedIndexBytes(opened), { priority: 4 });
  }
  return opened;
}

function enforceOutputRows(budget: QueryBudget, count: number): void {
  if (budget.maxOutputRows !== undefined && count > budget.maxOutputRows) {
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      `Lance scalar lookup exceeded output row budget (${count} > ${budget.maxOutputRows})`,
      {
        metric: "output rows",
        limit: budget.maxOutputRows,
        actual: count,
      },
    );
  }
}

async function materializeStableIds(
  context: LanceReadContext,
  pageData: InspectedLanceFile,
  groups: readonly number[][],
): Promise<Map<number, Row>> {
  const uniqueOffsets = [...new Set(groups.flat())];
  return uniqueOffsets.length === 0
    ? new Map<number, Row>()
    : await materializeRowsWithBudget({
        context,
        file: pageData,
        selections: [{ field: pageData.fields[1] as LanceField, columnIndex: 1 }],
        rowOffsets: uniqueOffsets,
      });
}

async function materializeCandidatePages(
  context: LanceReadContext,
  pageData: InspectedLanceFile,
  batchSize: number,
  pages: readonly number[],
  indexCache?: SharedMemoryCache,
  cacheNamespace?: string,
  indexUuid?: string,
): Promise<Map<number, Row>> {
  const uniquePages = [...new Set(pages)].sort((left, right) => left - right);
  const output = new Map<number, Row>();
  const missingPages: number[] = [];
  for (const page of uniquePages) {
    const cacheKey =
      indexCache === undefined || cacheNamespace === undefined || indexUuid === undefined
        ? undefined
        : `${cacheNamespace}:btree:${indexUuid}:page:${page}`;
    const cached = cacheKey === undefined ? undefined : indexCache?.get<Map<number, Row>>(cacheKey);
    if (cached === undefined) {
      missingPages.push(page);
      if (cacheKey !== undefined) context.stats.cacheMisses += 1;
      continue;
    }
    context.stats.cacheHits += 1;
    const firstRow = page * batchSize;
    for (const [columnIndex, column] of pageData.columns.entries()) {
      const physicalPage = column.pages.findIndex(
        (candidate) =>
          firstRow >= candidate.rowStart && firstRow < candidate.rowStart + candidate.length,
      );
      if (physicalPage >= 0) {
        context.stats.pages.add(`${pageData.path}:${columnIndex}:${physicalPage}`);
      }
    }
    for (const [offset, row] of cached.value) output.set(offset, row);
  }
  const rowOffsets = missingPages.flatMap((page) => {
    const low = page * batchSize;
    const high = Math.min(pageData.rowCount, low + batchSize);
    if (low < 0 || low >= high) {
      corrupt("Lance BTree lookup references an invalid page", {
        page,
        batchSize,
        rowCount: pageData.rowCount,
      });
    }
    return Array.from({ length: high - low }, (_value, index) => low + index);
  });
  if (rowOffsets.length > 0) {
    const loaded = await materializeRowsWithBudget({
      context,
      file: pageData,
      selections: pageData.fields.map((field, columnIndex) => ({ field, columnIndex })),
      rowOffsets,
    });
    for (const [offset, row] of loaded) output.set(offset, row);
    for (const page of missingPages) {
      if (indexCache === undefined || cacheNamespace === undefined || indexUuid === undefined) {
        continue;
      }
      const low = page * batchSize;
      const high = Math.min(pageData.rowCount, low + batchSize);
      const rows = new Map<number, Row>();
      for (let offset = low; offset < high; offset += 1) {
        const row = loaded.get(offset);
        if (row !== undefined) rows.set(offset, row);
      }
      indexCache.set(
        `${cacheNamespace}:btree:${indexUuid}:page:${page}`,
        rows,
        estimateRowsBytes(rows),
        { priority: 3 },
      );
    }
  }
  return output;
}

function estimateOpenedIndexBytes(opened: OpenedBTreeIndex): number {
  return (
    estimateRowsBytes(opened.lookupRows) +
    opened.pageData.fields.reduce(
      (bytes, field) => bytes + field.name.length * 2 + field.logicalType.length * 2 + 32,
      0,
    ) +
    opened.pageData.columns.reduce((bytes, column) => bytes + column.pages.length * 128, 0)
  );
}

function estimateRowsBytes(rows: ReadonlyMap<number, Row>): number {
  let bytes = rows.size * 32;
  for (const row of rows.values()) {
    for (const value of Object.values(row)) {
      if (typeof value === "string") bytes += value.length * 2;
      else if (typeof value === "number" || typeof value === "bigint") bytes += 8;
      else if (typeof value === "boolean") bytes += 1;
      else if (value instanceof Uint8Array) bytes += value.byteLength;
    }
  }
  return bytes;
}

function localBound(
  rows: ReadonlyMap<number, Row>,
  value: LanceScalarValue,
  mode: "lower" | "upper",
  initialLow: number,
  initialHigh: number,
): number {
  let low = initialLow;
  let high = initialHigh;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    const candidate = rows.get(middle)?.values;
    if (candidate === undefined || candidate === null) {
      low = middle + 1;
      continue;
    }
    const comparison = compareScalar(candidate as LanceScalarValue, value);
    if (comparison < 0 || (mode === "upper" && comparison === 0)) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
}

function stableIdsAtOffsets(idRows: Map<number, Row>, offsets: readonly number[]): bigint[] {
  return offsets.map((offset) => {
    const id = idRows.get(offset)?.ids;
    if (typeof id !== "number" && typeof id !== "bigint") {
      corrupt("Lance BTree page contains an invalid stable row ID", { offset });
    }
    return BigInt(id);
  });
}

interface SearchState {
  valueIndex: number;
  page: number;
  mode: "lower" | "upper";
  low: number;
  high: number;
}

async function runBinarySearch(
  context: LanceReadContext,
  pageData: InspectedLanceFile,
  values: readonly LanceScalarValue[],
  searches: SearchState[],
): Promise<void> {
  while (searches.some((search) => search.low < search.high)) {
    const active = searches.filter((search) => search.low < search.high);
    const middleBySearch = new Map<SearchState, number>(
      active.map((search) => [search, search.low + Math.floor((search.high - search.low) / 2)]),
    );
    const offsets = [...new Set(middleBySearch.values())];
    const rows = await materializeRowsWithBudget({
      context,
      file: pageData,
      selections: [{ field: pageData.fields[0] as LanceField, columnIndex: 0 }],
      rowOffsets: offsets,
    });
    for (const search of active) {
      const middle = middleBySearch.get(search) as number;
      const candidate = rows.get(middle)?.values;
      if (candidate === undefined || candidate === null) {
        search.low = middle + 1;
        continue;
      }
      const comparison = compareScalar(
        candidate as LanceScalarValue,
        values[search.valueIndex] as LanceScalarValue,
      );
      if (comparison < 0 || (search.mode === "upper" && comparison === 0)) {
        search.low = middle + 1;
      } else {
        search.high = middle;
      }
    }
  }
}

async function materializeRowsWithBudget(
  options: Parameters<typeof materializeInspectedLanceFileRows>[0],
): Promise<Map<number, Row>> {
  options.context.reserveDecodedRows(options.rowOffsets.length);
  return await materializeInspectedLanceFileRows(options);
}

function validateLookupFile(file: InspectedLanceFile, field: LanceField): void {
  const expected = [
    ["min", field.logicalType],
    ["max", field.logicalType],
    ["null_count", "uint32"],
    ["page_idx", "uint32"],
  ];
  if (
    file.fields.length !== expected.length ||
    expected.some(
      ([name, type], index) =>
        file.fields[index]?.name !== name || file.fields[index]?.logicalType !== type,
    )
  ) {
    corrupt("Unsupported Lance BTree lookup schema");
  }
}

function validatePageDataFile(file: InspectedLanceFile, field: LanceField): void {
  if (
    file.fields.length !== 2 ||
    file.fields[0]?.name !== "values" ||
    file.fields[0]?.logicalType !== field.logicalType ||
    file.fields[1]?.name !== "ids" ||
    file.fields[1]?.logicalType !== "uint64"
  ) {
    corrupt("Unsupported Lance BTree page-data schema");
  }
}

function decodeBatchSize(bytes: Uint8Array | undefined): number {
  if (bytes === undefined) corrupt("Lance BTree lookup has no batch-size metadata");
  const value = Number(decodeUtf8(bytes));
  if (!Number.isSafeInteger(value) || value <= 0) {
    corrupt("Invalid Lance BTree batch size");
  }
  return value;
}

function validatedScalarValue(value: LanceScalarValue, field: LanceField): LanceScalarValue {
  const type = field.logicalType;
  if (type === "string" || type === "large_string") {
    if (typeof value !== "string") invalidScalar(field, value);
    return value;
  }
  if (type === "bool") {
    if (typeof value !== "boolean") invalidScalar(field, value);
    return value;
  }
  if (/^(?:u?int(?:8|16|32)|float|double)$/u.test(type)) {
    if (typeof value !== "number" || !Number.isFinite(value)) invalidScalar(field, value);
    return value;
  }
  if (type === "int64" || type === "uint64") {
    if (typeof value === "bigint") return value;
    if (typeof value !== "number" || !Number.isSafeInteger(value)) invalidScalar(field, value);
    return BigInt(value);
  }
  unsupported("Unsupported Lance BTree lookup type", {
    column: field.name,
    logicalType: type,
  });
}

function invalidScalar(field: LanceField, value: LanceScalarValue): never {
  throw new LakeqlError(
    "LAKEQL_VALIDATION_ERROR",
    `Invalid scalar lookup value for Lance column ${field.name}`,
    { column: field.name, logicalType: field.logicalType, valueType: typeof value },
  );
}

function compareScalar(left: LanceScalarValue, right: LanceScalarValue): number {
  if (typeof left === "bigint" || typeof right === "bigint") {
    const leftBig = typeof left === "bigint" ? left : BigInt(left as number);
    const rightBig = typeof right === "bigint" ? right : BigInt(right as number);
    return leftBig < rightBig ? -1 : leftBig > rightBig ? 1 : 0;
  }
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function decodeUtf8(bytes: Uint8Array): string {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    corrupt("Invalid UTF-8 in Lance BTree metadata");
  }
}

function joinObjectPath(...parts: string[]): string {
  return parts
    .flatMap((part) => part.split("/"))
    .filter((part) => part.length > 0)
    .join("/");
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
