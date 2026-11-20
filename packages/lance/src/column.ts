/**
 * Range readers for Lance 2.0 columns that hold fixed-width values, variable-length binary
 * values, and variable-length lists. They serve index files (token sets, posting lists, doc
 * sets) where a query needs a handful of contiguous rows rather than a row-by-row take.
 *
 * Every reader requests only the bytes covering the requested rows; offsets are read before
 * the values they address, so a nested read costs one dependent wave per nesting level.
 */
import { LakeqlError } from "lakeql";
import type { InspectedLanceFile } from "./file.js";
import type { ByteRange, LanceReadContext } from "./io.js";
import type { LanceArrayEncoding, LanceFlatEncoding, LancePage } from "./proto.js";

export interface RowRange {
  start: number;
  end: number;
}

export type ColumnSelector = { name: string; parentId?: number };

export function resolveColumn(
  file: InspectedLanceFile,
  selector: ColumnSelector,
): { column: number; fieldId: number } {
  const field = file.fields.find(
    (candidate) =>
      candidate.name === selector.name && candidate.parentId === (selector.parentId ?? -1),
  );
  if (field === undefined) {
    corrupt(`Lance index file ${file.path} has no column ${selector.name}`);
  }
  const column = file.fieldColumns?.get(field.id);
  if (column === undefined) corrupt(`Lance index column ${selector.name} has no physical column`);
  return { column, fieldId: field.id };
}

export function childField(file: InspectedLanceFile, parentFieldId: number): number {
  const child = file.fields.find((field) => field.parentId === parentFieldId);
  if (child === undefined) corrupt("Lance list column has no item field");
  return child.id;
}

export function columnOfField(file: InspectedLanceFile, fieldId: number): number {
  const column = file.fieldColumns?.get(fieldId);
  if (column === undefined) corrupt("Lance field has no physical column");
  return column;
}

/** Physical encodings that can carry a fixed-width value for index files. */
function flatValues(encoding: LanceArrayEncoding): LanceFlatEncoding {
  let current = encoding;
  if (current.kind === "nullable") {
    if (current.mode !== "no_nulls") {
      unsupported("Nullable Lance index columns are not supported");
    }
    current = current.values;
  }
  if (current.kind !== "flat") {
    unsupported("Lance index column does not use flat fixed-width encoding", {
      encoding: current.kind,
    });
  }
  if (current.bufferType !== 0) unsupported("Lance index column uses a non-page buffer");
  if (current.compression !== undefined) {
    unsupported("Compressed Lance index columns are not supported", {
      compression: current.compression,
    });
  }
  return current;
}

function pageBuffer(page: LancePage, flat: LanceFlatEncoding): ByteRange {
  const offset = page.bufferOffsets[flat.bufferIndex];
  const length = page.bufferSizes[flat.bufferIndex];
  if (offset === undefined || length === undefined) corrupt("Lance page buffer is missing");
  return { offset, length };
}

interface PageSlice {
  page: LancePage;
  /** Page-local first row (inclusive). */
  from: number;
  /** Page-local last row (exclusive). */
  to: number;
  /** Index of the first selected row in the request range. */
  requestOffset: number;
}

function splitByPage(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  range: RowRange,
): PageSlice[] {
  const pages = file.columns[column]?.pages;
  if (pages === undefined) corrupt("Lance column is missing");
  if (range.start < 0 || range.end < range.start) corrupt("Invalid Lance row range");
  const out: PageSlice[] = [];
  let cursor = range.start;
  for (const [pageIndex, page] of pages.entries()) {
    const pageEnd = page.rowStart + page.length;
    if (pageEnd <= cursor) continue;
    if (page.rowStart > cursor) break;
    context.stats.pages.add(`${file.path}:${column}:${pageIndex}`);
    const to = Math.min(range.end, pageEnd);
    out.push({
      page,
      from: cursor - page.rowStart,
      to: to - page.rowStart,
      requestOffset: cursor - range.start,
    });
    cursor = to;
    if (cursor >= range.end) break;
  }
  if (cursor < range.end) corrupt("Lance row range is not covered by column pages");
  return out;
}

async function readLeased(
  context: LanceReadContext,
  file: InspectedLanceFile,
  ranges: readonly ByteRange[],
): Promise<Uint8Array[]> {
  if (ranges.length === 0) return [];
  const lease = await context.readRanges(file.path, ranges, "data", file.fileSize);
  try {
    return ranges.map((range) =>
      range.length === 0 ? new Uint8Array() : lease.slice(range).slice(),
    );
  } finally {
    lease.release();
  }
}

/** Fixed-width values for each row range, copied into one buffer per range. */
export async function readFixedRanges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
  bytesPerValue: number,
): Promise<Uint8Array[]> {
  const requests: { range: ByteRange; target: number; at: number }[] = [];
  const outputs = ranges.map((range) => new Uint8Array((range.end - range.start) * bytesPerValue));
  for (const [target, range] of ranges.entries()) {
    if (range.end === range.start) continue;
    for (const slice of splitByPage(context, file, column, range)) {
      const flat = flatValues(slice.page.encoding);
      if (flat.bitsPerValue !== bytesPerValue * 8) {
        unsupported("Lance index column has an unexpected value width", {
          expected: bytesPerValue * 8,
          actual: flat.bitsPerValue,
        });
      }
      const buffer = pageBuffer(slice.page, flat);
      const offset = buffer.offset + slice.from * bytesPerValue;
      const length = (slice.to - slice.from) * bytesPerValue;
      if (slice.to * bytesPerValue > buffer.length) corrupt("Lance flat range exceeds its buffer");
      requests.push({
        range: { offset, length },
        target,
        at: slice.requestOffset * bytesPerValue,
      });
    }
  }
  const bytes = await readLeased(
    context,
    file,
    requests.map((request) => request.range),
  );
  for (const [i, request] of requests.entries()) {
    (outputs[request.target] as Uint8Array).set(bytes[i] as Uint8Array, request.at);
  }
  return outputs;
}

export async function readU32Ranges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<Uint32Array[]> {
  const raw = await readFixedRanges(context, file, column, ranges, 4);
  return raw.map((bytes) => {
    const out = new Uint32Array(bytes.length / 4);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (let i = 0; i < out.length; i += 1) out[i] = view.getUint32(i * 4, true);
    return out;
  });
}

export async function readU64Ranges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<bigint[][]> {
  const raw = await readFixedRanges(context, file, column, ranges, 8);
  return raw.map((bytes) => {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const out: bigint[] = [];
    for (let i = 0; i < bytes.length / 8; i += 1) out.push(view.getBigUint64(i * 8, true));
    return out;
  });
}

function toNumber(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) corrupt(`Lance ${label} exceeds safe integers`);
  return Number(value);
}

/**
 * Absolute child offsets of the lists in each row range. The result for a range of `n` rows
 * has `n + 1` entries: entry `i` is the first child item of row `i`, and the last is the end of
 * the final row. Child indexes are global across list pages.
 */
export async function readListOffsets(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<number[][]> {
  const pages = file.columns[column]?.pages;
  if (pages === undefined) corrupt("Lance list column is missing");
  const itemBases: number[] = [];
  let base = 0;
  for (const page of pages) {
    itemBases.push(base);
    if (page.encoding.kind !== "list") unsupported("Lance column is not a Lance 2.0 list column");
    base += page.encoding.numItems;
  }
  type Request = { target: number; slice: PageSlice; range: ByteRange; first: boolean };
  const requests: Request[] = [];
  for (const [target, range] of ranges.entries()) {
    if (range.end === range.start) continue;
    for (const slice of splitByPage(context, file, column, range)) {
      const encoding = slice.page.encoding;
      if (encoding.kind !== "list") unsupported("Lance column is not a Lance 2.0 list column");
      const flat = flatValues(encoding.offsets);
      if (flat.bitsPerValue !== 64) unsupported("Lance list offsets must be 64-bit");
      const buffer = pageBuffer(slice.page, flat);
      // Offsets are end positions; row 0 of a page starts at 0, so one extra leading entry is
      // needed for any other starting row.
      const first = slice.from === 0;
      const from = first ? 0 : slice.from - 1;
      if (slice.to * 8 > buffer.length) corrupt("Lance list offsets exceed their buffer");
      requests.push({
        target,
        slice,
        first,
        range: { offset: buffer.offset + from * 8, length: (slice.to - from) * 8 },
      });
    }
  }
  const bytes = await readLeased(
    context,
    file,
    requests.map((request) => request.range),
  );
  const results = ranges.map((range) => new Array<number>(range.end - range.start + 1).fill(0));
  for (const [i, request] of requests.entries()) {
    const encoding = request.slice.page.encoding;
    if (encoding.kind !== "list") continue;
    const view = new DataView(
      (bytes[i] as Uint8Array).buffer,
      (bytes[i] as Uint8Array).byteOffset,
      (bytes[i] as Uint8Array).byteLength,
    );
    const adjustment = encoding.nullOffsetAdjustment;
    const pageIndex = pages.indexOf(request.slice.page);
    const itemBase = itemBases[pageIndex] as number;
    const resolve = (index: number): number => {
      const raw = view.getBigUint64(index * 8, true);
      return itemBase + toNumber(adjustment > 0n ? raw % adjustment : raw, "list offset");
    };
    const target = results[request.target] as number[];
    const count = request.slice.to - request.slice.from;
    const startOffset = request.first ? itemBase : resolve(0);
    const entries = request.first ? 0 : 1;
    target[request.slice.requestOffset] = startOffset;
    for (let row = 0; row < count; row += 1) {
      target[request.slice.requestOffset + row + 1] = resolve(entries + row);
    }
  }
  for (const [i, range] of ranges.entries()) {
    const offsets = results[i] as number[];
    for (let j = 1; j < offsets.length; j += 1) {
      if ((offsets[j] as number) < (offsets[j - 1] as number)) {
        corrupt("Lance list offsets are not monotonic", { range });
      }
    }
  }
  return results;
}

/** Binary values for each (global) item range of a binary column. */
export async function readBinaryRanges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<Uint8Array[][]> {
  type Plan = {
    target: number;
    slice: PageSlice;
    indices: ByteRange;
    nullAdjustment: bigint;
    first: boolean;
    valueBuffer: ByteRange;
  };
  const plans: Plan[] = [];
  for (const [target, range] of ranges.entries()) {
    if (range.end === range.start) continue;
    for (const slice of splitByPage(context, file, column, range)) {
      const encoding = slice.page.encoding;
      if (encoding.kind !== "binary") unsupported("Lance column does not use binary encoding");
      const indices = flatValues(encoding.indices);
      const bytes = flatValues(encoding.bytes);
      if (indices.bitsPerValue !== 64) unsupported("Lance binary offsets must be 64-bit");
      if (bytes.bitsPerValue !== 8) unsupported("Lance binary values must be byte arrays");
      const indexBuffer = pageBuffer(slice.page, indices);
      const first = slice.from === 0;
      const from = first ? 0 : slice.from - 1;
      if (slice.to * 8 > indexBuffer.length) corrupt("Lance binary offsets exceed their buffer");
      plans.push({
        target,
        slice,
        first,
        nullAdjustment: encoding.nullAdjustment,
        indices: { offset: indexBuffer.offset + from * 8, length: (slice.to - from) * 8 },
        valueBuffer: pageBuffer(slice.page, bytes),
      });
    }
  }
  const offsetBytes = await readLeased(
    context,
    file,
    plans.map((plan) => plan.indices),
  );
  const outputs: Uint8Array[][] = ranges.map(
    (range) => new Array<Uint8Array>(range.end - range.start),
  );
  const valueRanges: { plan: Plan; ends: number[]; start: number; range: ByteRange }[] = [];
  for (const [i, plan] of plans.entries()) {
    const raw = offsetBytes[i] as Uint8Array;
    const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    const adjust = (value: bigint): number =>
      toNumber(plan.nullAdjustment > 0n ? value % plan.nullAdjustment : value, "binary offset");
    const total = raw.length / 8;
    const all: number[] = [];
    for (let j = 0; j < total; j += 1) all.push(adjust(view.getBigUint64(j * 8, true)));
    const start = plan.first ? 0 : (all[0] as number);
    const ends = plan.first ? all : all.slice(1);
    const last = ends.at(-1) ?? start;
    if (last < start || last > plan.valueBuffer.length) {
      corrupt("Lance binary value range exceeds its page buffer");
    }
    valueRanges.push({
      plan,
      ends,
      start,
      range: { offset: plan.valueBuffer.offset + start, length: last - start },
    });
  }
  const values = await readLeased(
    context,
    file,
    valueRanges.map((item) => item.range),
  );
  for (const [i, item] of valueRanges.entries()) {
    const bytes = values[i] as Uint8Array;
    const target = outputs[item.plan.target] as Uint8Array[];
    let cursor = item.start;
    for (const [row, end] of item.ends.entries()) {
      if (end < cursor) corrupt("Lance binary offsets are not monotonic");
      target[item.plan.slice.requestOffset + row] = bytes.subarray(
        cursor - item.start,
        end - item.start,
      );
      cursor = end;
    }
  }
  return outputs;
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}
