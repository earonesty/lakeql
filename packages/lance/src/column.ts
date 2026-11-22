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
import type { ByteRange, LanceReadContext, MemoryLease, RangeLease } from "./io.js";
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

/**
 * Buffers returned by a range reader, together with their memory charge. Every reader admits
 * its output allocations against the operation's memory budget *before* allocating them, and
 * the charge lives exactly as long as the caller keeps the buffers: call `release()` once they
 * are dropped, or `retainForOperation()` to keep them (and their charge) until the operation
 * ends. Physical response bytes are charged separately by the read itself and released as soon
 * as they are copied out; no reader makes an unaccounted intermediate copy.
 */
export class Charged<T> {
  constructor(
    readonly values: T,
    readonly bytes: number,
    private readonly lease: MemoryLease,
    private readonly context: LanceReadContext,
  ) {}

  release(): void {
    this.lease.release();
  }

  retainForOperation(): T {
    this.lease.release();
    this.context.accountDecodedMemory(this.bytes);
    return this.values;
  }
}

/** Rows (or items, for child columns) covered by a column's pages. */
function columnRowCount(file: InspectedLanceFile, column: number): number {
  const pages = file.columns[column]?.pages;
  if (pages === undefined) corrupt("Lance column is missing");
  return pages.reduce((end, page) => Math.max(end, page.rowStart + page.length), 0);
}

/** Validate every requested range before anything is allocated or read. */
function validateRanges(
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): number {
  const rows = columnRowCount(file, column);
  let total = 0;
  for (const range of ranges) {
    if (
      !Number.isSafeInteger(range.start) ||
      !Number.isSafeInteger(range.end) ||
      range.start < 0 ||
      range.end < range.start ||
      range.end > rows
    ) {
      corrupt("Invalid Lance row range", { range, rows });
    }
    total += range.end - range.start;
  }
  return total;
}

/** Admit `bytes` of output, run `build`, and hand back the buffers with their charge. */
function admit<T>(
  context: LanceReadContext,
  bytes: number,
  build: () => Promise<T>,
): Promise<Charged<T>> {
  const lease = context.leaseDecodedMemory(bytes);
  return build().then(
    (values) => new Charged(values, bytes, lease, context),
    (error: unknown) => {
      lease.release();
      throw error;
    },
  );
}

/** Read physical ranges, let `consume` copy out of the response, then release it. */
async function withResponse(
  context: LanceReadContext,
  file: InspectedLanceFile,
  ranges: readonly ByteRange[],
  consume: (lease: RangeLease) => void,
): Promise<void> {
  const physical = ranges.filter((range) => range.length > 0);
  if (physical.length === 0) {
    consume(EMPTY_RESPONSE);
    return;
  }
  const lease = await context.readRanges(file.path, physical, "data", file.fileSize);
  try {
    consume(lease);
  } finally {
    lease.release();
  }
}

const EMPTY_RESPONSE = {
  slice: () => new Uint8Array(),
  release: () => undefined,
} as unknown as RangeLease;

function sliceOf(lease: RangeLease, range: ByteRange): Uint8Array {
  return range.length === 0 ? new Uint8Array() : lease.slice(range);
}

interface FixedRequest {
  range: ByteRange;
  target: number;
  /** First value index within the target range. */
  at: number;
  count: number;
}

function planFixed(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
  bytesPerValue: number,
): FixedRequest[] {
  const requests: FixedRequest[] = [];
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
      if (slice.to * bytesPerValue > buffer.length) corrupt("Lance flat range exceeds its buffer");
      requests.push({
        range: {
          offset: buffer.offset + slice.from * bytesPerValue,
          length: (slice.to - slice.from) * bytesPerValue,
        },
        target,
        at: slice.requestOffset,
        count: slice.to - slice.from,
      });
    }
  }
  return requests;
}

/**
 * Fixed-width values decoded straight from the physical response into one admitted output per
 * range. `valueBytes` is the retained size of one decoded value.
 */
async function readFixed<T>(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
  bytesPerValue: number,
  valueBytes: number,
  allocate: (count: number) => T,
  decode: (out: T, at: number, view: DataView, count: number) => void,
): Promise<Charged<T[]>> {
  const total = validateRanges(file, column, ranges);
  const requests = planFixed(context, file, column, ranges, bytesPerValue);
  return await admit(context, total * valueBytes, async () => {
    const outputs = ranges.map((range) => allocate(range.end - range.start));
    await withResponse(
      context,
      file,
      requests.map((request) => request.range),
      (lease) => {
        for (const request of requests) {
          const bytes = sliceOf(lease, request.range);
          const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          decode(outputs[request.target] as T, request.at, view, request.count);
        }
      },
    );
    return outputs;
  });
}

/** Raw fixed-width values, one buffer per range. */
export async function readFixedRanges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
  bytesPerValue: number,
): Promise<Charged<Uint8Array[]>> {
  return await readFixed(
    context,
    file,
    column,
    ranges,
    bytesPerValue,
    bytesPerValue,
    (count) => new Uint8Array(count * bytesPerValue),
    (out, at, view, count) => {
      out.set(
        new Uint8Array(view.buffer, view.byteOffset, count * bytesPerValue),
        at * bytesPerValue,
      );
    },
  );
}

export async function readU32Ranges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<Charged<Uint32Array[]>> {
  return await readFixed(
    context,
    file,
    column,
    ranges,
    4,
    4,
    (count) => new Uint32Array(count),
    (out, at, view, count) => {
      for (let i = 0; i < count; i += 1) out[at + i] = view.getUint32(i * 4, true);
    },
  );
}

/** Retained bytes charged per decoded `bigint` (heap object plus array slot). */
const BIGINT_BYTES = 32;

export async function readU64Ranges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<Charged<bigint[][]>> {
  return await readFixed(
    context,
    file,
    column,
    ranges,
    8,
    BIGINT_BYTES,
    (count) => new Array<bigint>(count).fill(0n),
    (out, at, view, count) => {
      for (let i = 0; i < count; i += 1) out[at + i] = view.getBigUint64(i * 8, true);
    },
  );
}

function toNumber(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) corrupt(`Lance ${label} exceeds safe integers`);
  return Number(value);
}

/** Retained bytes charged per decoded offset held in a JavaScript number array. */
const NUMBER_BYTES = 8;

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
): Promise<Charged<number[][]>> {
  const pages = file.columns[column]?.pages;
  if (pages === undefined) corrupt("Lance list column is missing");
  const total = validateRanges(file, column, ranges);
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
  const outputBytes = (total + ranges.length) * NUMBER_BYTES;
  return await admit(context, outputBytes, async () => {
    const results = ranges.map((range) => new Array<number>(range.end - range.start + 1).fill(0));
    await withResponse(
      context,
      file,
      requests.map((request) => request.range),
      (lease) => {
        for (const request of requests) {
          const encoding = request.slice.page.encoding;
          if (encoding.kind !== "list") continue;
          const bytes = sliceOf(lease, request.range);
          const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          const adjustment = encoding.nullOffsetAdjustment;
          const itemBase = itemBases[pages.indexOf(request.slice.page)] as number;
          const resolve = (index: number): number => {
            const raw = view.getBigUint64(index * 8, true);
            return itemBase + toNumber(adjustment > 0n ? raw % adjustment : raw, "list offset");
          };
          const target = results[request.target] as number[];
          const count = request.slice.to - request.slice.from;
          const entries = request.first ? 0 : 1;
          target[request.slice.requestOffset] = request.first ? itemBase : resolve(0);
          for (let row = 0; row < count; row += 1) {
            target[request.slice.requestOffset + row + 1] = resolve(entries + row);
          }
        }
      },
    );
    for (const [i, range] of ranges.entries()) {
      const offsets = results[i] as number[];
      for (let j = 1; j < offsets.length; j += 1) {
        if ((offsets[j] as number) < (offsets[j - 1] as number)) {
          corrupt("Lance list offsets are not monotonic", { range });
        }
      }
    }
    return results;
  });
}

/** Bytes charged per returned binary item beyond its payload (the view object). */
const ITEM_BYTES = 64;

/** Binary values for each (global) item range of a binary column. */
export async function readBinaryRanges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<Charged<Uint8Array[][]>> {
  const totalItems = validateRanges(file, column, ranges);
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
  // Phase 1: item offsets, decoded straight from the response into admitted (transient) arrays.
  const offsets = await admit(context, (totalItems + plans.length) * NUMBER_BYTES, async () => {
    const decoded: { plan: Plan; ends: number[]; start: number; range: ByteRange }[] = [];
    await withResponse(
      context,
      file,
      plans.map((plan) => plan.indices),
      (lease) => {
        for (const plan of plans) {
          const raw = sliceOf(lease, plan.indices);
          const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
          const adjust = (value: bigint): number =>
            toNumber(
              plan.nullAdjustment > 0n ? value % plan.nullAdjustment : value,
              "binary offset",
            );
          const all: number[] = [];
          for (let j = 0; j < raw.length / 8; j += 1)
            all.push(adjust(view.getBigUint64(j * 8, true)));
          const start = plan.first ? 0 : (all[0] as number);
          const ends = plan.first ? all : all.slice(1);
          const last = ends.at(-1) ?? start;
          if (last < start || last > plan.valueBuffer.length) {
            corrupt("Lance binary value range exceeds its page buffer");
          }
          let cursor = start;
          for (const end of ends) {
            if (end < cursor) corrupt("Lance binary offsets are not monotonic");
            cursor = end;
          }
          decoded.push({
            plan,
            ends,
            start,
            range: { offset: plan.valueBuffer.offset + start, length: last - start },
          });
        }
      },
    );
    return decoded;
  });
  try {
    // Phase 2: payload bytes, admitted at their exact size before they are allocated.
    const valueBytes = offsets.values.reduce((sum, item) => sum + item.range.length, 0);
    return await admit(context, valueBytes + totalItems * ITEM_BYTES, async () => {
      const outputs: Uint8Array[][] = ranges.map(
        (range) => new Array<Uint8Array>(range.end - range.start),
      );
      await withResponse(
        context,
        file,
        offsets.values.map((item) => item.range),
        (lease) => {
          for (const item of offsets.values) {
            const bytes = new Uint8Array(item.range.length);
            bytes.set(sliceOf(lease, item.range));
            const target = outputs[item.plan.target] as Uint8Array[];
            let cursor = item.start;
            for (const [row, end] of item.ends.entries()) {
              target[item.plan.slice.requestOffset + row] = bytes.subarray(
                cursor - item.start,
                end - item.start,
              );
              cursor = end;
            }
          }
        },
      );
      return outputs;
    });
  } finally {
    offsets.release();
  }
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}
