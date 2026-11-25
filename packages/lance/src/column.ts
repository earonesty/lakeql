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
import type {
  ByteRange,
  LanceReadContext,
  MemoryLease,
  MemoryReservation,
  RangeLease,
} from "./io.js";
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
 *
 * The charge covers every object a reader creates, not only payload bytes: each returned
 * container costs `TYPED_ARRAY_BYTES` or `ARRAY_BYTES` even when it is empty, and the read
 * plan costs `REQUEST_BYTES` per planned request for as long as the read runs. A request for
 * many empty or tiny ranges is therefore bounded by the budget like any other.
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

/** Retained bytes of one typed array beyond its elements (view, buffer, and the slot holding it). */
const TYPED_ARRAY_BYTES = 96;
/** Retained bytes of one plain array beyond its elements (header and the slot holding it). */
const ARRAY_BYTES = 32;
/** Bytes of one planned request while a read runs (page slice, byte range, and bookkeeping). */
const REQUEST_BYTES = 128;
/** Bytes of one located payload range while a binary read runs. */
const LOCATED_BYTES = 64;

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

/** Run a read with a plan reservation that is released when the read settles. */
async function withPlan<T>(
  context: LanceReadContext,
  run: (plan: MemoryReservation) => Promise<T>,
): Promise<T> {
  const plan = context.openReservation();
  try {
    return await run(plan);
  } finally {
    plan.release();
  }
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

/** The value buffer of a fixed-width page slice, checked for width and bounds. */
function fixedPageBuffer(slice: PageSlice, bytesPerValue: number): ByteRange {
  const flat = flatValues(slice.page.encoding);
  if (flat.bitsPerValue !== bytesPerValue * 8) {
    unsupported("Lance index column has an unexpected value width", {
      expected: bytesPerValue * 8,
      actual: flat.bitsPerValue,
    });
  }
  const buffer = pageBuffer(slice.page, flat);
  if (slice.to * bytesPerValue > buffer.length) corrupt("Lance flat range exceeds its buffer");
  return buffer;
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
  plan: MemoryReservation,
): FixedRequest[] {
  const requests: FixedRequest[] = [];
  for (const [target, range] of ranges.entries()) {
    if (range.end === range.start) continue;
    for (const slice of splitByPage(context, file, column, range)) {
      const buffer = fixedPageBuffer(slice, bytesPerValue);
      plan.grow(REQUEST_BYTES);
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

/** How one decoded element type is allocated, charged and filled. */
interface FixedCodec<T> {
  /** Encoded width of one value. */
  bytesPerValue: number;
  /** Retained size of one decoded value. */
  valueBytes: number;
  /** Retained size of one output container beyond its values. */
  containerBytes: number;
  allocate: (count: number) => T;
  /** Decode the value at `valueIndex` of `view` into `out[at]`. */
  decode: (out: T, at: number, view: DataView, valueIndex: number) => void;
}

const U32: FixedCodec<Uint32Array> = {
  bytesPerValue: 4,
  valueBytes: 4,
  containerBytes: TYPED_ARRAY_BYTES,
  allocate: (count) => new Uint32Array(count),
  decode: (out, at, view, valueIndex) => {
    out[at] = view.getUint32(valueIndex * 4, true);
  },
};

/** Retained bytes charged per decoded `bigint` (heap object plus array slot). */
const BIGINT_BYTES = 32;

const U64: FixedCodec<bigint[]> = {
  bytesPerValue: 8,
  valueBytes: BIGINT_BYTES,
  containerBytes: ARRAY_BYTES,
  allocate: (count) => new Array<bigint>(count).fill(0n),
  decode: (out, at, view, valueIndex) => {
    out[at] = view.getBigUint64(valueIndex * 8, true);
  },
};

/**
 * Fixed-width values decoded straight from the physical response into one admitted output per
 * range. The plan is charged request by request as it is built, and the outputs (containers
 * included, so empty ranges are not free) are admitted before any of them is allocated.
 */
async function readFixed<T>(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
  codec: FixedCodec<T>,
): Promise<Charged<T[]>> {
  const total = validateRanges(file, column, ranges);
  const plan = context.openReservation();
  try {
    const requests = planFixed(context, file, column, ranges, codec.bytesPerValue, plan);
    const outputBytes = ranges.length * codec.containerBytes + total * codec.valueBytes;
    return await admit(context, outputBytes, async () => {
      const outputs = ranges.map((range) => codec.allocate(range.end - range.start));
      await withResponse(
        context,
        file,
        requests.map((request) => request.range),
        (lease) => {
          for (const request of requests) {
            const bytes = sliceOf(lease, request.range);
            const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
            const out = outputs[request.target] as T;
            for (let i = 0; i < request.count; i += 1) codec.decode(out, request.at + i, view, i);
          }
        },
      );
      return outputs;
    });
  } finally {
    plan.release();
  }
}

/**
 * Fixed-width values of individual rows (ascending), gathered into one admitted output.
 *
 * Rows closer together than the read planner's coalescing gap share one planned request, so
 * plan memory follows the number of physical requests rather than the number of rows, and the
 * output is a single container however scattered the rows are.
 */
async function readFixedRows<T>(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  rows: ArrayLike<number>,
  codec: FixedCodec<T>,
): Promise<Charged<T>> {
  const limit = columnRowCount(file, column);
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] as number;
    if (!Number.isSafeInteger(row) || row < 0 || row >= limit) {
      corrupt("Invalid Lance row", { row, rows: limit });
    }
    if (i > 0 && row < (rows[i - 1] as number)) corrupt("Lance rows must be ascending", { row });
  }
  const gapRows = Math.floor(context.planning.coalesceGapBytes / codec.bytesPerValue);
  const maxRows = Math.max(
    1,
    Math.floor(context.planning.maxCoalescedRangeBytes / codec.bytesPerValue),
  );
  type Request = { range: ByteRange; rowStart: number; rowEnd: number };
  const plan = context.openReservation();
  try {
    const requests: Request[] = [];
    const planRun = (start: number, end: number): void => {
      for (const slice of splitByPage(context, file, column, { start, end })) {
        const buffer = fixedPageBuffer(slice, codec.bytesPerValue);
        plan.grow(REQUEST_BYTES);
        requests.push({
          range: {
            offset: buffer.offset + slice.from * codec.bytesPerValue,
            length: (slice.to - slice.from) * codec.bytesPerValue,
          },
          rowStart: start + slice.requestOffset,
          rowEnd: start + slice.requestOffset + (slice.to - slice.from),
        });
      }
    };
    let start = -1;
    let end = -1;
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i] as number;
      if (start >= 0 && row - end <= gapRows && row + 1 - start <= maxRows) {
        end = Math.max(end, row + 1);
        continue;
      }
      if (start >= 0) planRun(start, end);
      start = row;
      end = row + 1;
    }
    if (start >= 0) planRun(start, end);
    const outputBytes = codec.containerBytes + rows.length * codec.valueBytes;
    return await admit(context, outputBytes, async () => {
      const out = codec.allocate(rows.length);
      await withResponse(
        context,
        file,
        requests.map((request) => request.range),
        (lease) => {
          let i = 0;
          for (const request of requests) {
            const bytes = sliceOf(lease, request.range);
            const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
            while (i < rows.length && (rows[i] as number) < request.rowEnd) {
              codec.decode(out, i, view, (rows[i] as number) - request.rowStart);
              i += 1;
            }
          }
          if (i !== rows.length) corrupt("Lance rows are not covered by column pages");
        },
      );
      return out;
    });
  } finally {
    plan.release();
  }
}

/** 32-bit values of individual ascending rows, in one output. */
export async function readU32Rows(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  rows: ArrayLike<number>,
): Promise<Charged<Uint32Array>> {
  return await readFixedRows(context, file, column, rows, U32);
}

/** 64-bit values of individual ascending rows, in one output. */
export async function readU64Rows(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  rows: ArrayLike<number>,
): Promise<Charged<bigint[]>> {
  return await readFixedRows(context, file, column, rows, U64);
}

/** Raw fixed-width values, one buffer per range. */
export async function readFixedRanges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
  bytesPerValue: number,
): Promise<Charged<Uint8Array[]>> {
  return await readFixed(context, file, column, ranges, {
    bytesPerValue,
    valueBytes: bytesPerValue,
    containerBytes: TYPED_ARRAY_BYTES,
    allocate: (count) => new Uint8Array(count * bytesPerValue),
    decode: (out, at, view, valueIndex) => {
      for (let b = 0; b < bytesPerValue; b += 1) {
        out[at * bytesPerValue + b] = view.getUint8(valueIndex * bytesPerValue + b);
      }
    },
  });
}

export async function readU32Ranges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<Charged<Uint32Array[]>> {
  return await readFixed(context, file, column, ranges, U32);
}

export async function readU64Ranges(
  context: LanceReadContext,
  file: InspectedLanceFile,
  column: number,
  ranges: readonly RowRange[],
): Promise<Charged<bigint[][]>> {
  return await readFixed(context, file, column, ranges, U64);
}

function toNumber(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) corrupt(`Lance ${label} exceeds safe integers`);
  return Number(value);
}

/** Retained bytes charged per decoded offset (a double, in a number or `Float64Array`). */
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
  type Request = { target: number; slice: PageSlice; range: ByteRange; first: boolean };
  return await withPlan(context, async (plan) => {
    plan.grow(pages.length * NUMBER_BYTES);
    const itemBases: number[] = [];
    let base = 0;
    for (const page of pages) {
      itemBases.push(base);
      if (page.encoding.kind !== "list") unsupported("Lance column is not a Lance 2.0 list column");
      base += page.encoding.numItems;
    }
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
        plan.grow(REQUEST_BYTES);
        requests.push({
          target,
          slice,
          first,
          range: { offset: buffer.offset + from * 8, length: (slice.to - from) * 8 },
        });
      }
    }
    const outputBytes = ranges.length * ARRAY_BYTES + (total + ranges.length) * NUMBER_BYTES;
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
  });
}

/** Bytes charged per returned binary item beyond its payload (the view and its slot). */
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
  /**
   * One plan's items: `count` end offsets (page-buffer relative) at `ends[at..]`, and the
   * payload range they span.
   */
  type Located = { plan: Plan; at: number; count: number; start: number; range: ByteRange };
  const planning = context.openReservation();
  try {
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
        // The plan, and the located payload range phase 1 derives from it.
        planning.grow(REQUEST_BYTES + LOCATED_BYTES);
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
    // Phase 1: item offsets, decoded straight from the response into one admitted (transient)
    // array holding exactly one entry per item. The leading offset of a mid-page slice is read
    // in place; nothing is copied out of a longer array.
    const offsets = await admit(
      context,
      TYPED_ARRAY_BYTES + totalItems * NUMBER_BYTES,
      async () => {
        const ends = new Float64Array(totalItems);
        const located: Located[] = [];
        await withResponse(
          context,
          file,
          plans.map((plan) => plan.indices),
          (lease) => {
            let at = 0;
            for (const plan of plans) {
              const raw = sliceOf(lease, plan.indices);
              const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
              const adjusted = (index: number): number => {
                const value = view.getBigUint64(index * 8, true);
                return toNumber(
                  plan.nullAdjustment > 0n ? value % plan.nullAdjustment : value,
                  "binary offset",
                );
              };
              const lead = plan.first ? 0 : 1;
              const start = plan.first ? 0 : adjusted(0);
              const count = plan.slice.to - plan.slice.from;
              let cursor = start;
              for (let j = 0; j < count; j += 1) {
                const end = adjusted(lead + j);
                if (end < cursor) corrupt("Lance binary offsets are not monotonic");
                ends[at + j] = end;
                cursor = end;
              }
              if (cursor > plan.valueBuffer.length) {
                corrupt("Lance binary value range exceeds its page buffer");
              }
              located.push({
                plan,
                at,
                count,
                start,
                range: { offset: plan.valueBuffer.offset + start, length: cursor - start },
              });
              at += count;
            }
          },
        );
        return { ends, located };
      },
    );
    try {
      // Phase 2: payload bytes, admitted at their exact size before they are allocated as one
      // buffer, along with one container per range and one view per item.
      const { ends, located } = offsets.values;
      const valueBytes = located.reduce((sum, item) => sum + item.range.length, 0);
      const outputBytes =
        TYPED_ARRAY_BYTES + valueBytes + ranges.length * ARRAY_BYTES + totalItems * ITEM_BYTES;
      return await admit(context, outputBytes, async () => {
        const payload = new Uint8Array(valueBytes);
        const outputs: Uint8Array[][] = ranges.map(
          (range) => new Array<Uint8Array>(range.end - range.start),
        );
        await withResponse(
          context,
          file,
          located.map((item) => item.range),
          (lease) => {
            let base = 0;
            for (const item of located) {
              payload.set(sliceOf(lease, item.range), base);
              const target = outputs[item.plan.target] as Uint8Array[];
              let cursor = item.start;
              for (let row = 0; row < item.count; row += 1) {
                const end = ends[item.at + row] as number;
                target[item.plan.slice.requestOffset + row] = payload.subarray(
                  base + cursor - item.start,
                  base + end - item.start,
                );
                cursor = end;
              }
              base += item.range.length;
            }
          },
        );
        return outputs;
      });
    } finally {
      offsets.release();
    }
  } finally {
    planning.release();
  }
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}
