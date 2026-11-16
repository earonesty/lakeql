// Bounded random access to Lance 2.1 primitive/struct leaf mini-blocks.
import type { Compression21, Layout21 } from "./encoding21.js";
import { corrupt21, unsupported21 } from "./encoding21.js";
import type { ByteRange, LanceReadContext, MemoryLease } from "./io.js";
import type { LancePage } from "./proto.js";

type Atom = number | bigint | Uint8Array;
type Accessor = { bits: number; get(index: number): Atom; symbols?: Uint8Array };
type DecodeFixed = (bytes: Uint8Array, bits: number, index: number, logicalType: string) => unknown;

export async function readStructuralColumn(
  options: {
    context: LanceReadContext;
    path: string;
    fileSize: number;
    page: LancePage;
    layout: Layout21;
    rows?: readonly number[];
    rowRange?: { start: number; count: number };
    logicalType: string;
    retainValues: boolean;
    decodeFixed: DecodeFixed;
  },
  emit: (row: number, value: unknown, definition: number, layers: readonly number[]) => void,
): Promise<void> {
  const { context, page, layout } = options;
  const scratch: MemoryLease[] = [];
  scratch.push(context.leaseDecodedMemory((options.rows?.length ?? 0) * 32 + 64));
  const rows =
    options.rowRange === undefined ? [...new Set(options.rows ?? [])].sort((a, b) => a - b) : [];
  const rowCount = options.rowRange?.count ?? rows.length;
  const rowAt = (i: number) =>
    options.rowRange === undefined ? (rows[i] as number) : options.rowRange.start + i;
  const raw = new Uint8Array(8);
  const rawView = new DataView(raw.buffer);
  let dictionary: Accessor | undefined;
  let dictionaryLease: Awaited<ReturnType<LanceReadContext["readRange"]>> | undefined;
  const maxDef = layout.layers.filter((layer) => layer === 3).length;
  const deliver = (
    row: number,
    accessor: Accessor | undefined,
    index: number,
    definition: number,
  ) => {
    context.check();
    if (!Number.isSafeInteger(definition) || definition < 0 || definition > maxDef)
      corrupt21("Invalid definition level");
    if (definition > 0 || accessor === undefined) {
      emit(row, null, definition, layout.layers);
      return;
    }
    if (dictionary !== undefined) {
      const id = Number(accessor.get(index));
      if (!Number.isSafeInteger(id) || id < 0 || id >= layout.dictionaryItems)
        corrupt21("Dictionary index is outside page dictionary");
      accessor = dictionary;
      index = id;
    }
    const atom = accessor.get(index);
    let value: unknown;
    if (atom instanceof Uint8Array) {
      value = binaryValue(context, atom, accessor.symbols, options.logicalType);
    } else {
      if (accessor.bits < 1 || accessor.bits > 64) corrupt21("Invalid primitive bit width");
      raw.fill(0);
      rawView.setBigUint64(0, typeof atom === "bigint" ? atom : BigInt(atom), true);
      value = options.decodeFixed(raw, accessor.bits, 0, options.logicalType);
      if (options.retainValues) context.accountDecodedMemory(8);
    }
    emit(row, value, definition, layout.layers);
  };
  try {
    if (
      !Number.isSafeInteger(rowCount) ||
      rowCount < 0 ||
      (rowCount > 0 &&
        (!Number.isSafeInteger(rowAt(0)) || rowAt(0) < 0 || rowAt(rowCount - 1) >= page.length)) ||
      rows.some((row) => !Number.isSafeInteger(row) || row < 0 || row >= page.length)
    )
      corrupt21("Structural row is outside page");
    if (layout.layout === "null") {
      if (maxDef === 0) corrupt21("All-null page has no nullable layer");
      for (let i = 0; i < rowCount; i++) deliver(rowAt(i), undefined, 0, maxDef);
      return;
    }
    if (layout.layout === "zip") {
      if (
        layout.items !== page.length ||
        layout.bitsDef > 16 ||
        layout.values === undefined ||
        layout.bitsValue > 0 === layout.bitsOffset > 0
      )
        unsupported21("Full-zip projection requires one primitive item per row");
      scratch.push(context.leaseDecodedMemory(rowCount * 256));
      const controlBytes = Math.ceil(layout.bitsDef / 8);
      const data = bufferRange(page, 0);
      const entries: { row: number; range: ByteRange }[] = [];
      if (layout.bitsOffset > 0) {
        if (![32, 64].includes(layout.bitsOffset))
          unsupported21("Full-zip offsets must be 32 or 64 bits");
        const index = bufferRange(page, 1);
        const width = index.length / (page.length + 1);
        if (![1, 2, 4, 8].includes(width)) corrupt21("Invalid full-zip index width");
        const ranges = Array.from({ length: rowCount }, (_, i) => ({
          offset: index.offset + rowAt(i) * width,
          length: width * 2,
        }));
        const lease = await context.readRanges(
          options.path,
          ranges,
          "file_metadata",
          options.fileSize,
        );
        try {
          for (let i = 0; i < rowCount; i++) {
            const range = ranges[i] as ByteRange;
            const pair = lease.slice(range);
            const start = Number(unsigned(pair, 0, width * 8)),
              end = Number(unsigned(pair, width, width * 8));
            if (
              !Number.isSafeInteger(start) ||
              !Number.isSafeInteger(end) ||
              start < 0 ||
              end < start ||
              end > data.length ||
              end - start < controlBytes
            )
              corrupt21("Invalid full-zip row offsets");
            entries.push({
              row: rowAt(i),
              range: { offset: data.offset + start, length: end - start },
            });
          }
        } finally {
          lease.release();
        }
      } else {
        if (layout.bitsValue % 8 !== 0 || layout.bitsValue > 64)
          unsupported21("Full-zip numeric values must fit one primitive word");
        const stride = controlBytes + layout.bitsValue / 8;
        if (data.length !== page.length * stride) corrupt21("Invalid fixed full-zip data length");
        for (let i = 0; i < rowCount; i++)
          entries.push({
            row: rowAt(i),
            range: { offset: data.offset + rowAt(i) * stride, length: stride },
          });
      }
      if (entries.length === 0) return;
      const lease = await context.readRanges(
        options.path,
        entries.map((entry) => entry.range),
        "data",
        options.fileSize,
      );
      try {
        for (const entry of entries) {
          const bytes = lease.slice(entry.range);
          const definition = controlBytes === 0 ? 0 : Number(unsigned(bytes, 0, controlBytes * 8));
          if (definition > 0) {
            deliver(entry.row, undefined, 0, definition);
            continue;
          }
          if (layout.bitsOffset > 0) {
            let compressor = layout.values;
            const symbols = compressor.kind === "fsst" ? compressor.symbols : undefined;
            if (compressor.kind === "fsst") compressor = compressor.values;
            if (
              compressor.kind !== "variable" ||
              compressor.offsets.kind !== "flat" ||
              compressor.offsets.bits !== layout.bitsOffset
            )
              unsupported21("Unsupported full-zip variable compression");
            const length = Number(unsigned(bytes, controlBytes, layout.bitsOffset));
            const start = controlBytes + layout.bitsOffset / 8;
            if (!Number.isSafeInteger(length) || start + length !== bytes.length)
              corrupt21("Invalid full-zip value length");
            const value = binaryValue(context, bytes.subarray(start), symbols, options.logicalType);
            emit(entry.row, value, definition, layout.layers);
          } else {
            if (layout.values.kind !== "flat" || layout.values.bits !== layout.bitsValue)
              unsupported21("Unsupported full-zip numeric compression");
            deliver(
              entry.row,
              {
                bits: layout.bitsValue,
                get: () => unsigned(bytes, controlBytes, layout.bitsValue),
              },
              0,
              definition,
            );
          }
        }
      } finally {
        lease.release();
      }
      return;
    }
    if (layout.items !== page.length)
      unsupported21("Numeric structural projection requires one item per row");
    const meta = bufferRange(page, 0);
    const data = bufferRange(page, 1);
    if (meta.length === 0 || meta.length % 2 !== 0)
      corrupt21("Invalid mini-block directory length");
    const directory = await context.readRange(
      options.path,
      meta,
      "file_metadata",
      options.fileSize,
    );
    const chunks: { range: ByteRange; start: number; count: number; first: number; end: number }[] =
      [];
    try {
      const bytes = directory.slice(meta);
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      let rowStart = 0;
      let offset = data.offset;
      let selected = 0;
      for (let i = 0; i < meta.length; i += 2) {
        const word = view.getUint16(i, true);
        const log = word & 15;
        const count = i + 2 === meta.length ? page.length - rowStart : 2 ** log;
        const length = ((word >>> 4) + 1) * 8;
        if (
          (i + 2 < meta.length && log === 0) ||
          count <= 0 ||
          count > 32768 ||
          rowStart + count > page.length ||
          offset + length > data.offset + data.length
        )
          corrupt21("Invalid mini-block directory entry");
        if (i + 2 === meta.length && log !== 0 && count !== 2 ** log)
          corrupt21("Invalid final mini-block row count");
        const first = selected;
        if (options.rowRange === undefined) {
          while (selected < rowCount && rowAt(selected) < rowStart + count) selected++;
        } else
          selected = Math.max(
            selected,
            Math.min(rowCount, rowStart + count - options.rowRange.start),
          );
        if (selected > first) {
          scratch.push(context.leaseDecodedMemory(192));
          chunks.push({ range: { offset, length }, start: rowStart, count, first, end: selected });
        }
        rowStart += count;
        offset += length;
      }
      if (rowStart !== page.length || offset !== data.offset + data.length || selected !== rowCount)
        corrupt21("Mini-block directory does not cover page");
    } finally {
      directory.release();
    }
    if (chunks.length === 0) return;
    if (layout.dictionary !== undefined) {
      if (layout.dictionaryItems < 1) corrupt21("Invalid page dictionary count");
      const range = bufferRange(page, 2);
      dictionaryLease = await context.readRange(options.path, range, "data", options.fileSize);
      const bytes = dictionaryLease.slice(range);
      if (layout.dictionary.kind === "variable") {
        const width =
          layout.dictionary.offsets.kind === "flat" ? layout.dictionary.offsets.bits / 8 : 0;
        if (![4, 8].includes(width) || bytes.length < width * 2)
          unsupported21("Unsupported dictionary offset width");
        if (Number(unsigned(bytes, 0, width * 8)) !== width * 8)
          corrupt21("Dictionary binary header disagrees with encoding");
        const start = Number(unsigned(bytes, width, width * 8));
        if (start !== (layout.dictionaryItems + 3) * width || start > bytes.length)
          corrupt21("Invalid dictionary offsets");
        let previous = 0;
        for (let i = 0; i <= layout.dictionaryItems; i++) {
          const offset = Number(unsigned(bytes, (i + 2) * width, width * 8));
          if (!Number.isSafeInteger(offset) || offset < previous || start + offset > bytes.length)
            corrupt21("Invalid dictionary value offset");
          previous = offset;
        }
        dictionary = {
          bits: 0,
          get: (i) =>
            bytes.subarray(
              start + Number(unsigned(bytes, (i + 2) * width, width * 8)),
              start + Number(unsigned(bytes, (i + 3) * width, width * 8)),
            ),
        };
      } else
        dictionary = blockAccessor(
          layout.dictionary,
          [bytes],
          layout.dictionaryItems,
          context,
          scratch,
        );
    }
    const encoded = await context.readRanges(
      options.path,
      chunks.map((chunk) => chunk.range),
      "data",
      options.fileSize,
    );
    try {
      for (const chunk of chunks) {
        const workspace: MemoryLease[] = [];
        try {
          const bytes = encoded.slice(chunk.range);
          const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          if (bytes.length < 2 + 2 * layout.buffers + (layout.definitions === undefined ? 0 : 2))
            corrupt21("Truncated mini-block header");
          const levels = view.getUint16(0, true);
          let cursor = 2;
          const defLength = layout.definitions === undefined ? 0 : view.getUint16(cursor, true);
          if (layout.definitions !== undefined) cursor += 2;
          const lengths: number[] = [];
          for (let i = 0; i < layout.buffers; i++, cursor += 2)
            lengths.push(view.getUint16(cursor, true));
          cursor = align8(cursor);
          const section = (length: number): Uint8Array => {
            if (cursor + length > bytes.length) corrupt21("Mini-block buffer is outside chunk");
            const value = bytes.subarray(cursor, cursor + length);
            cursor = align8(cursor + length);
            return value;
          };
          const definitions =
            layout.definitions === undefined
              ? undefined
              : blockAccessor(
                  layout.definitions,
                  [section(defLength)],
                  chunk.count,
                  context,
                  workspace,
                );
          if (definitions !== undefined && levels !== chunk.count)
            corrupt21("Primitive definition count differs from rows");
          if (definitions === undefined && levels !== 0)
            corrupt21("Unexpected mini-block definition count");
          const buffers = lengths.map(section);
          if (cursor !== bytes.length) corrupt21("Mini-block has trailing data");
          if (layout.values === undefined) corrupt21("Missing mini-block value encoding");
          const accessor = blockAccessor(layout.values, buffers, chunk.count, context, workspace);
          for (let i = chunk.first; i < chunk.end; i++) {
            const row = rowAt(i);
            const index = row - chunk.start;
            const definition = definitions === undefined ? 0 : Number(definitions.get(index));
            deliver(row, accessor, index, definition);
          }
        } finally {
          for (const lease of workspace) lease.release();
        }
      }
    } finally {
      encoded.release();
    }
  } finally {
    dictionaryLease?.release();
    for (const lease of scratch) lease.release();
  }
}

function blockAccessor(
  encoding: Compression21,
  buffers: readonly Uint8Array[],
  count: number,
  context: LanceReadContext,
  workspace: MemoryLease[],
): Accessor {
  const bytes = buffers[0];
  if (encoding.kind === "constant") {
    if (encoding.value.length < 1 || encoding.value.length > 8 || buffers.length !== 0)
      unsupported21("Unsupported constant primitive encoding");
    const bits = encoding.value.length * 8;
    return { bits, get: () => unsigned(encoding.value, 0, bits) };
  }
  if (bytes === undefined) corrupt21("Missing compressed buffer");
  if (encoding.kind === "flat") {
    if (buffers.length !== 1 || bytes.length < Math.ceil((count * encoding.bits) / 8))
      corrupt21("Invalid flat mini-block size");
    return {
      bits: encoding.bits,
      get: (index) =>
        encoding.bits < 8
          ? packedSmall(bytes, index * encoding.bits, encoding.bits)
          : unsigned(bytes, (index * encoding.bits) / 8, encoding.bits),
    };
  }
  if (encoding.kind === "bitpack") {
    const bits = encoding.bits;
    const width = Number(unsigned(bytes, 0, bits));
    if (
      buffers.length !== 1 ||
      count > 1024 ||
      width < 0 ||
      width > bits ||
      bytes.length !== bits / 8 + width * 128
    )
      corrupt21("Invalid inline bitpacking buffer");
    return { bits, get: (index) => fastLanes(bytes, bits / 8, bits, width, index) };
  }
  if (encoding.kind === "out-of-line") {
    if (encoding.values.kind !== "flat" || buffers.length !== 1)
      unsupported21("Out-of-line bitpacking requires flat packed words");
    const bits = encoding.bits,
      width = encoding.values.bits;
    if (width > bits) corrupt21("Packed width exceeds word width");
    const whole = Math.floor(count / 1024),
      tail = count % 1024;
    const chunkBytes = width * 128;
    const full = whole * chunkBytes;
    const rawTail = tail > 0 && bytes.length === full + (tail * bits) / 8;
    if (bytes.length !== full + (tail === 0 ? 0 : rawTail ? (tail * bits) / 8 : chunkBytes))
      corrupt21("Invalid out-of-line bitpacking length");
    return {
      bits,
      get: (index) => {
        const chunk = Math.floor(index / 1024),
          local = index % 1024;
        if (rawTail && chunk === whole) return unsigned(bytes, full + (local * bits) / 8, bits);
        return fastLanes(bytes, chunk * chunkBytes, bits, width, local);
      },
    };
  }
  if (encoding.kind === "rle") {
    if (
      encoding.values.kind !== "flat" ||
      encoding.lengths.kind !== "flat" ||
      encoding.lengths.bits !== 8 ||
      buffers.length !== 2 ||
      buffers[1] === undefined
    )
      unsupported21("RLE requires flat primitive values and byte run lengths");
    const lengths = buffers[1];
    const bits = encoding.values.bits;
    if (bits < 8 || bytes.length !== (lengths.length * bits) / 8)
      corrupt21("Invalid RLE buffer sizes");
    workspace.push(context.leaseDecodedMemory(lengths.length * 4));
    const ends = new Uint32Array(lengths.length);
    let total = 0;
    for (let i = 0; i < lengths.length; i++) {
      const length = lengths[i] as number;
      if (length === 0) corrupt21("Zero RLE run length");
      total += length;
      ends[i] = total;
    }
    if (total < count || total - (lengths[lengths.length - 1] ?? 0) >= count)
      corrupt21("RLE runs do not cover mini-block");
    return {
      bits,
      get: (index) => {
        let low = 0,
          high = ends.length;
        while (low < high) {
          const mid = Math.floor((low + high) / 2);
          if ((ends[mid] as number) <= index) low = mid + 1;
          else high = mid;
        }
        return unsigned(bytes, (low * bits) / 8, bits);
      },
    };
  }
  if (encoding.kind === "fsst") {
    const inner = blockAccessor(encoding.values, buffers, count, context, workspace);
    const symbols = encoding.symbols;
    if (
      symbols.length !== 2312 ||
      new DataView(symbols.buffer, symbols.byteOffset, symbols.byteLength).getUint32(4, true) !==
        0x46535354
    )
      corrupt21("Invalid FSST symbol table");
    if ((symbols[3] as number) & 1) return { ...inner, symbols };
    return inner;
  }
  if (encoding.kind === "variable") {
    if (
      encoding.offsets.kind !== "flat" ||
      ![32, 64].includes(encoding.offsets.bits) ||
      buffers.length !== 1
    )
      unsupported21("Variable mini-blocks require flat 32/64-bit offsets");
    const width = encoding.offsets.bits / 8;
    if (bytes.length < (count + 1) * width) corrupt21("Truncated variable offsets");
    let previous = (count + 1) * width;
    for (let i = 0; i <= count; i++) {
      const offset = Number(unsigned(bytes, i * width, width * 8));
      if (!Number.isSafeInteger(offset) || offset < previous || offset > bytes.length)
        corrupt21("Invalid variable mini-block offset");
      previous = offset;
    }
    return {
      bits: 0,
      get: (index) =>
        bytes.subarray(
          Number(unsigned(bytes, index * width, width * 8)),
          Number(unsigned(bytes, (index + 1) * width, width * 8)),
        ),
    };
  }
  return unsupported21("Unsupported mini-block compressor");
}

function binaryValue(
  context: LanceReadContext,
  encoded: Uint8Array,
  symbols: Uint8Array | undefined,
  logicalType: string,
): unknown {
  if (symbols !== undefined) {
    if (
      symbols.length !== 2312 ||
      new DataView(symbols.buffer, symbols.byteOffset, symbols.byteLength).getUint32(4, true) !==
        0x46535354
    )
      corrupt21("Invalid FSST symbol table");
    if (((symbols[3] ?? 0) & 1) === 0) symbols = undefined;
  }
  let length = encoded.length;
  const symbolCount = symbols?.[0] ?? 0;
  if (symbols !== undefined) {
    length = 0;
    for (let i = 0; i < encoded.length; i++) {
      const code = encoded[i] as number;
      if (code === 255) {
        if (++i >= encoded.length) corrupt21("Truncated FSST escape");
        length++;
      } else {
        const size = symbols[8 + symbolCount * 8 + code] ?? 0;
        if (code >= symbolCount || size < 1 || size > 8) corrupt21("Invalid FSST symbol code");
        length += size;
      }
    }
  }
  context.accountDecodedMemory(
    length * (logicalType === "string" || logicalType === "large_string" ? 2 : 1),
  );
  const scratch = context.leaseDecodedMemory(symbols === undefined ? 0 : length);
  try {
    let bytes = encoded;
    if (symbols !== undefined) {
      bytes = new Uint8Array(length);
      let offset = 0;
      for (let i = 0; i < encoded.length; i++) {
        const code = encoded[i] as number;
        if (code === 255) bytes[offset++] = encoded[++i] as number;
        else {
          const size = symbols[8 + symbolCount * 8 + code] as number;
          bytes.set(symbols.subarray(8 + code * 8, 8 + code * 8 + size), offset);
          offset += size;
        }
      }
    }
    if (logicalType === "string" || logicalType === "large_string") {
      try {
        return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      } catch {
        corrupt21("Invalid UTF-8 in structural string");
      }
    }
    if (logicalType === "binary" || logicalType === "large_binary") return bytes.slice();
    unsupported21("Variable structural values require binary or string columns");
  } finally {
    scratch.release();
  }
}

function packedSmall(bytes: Uint8Array, firstBit: number, bits: number): number {
  let result = 0;
  for (let i = 0; i < bits; i++) {
    const bit = firstBit + i;
    result += (((bytes[Math.floor(bit / 8)] ?? 0) >>> (bit % 8)) & 1) * 2 ** i;
  }
  return result;
}

function fastLanes(
  bytes: Uint8Array,
  base: number,
  bits: number,
  width: number,
  index: number,
): bigint {
  if (width === 0) return 0n;
  const lanes = 1024 / bits;
  const lane = (index % 128) % lanes;
  let group = Math.floor((index % 128) / lanes),
    reversed = 0;
  for (let i = bits / 8; i > 1; i /= 2) {
    reversed = reversed * 2 + (group % 2);
    group = Math.floor(group / 2);
  }
  const row = reversed * 8 + Math.floor(index / 128);
  const bit = row * width,
    word = Math.floor(bit / bits),
    shift = bit % bits;
  const at = (word: number) =>
    BigInt(unsigned(bytes, base + ((word * lanes + lane) * bits) / 8, bits));
  let result = at(word) >> BigInt(shift);
  if (shift + width > bits) result |= at(word + 1) << BigInt(bits - shift);
  return result & ((1n << BigInt(width)) - 1n);
}

function unsigned(bytes: Uint8Array, offset: number, bits: number): number | bigint {
  if (!Number.isSafeInteger(offset) || offset < 0 || offset + bits / 8 > bytes.length)
    corrupt21("Compressed value is outside its buffer");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bits === 8) return view.getUint8(offset);
  if (bits === 16) return view.getUint16(offset, true);
  if (bits === 32) return view.getUint32(offset, true);
  if (bits === 64) return view.getBigUint64(offset, true);
  return corrupt21("Invalid compressed word width");
}
function bufferRange(page: LancePage, index: number): ByteRange {
  const offset = page.bufferOffsets[index],
    length = page.bufferSizes[index];
  if (offset === undefined || length === undefined) corrupt21("Missing structural page buffer");
  return { offset, length };
}
function align8(value: number): number {
  return Math.ceil(value / 8) * 8;
}
