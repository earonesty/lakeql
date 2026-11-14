import { brotliCompressSync, constants, createBrotliCompress } from "node:zlib";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { BROTLI_SLOT_BYTES, BROTLI_STATE_BYTES, scopedParquetCompressors } from "./compressors.js";
import { decompressBrotliInto } from "./vendor/brotli.js";

function workspace(limit = Number.MAX_SAFE_INTEGER, slotBytes = 0) {
  const live = new Set<Uint8Array>();
  let used = 0;
  let peak = 0;
  return {
    live,
    get peak() {
      return peak;
    },
    allocate(bytes: number) {
      if (used + bytes + slotBytes > limit) throw new Error("workspace limit");
      const buffer = new Uint8Array(bytes);
      live.add(buffer);
      used += bytes + slotBytes;
      peak = Math.max(peak, used);
      return buffer;
    },
    release(buffer: Uint8Array) {
      expect(live.delete(buffer)).toBe(true);
      used -= buffer.length + slotBytes;
    },
  };
}

it("rejects trailing input even beyond the bit-reader buffer", () => {
  const encoded = brotliCompressSync(new TextEncoder().encode("hello"));
  for (const extra of [1, 4096, 8192]) {
    const input = new Uint8Array(encoded.length + extra);
    input.set(encoded);
    const memory = workspace();
    expect(() =>
      decompressBrotliInto(input, new Uint8Array(5), memory.allocate, memory.release),
    ).toThrow();
    expect(memory.live.size).toBe(0);
  }
});

it("rejects overlapping input/output before allocating workspace", () => {
  const encoded = brotliCompressSync(new TextEncoder().encode("hello"));
  const memory = workspace();
  expect(() =>
    decompressBrotliInto(encoded, encoded.subarray(0, 5), memory.allocate, memory.release),
  ).toThrow(/overlap/);
  expect(memory.peak).toBe(0);
});

it("fits exact power-of-two output in one ring and preserves history across flushed blocks", async () => {
  const block = new TextEncoder().encode("history ".repeat(8192));
  let largest = 0;
  const memory = workspace();
  const allocate = (bytes: number) => {
    largest = Math.max(largest, bytes);
    return memory.allocate(bytes);
  };
  const compressed = brotliCompressSync(block);
  expect(
    decompressBrotliInto(compressed, new Uint8Array(block.length), allocate, memory.release),
  ).toEqual(block);
  expect(largest).toBe(block.length + 37);
  expect(memory.live.size).toBe(0);

  const encoder = createBrotliCompress();
  const chunks: Buffer[] = [];
  encoder.on("data", (chunk: Buffer) => chunks.push(chunk));
  const complete = new Promise<void>((resolve, reject) => {
    encoder.on("end", resolve);
    encoder.on("error", reject);
  });
  const expected = new Uint8Array(block.length * 3);
  for (let index = 0; index < 3; index++) {
    expected.set(block, index * block.length);
    encoder.write(block);
    await new Promise<void>((resolve, reject) =>
      encoder.flush(constants.BROTLI_OPERATION_FLUSH, (error) =>
        error ? reject(error) : resolve(),
      ),
    );
  }
  encoder.end();
  await complete;
  expect(
    decompressBrotliInto(
      Buffer.concat(chunks),
      new Uint8Array(expected.length),
      allocate,
      memory.release,
    ),
  ).toEqual(expected);
  expect(memory.live.size).toBe(0);
});

it("decodes raw blocks and ring-buffer wrapping with offset views", () => {
  let seed = 12345;
  const raw = Uint8Array.from({ length: 131073 }, () => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return seed >>> 24;
  });
  for (const input of [raw, new TextEncoder().encode("ring buffer history ".repeat(20000))]) {
    const encoded = brotliCompressSync(input, { params: { [constants.BROTLI_PARAM_LGWIN]: 10 } });
    const backing = new Uint8Array(input.length + 16).fill(99);
    const output = backing.subarray(8, -8);
    const memory = workspace();
    expect(decompressBrotliInto(encoded, output, memory.allocate, memory.release)).toEqual(input);
    expect(backing.subarray(0, 8)).toEqual(new Uint8Array(8).fill(99));
    expect(backing.subarray(-8)).toEqual(new Uint8Array(8).fill(99));
    expect(memory.live.size).toBe(0);
  }
});

it("charges scoped output and workspace, with atomic cleanup on failure", () => {
  const input = new TextEncoder().encode("bounded output ".repeat(1000));
  const encoded = brotliCompressSync(input);
  const measured = workspace(Number.MAX_SAFE_INTEGER, BROTLI_SLOT_BYTES);
  decompressBrotliInto(encoded, new Uint8Array(input.length), measured.allocate, measured.release);
  const required = input.length + measured.peak + BROTLI_STATE_BYTES;
  for (const limit of [input.length - 1, required - 1, required]) {
    const budget = new MemoryBudget(limit);
    const scope = new MemoryScope(budget);
    const codec = scopedParquetCompressors(scope).BROTLI;
    if (!codec) throw new Error("Missing Brotli codec");
    if (limit < required) {
      expect(() => codec(encoded, input.length)).toThrow(/memory budget/);
      expect(budget.usedBytes).toBe(0);
    } else {
      expect(codec(encoded, input.length)).toEqual(input);
      expect(budget.usedBytes).toBe(input.length);
    }
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});

it.each([
  0, 1, 4, 11,
])("decodes fixed output at Brotli quality %s and releases workspace", (quality) => {
  for (const text of ["", "hello", "The quick brown fox jumps over the lazy dog. ".repeat(5000)]) {
    const input = new TextEncoder().encode(text);
    const compressed = brotliCompressSync(input, {
      params: { [constants.BROTLI_PARAM_QUALITY]: quality },
    });
    const memory = workspace();
    const output = new Uint8Array(input.length);
    expect(decompressBrotliInto(compressed, output, memory.allocate, memory.release)).toBe(output);
    expect(output).toEqual(input);
    expect(memory.live.size).toBe(0);
  }
});

it("rejects wrong output lengths and truncated input without retaining workspace", () => {
  const input = new TextEncoder().encode(
    "a document with dictionary words and repeated words".repeat(100),
  );
  const compressed = brotliCompressSync(input);
  for (const length of [0, input.length - 1, input.length + 1]) {
    const memory = workspace();
    expect(() =>
      decompressBrotliInto(compressed, new Uint8Array(length), memory.allocate, memory.release),
    ).toThrow();
    expect(memory.live.size).toBe(0);
  }
  for (let end = 0; end < compressed.length; end++) {
    const memory = workspace();
    expect(() =>
      decompressBrotliInto(
        compressed.subarray(0, end),
        new Uint8Array(input.length),
        memory.allocate,
        memory.release,
      ),
    ).toThrow();
    expect(memory.live.size).toBe(0);
  }
});

it("admits workspace before allocation and releases it on admission failure", () => {
  const input = new TextEncoder().encode("dictionary and compression ".repeat(10000));
  const compressed = brotliCompressSync(input);
  const measured = workspace();
  decompressBrotliInto(
    compressed,
    new Uint8Array(input.length),
    measured.allocate,
    measured.release,
  );
  for (const limit of [0, 40, 10000, measured.peak - 1, measured.peak]) {
    const memory = workspace(limit);
    const decode = () =>
      decompressBrotliInto(
        compressed,
        new Uint8Array(input.length),
        memory.allocate,
        memory.release,
      );
    if (limit < measured.peak) expect(decode).toThrow("workspace limit");
    else expect(decode()).toEqual(input);
    expect(memory.peak).toBeLessThanOrEqual(limit);
    expect(memory.live.size).toBe(0);
  }
});
