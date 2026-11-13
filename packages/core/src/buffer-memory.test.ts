import { describe, expect, it } from "vitest";
import { BufferMemory } from "./buffer-memory.js";
import { jsonSafeValue } from "./evaluator.js";
import { stableStringify } from "./manifest.js";

const bytes = (value: unknown) =>
  new TextEncoder().encode(stableStringify(jsonSafeValue(value))).byteLength;

describe("serialized buffer accounting", () => {
  it("matches list serialization for nested and escaped values through replacements and resets", () => {
    const values: unknown[] = [];
    const ledger = new BufferMemory<unknown>((value) => bytes([value]) - 2);
    const examples = [
      null,
      undefined,
      123n,
      99999999999999999999999n,
      new Uint8Array([0, 255]),
      { a: 'é😀\n"\\', b: [NaN, undefined, -0], nested: { x: "\ud800" } },
    ];
    for (const value of examples) {
      values.push(value);
      ledger.add(value);
      expect(ledger.bytes).toBe(bytes(values));
    }
    while (values.length > 0) {
      ledger.remove(values.shift());
      expect(ledger.bytes).toBe(bytes(values));
    }
    const restored = new BufferMemory<unknown>((value) => bytes([value]) - 2, examples);
    expect(restored.bytes).toBe(bytes(examples));
    restored.clear();
    expect(restored.bytes).toBe(2);
  });

  it("measures each appended item once instead of reserializing the buffer", () => {
    let measured = 0;
    const ledger = new BufferMemory<number>((value) => {
      measured += 1;
      return bytes([value]) - 2;
    });
    for (let i = 0; i < 10_000; i += 1) ledger.add(i);
    expect(measured).toBe(10_000);
    expect(ledger.bytes).toBe(bytes(Array.from({ length: 10_000 }, (_, i) => i)));
  });
});
