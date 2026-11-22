import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore } from "lakeql";
import { describe, expect, it } from "vitest";
import { BLOCK_LEN, packedBlockBytes, unpackBlock, unpackSortedBlock } from "./bitpack.js";
import { readBinaryRanges, resolveColumn } from "./column.js";
import { inspectLanceFile } from "./file.js";
import { FstMap } from "./fst.js";
import { decodePositions, decodePosting, phraseMatches } from "./fts.js";
import {
  analyze,
  assertSupportedAnalyzer,
  foldToAscii,
  parseTokenizerParams,
} from "./fts-tokenizer.js";
import { LanceReadContext } from "./io.js";

const defaults = (override: Record<string, unknown> = {}) =>
  parseTokenizerParams(
    JSON.stringify({
      lance_tokenizer: "text",
      base_tokenizer: "simple",
      language: "English",
      with_position: true,
      max_token_length: 40,
      lower_case: true,
      stem: true,
      remove_stop_words: true,
      custom_stop_words: null,
      ascii_folding: true,
      ...override,
    }),
  );

describe("Lance analyzer", () => {
  it("applies the index-time pipeline in order and keeps positions across removed words", () => {
    expect(analyze("The Quick-Brown FOXES jumped over THE lazy dog's café!", defaults())).toEqual([
      { text: "quick", position: 1 },
      { text: "brown", position: 2 },
      { text: "fox", position: 3 },
      { text: "jump", position: 4 },
      { text: "over", position: 5 },
      { text: "lazi", position: 7 },
      { text: "dog", position: 8 },
      { text: "s", position: 9 },
      { text: "cafe", position: 10 },
    ]);
  });

  it("drops overlong tokens by byte length, before any other filter", () => {
    const long = "x".repeat(40);
    expect(
      analyze(`${"x".repeat(39)} ${long} é${"y".repeat(37)}`, defaults({ stem: false })).map(
        (t) => t.text,
      ),
    ).toEqual(["x".repeat(39), `e${"y".repeat(37)}`]);
    expect(analyze(long, defaults({ max_token_length: null, stem: false }))).toHaveLength(1);
  });

  it("supports whitespace, raw, custom stop words and disabled filters", () => {
    expect(
      analyze(
        "Tab\tseparated, E-Mail",
        defaults({ base_tokenizer: "whitespace", stem: false }),
      ).map((t) => t.text),
    ).toEqual(["tab", "separated,", "e-mail"]);
    expect(
      analyze(
        "A Whole Phrase",
        defaults({ base_tokenizer: "raw", remove_stop_words: false, stem: false }),
      ),
    ).toEqual([{ text: "a whole phrase", position: 0 }]);
    expect(
      analyze("Quick fox the", defaults({ custom_stop_words: ["quick"], stem: false })).map(
        (t) => t.text,
      ),
    ).toEqual(["fox", "the"]);
    expect(
      analyze("Café", defaults({ lower_case: false, ascii_folding: false, stem: false })).map(
        (t) => t.text,
      ),
    ).toEqual(["Café"]);
  });

  it("folds Latin letters to ASCII like Lance's filter and leaves other scripts alone", () => {
    expect(foldToAscii("straße café ÅNGSTRÖM Œuvre ł đ þ ŋ ı ſ")).toBe(
      "strasse cafe ANGSTROM OEuvre l d th n i s",
    );
    expect(foldToAscii("日本語 Привет")).toBe("日本語 Привет");
  });

  it("rejects analyzer components it cannot reproduce exactly", () => {
    for (const override of [
      { base_tokenizer: "ngram" },
      { base_tokenizer: "icu" },
      { base_tokenizer: "jieba/default" },
      { lance_tokenizer: "json" },
      { language: "French" },
    ]) {
      expect(
        () => assertSupportedAnalyzer(defaults(override)),
        JSON.stringify(override),
      ).toThrowError(expect.objectContaining({ code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" }));
    }
    // A non-English language is fine when neither stemming nor default stop words apply.
    const french = defaults({ language: "French", stem: false, remove_stop_words: false });
    expect(() => assertSupportedAnalyzer(french)).not.toThrow();
    expect(french.language).toBe("French");
    expect(() => parseTokenizerParams("not json")).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });
});

describe("Lance bit-packed blocks", () => {
  // Reference packer for the BitPacker4x layout: four lanes, lane words interleaved.
  function pack(values: readonly number[], bits: number): Uint8Array {
    if (bits === 0) return new Uint8Array();
    const out = new DataView(new ArrayBuffer(packedBlockBytes(bits)));
    for (let lane = 0; lane < 4; lane += 1) {
      for (let j = 0; j < 32; j += 1) {
        const value = BigInt(values[4 * j + lane] as number);
        const bit = j * bits;
        const word = bit >>> 5;
        const shift = bit & 31;
        const at = (4 * word + lane) * 4;
        const merged = BigInt(out.getUint32(at, true)) | (value << BigInt(shift));
        out.setUint32(at, Number(merged & 0xffffffffn), true);
        if (shift + bits > 32) {
          const next = (4 * (word + 1) + lane) * 4;
          out.setUint32(next, (out.getUint32(next, true) | Number(merged >> 32n)) >>> 0, true);
        }
      }
    }
    return new Uint8Array(out.buffer);
  }

  it.each([0, 1, 3, 7, 8, 13, 17, 24, 31, 32])("round-trips %i-bit blocks", (bits) => {
    const max = bits === 32 ? 0xffffffff : 2 ** bits - 1;
    const values = Array.from(
      { length: BLOCK_LEN },
      (_v, i) => (bits === 0 ? 0 : (i * 2654435761) % (max + 1)) >>> 0,
    );
    const out = new Uint32Array(BLOCK_LEN);
    unpackBlock(pack(values, bits), 0, bits, out);
    expect([...out]).toEqual(values);
  });

  it("restores a sorted block from its deltas", () => {
    const sorted = Array.from({ length: BLOCK_LEN }, (_v, i) => 1000 + i * 3 + Math.floor(i / 5));
    const deltas = sorted.map((value, i) => value - (i === 0 ? 1000 : (sorted[i - 1] as number)));
    const bits = Math.ceil(Math.log2(Math.max(...deltas) + 1));
    const out = new Uint32Array(BLOCK_LEN);
    unpackSortedBlock(pack(deltas, bits), 0, bits, 1000, out);
    expect([...out]).toEqual(sorted);
  });
});

describe("Lance posting and position blocks", () => {
  const u32 = (...values: number[]) => {
    const bytes = new Uint8Array(values.length * 4);
    const view = new DataView(bytes.buffer);
    for (const [i, value] of values.entries()) view.setUint32(i * 4, value, true);
    return bytes;
  };
  const concat = (...parts: Uint8Array[]) => {
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let at = 0;
    for (const part of parts) {
      out.set(part, at);
      at += part.length;
    }
    return out;
  };

  it("decodes fixed32 and varint-delta tail blocks", () => {
    const score = u32(0);
    const fixed = concat(score, u32(3, 9, 400), u32(1, 2, 7));
    expect(decodePosting([fixed], 3, "fixed32")).toEqual({
      docIds: Uint32Array.of(3, 9, 400),
      freqs: Uint32Array.of(1, 2, 7),
    });
    const varint = concat(score, Uint8Array.of(3, 6, 0x87, 0x03), Uint8Array.of(1, 2, 7));
    expect(decodePosting([varint], 3, "varint_delta")).toEqual({
      docIds: Uint32Array.of(3, 9, 400),
      freqs: Uint32Array.of(1, 2, 7),
    });
  });

  it("rejects truncated or inconsistent posting lists", () => {
    expect(() => decodePosting([], 3, "fixed32")).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    expect(() => decodePosting([concat(u32(0), u32(1))], 3, "fixed32")).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    expect(() => decodePosting([Uint8Array.of(0, 0, 0, 0, 0x80)], 1, "varint_delta")).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });

  it("decodes remainder position lists and checks their length header", () => {
    expect(decodePositions([u32(3), u32(2, 5, 9)])).toEqual([2, 5, 9]);
    expect(() => decodePositions([u32(4), u32(2, 5, 9)])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    expect(() => decodePositions([])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });

  it("matches phrases with exact positions, gaps and slop", () => {
    // "quick brown fox" at positions 4,5,6 and again at 10,12,13.
    const positions = [
      [4, 10],
      [5, 12],
      [6, 13],
    ];
    expect(phraseMatches(positions, [0, 1, 2], 0)).toBe(true);
    expect(phraseMatches([[10], [12], [13]], [0, 1, 2], 0)).toBe(false);
    expect(phraseMatches([[10], [12], [13]], [0, 1, 2], 1)).toBe(true);
    expect(phraseMatches([[10], [13], [14]], [0, 1, 2], 1)).toBe(false);
    // Out-of-order terms never match, whatever the slop.
    expect(phraseMatches([[7], [3]], [0, 1], 5)).toBe(false);
    // A removed stop word leaves a gap in query positions that documents must also have.
    expect(phraseMatches([[0]], [1], 0)).toBe(false);
    expect(phraseMatches([[3]], [1], 0)).toBe(true);
    expect(phraseMatches([[2], [3]], [0, 1], 0)).toBe(true);
  });
});

describe("Lance FST token set", () => {
  it("decodes a real Lance token set: ordered, dense ids, exact lookups, bounded search", async () => {
    const root = resolve(import.meta.dirname, "../fixtures/fts-inverted-v2.0.lance/_indices");
    const store = memoryStore();
    for (const dir of await readdir(root)) {
      for (const name of await readdir(resolve(root, dir))) {
        await store.put(
          `i/${dir}/${name}`,
          new Uint8Array(await readFile(resolve(root, dir, name))),
        );
      }
    }
    const dir = (await readdir(root))[0] as string;
    const context = new LanceReadContext(
      store,
      { maxMemoryBytes: 64e6 },
      {
        snapshotMetadataBytes: 0,
        dataMetadataBytes: 0,
        logicalBytesRequested: 0,
        physicalBytesRequested: 0,
        rangeRequests: 0,
        cacheHits: 0,
        cacheMisses: 0,
        peakMemoryBytes: 0,
        rowsDecoded: 0,
        fragments: new Set(),
        pages: new Set(),
      },
      0,
      () => 0,
      { coalesceGapBytes: 0, maxCoalescedRangeBytes: 1 << 20 },
    );
    const file = await inspectLanceFile(context, `i/${dir}/part_0_tokens.lance`);
    const column = resolveColumn(file, { name: "_token_fst_bytes" }).column;
    const read = await readBinaryRanges(context, file, column, [{ start: 0, end: 1 }]);
    const [[bytes]] = read.values as [[Uint8Array]];
    const fst = new FstMap(bytes.slice());
    const entries = fst.entries();
    expect(entries.length).toBe(fst.length);
    const words = entries.map(([key]) => new TextDecoder().decode(key));
    expect([...words].sort()).toEqual(words);
    expect(entries.map(([, id]) => id).sort((a, b) => a - b)).toEqual(entries.map((_e, i) => i));
    for (const [key, id] of entries) expect(fst.get(key)).toBe(id);
    for (const word of ["quick", "brown", "fox", "jump", "zebra", "term7"]) {
      expect(words, word).toContain(word);
    }
    expect(fst.get(new TextEncoder().encode("nonexistent"))).toBeUndefined();
    expect(fst.get(new Uint8Array())).toBeUndefined();
    // Early termination from the visitor stops the walk.
    const seen: string[] = [];
    fst.search(
      { start: () => 0, canMatch: () => true, isMatch: () => true, accept: () => 0 },
      (key) => {
        seen.push(new TextDecoder().decode(key));
        return seen.length < 3;
      },
    );
    expect(seen).toEqual(words.slice(0, 3));
  });

  it("rejects malformed token sets", () => {
    expect(() => new FstMap(new Uint8Array(10))).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    const bad = new Uint8Array(40);
    new DataView(bad.buffer).setBigUint64(0, 2n, true);
    expect(() => new FstMap(bad)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" }),
    );
  });
});
