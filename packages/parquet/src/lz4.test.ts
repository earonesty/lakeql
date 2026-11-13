import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { scopedParquetCompressors } from "./compressors.js";
import { createParquetLake, readParquetObjects } from "./index.js";
import { decodeLegacyLz4Into, decodeLz4Into } from "./lz4.js";

it("decodes literals, extended lengths and overlapping matches into exact output", () => {
  const output = new Uint8Array(13);
  expect(decodeLz4Into(Uint8Array.of(0x13, 97, 1, 0, 0x50, 98, 99, 100, 101, 102), output)).toBe(
    output,
  );
  expect(new TextDecoder().decode(output)).toBe("aaaaaaaabcdef");
  expect(
    decodeLz4Into(Uint8Array.from([0xf0, 255, 0, ...Array(270).fill(42)]), new Uint8Array(270)),
  ).toEqual(new Uint8Array(270).fill(42));
  expect(decodeLz4Into(Uint8Array.of(0), new Uint8Array(0))).toHaveLength(0);
});

it("rejects malformed lengths, offsets, aliases and incomplete output", () => {
  for (const [bytes, size] of [
    [[0xf0], 20],
    [[0xf0, 255], 300],
    [[0x20, 1], 2],
    [[0x10, 1, 0, 0], 5],
    [[0x10, 1, 2, 0], 5],
    [[0x10, 1, 1], 5],
    [[0x10, 1, 1, 0], 5],
    [[0x1f, 1, 1, 0], 40],
    [[0x10, 1], 0],
    [[0x10, 1], 2],
  ] as const)
    expect(() => decodeLz4Into(Uint8Array.from(bytes), new Uint8Array(size))).toThrow(/LZ4/);
  const alias = Uint8Array.of(0x10, 1);
  expect(() => decodeLz4Into(alias, alias.subarray(1))).toThrow(/LZ4/);
});

it("admits exact output capacity and releases failed decode allocations", () => {
  const scope = new MemoryScope(new MemoryBudget(5));
  expect(codec(scope)(Uint8Array.of(0x50, 104, 101, 108, 108, 111), 5)).toEqual(
    new TextEncoder().encode("hello"),
  );
  expect(scope.budget.usedBytes).toBe(5);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(4));
  expect(() => codec(tight)(Uint8Array.of(0), 5)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  expect(() => codec(tight)(Uint8Array.of(0xf0), 4)).toThrow(/LZ4/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

for (const [name, count, hash] of [
  [
    "hadoop_lz4_compressed.parquet",
    4,
    "162a2c130541d3897a3bdb0f64861cff3a2d753c02d06cf089ef6452ef83bbe2",
  ],
  [
    "hadoop_lz4_compressed_larger.parquet",
    10000,
    "54c2deb990b6cadfea35824768b68c4be5e319188860ed08e22a4e5e639b1ac5",
  ],
  [
    "non_hadoop_lz4_compressed.parquet",
    4,
    "162a2c130541d3897a3bdb0f64861cff3a2d753c02d06cf089ef6452ef83bbe2",
  ],
  [
    "lz4_raw_compressed.parquet",
    4,
    "162a2c130541d3897a3bdb0f64861cff3a2d753c02d06cf089ef6452ef83bbe2",
  ],
  [
    "lz4_raw_compressed_larger.parquet",
    10000,
    "54c2deb990b6cadfea35824768b68c4be5e319188860ed08e22a4e5e639b1ac5",
  ],
] as const) {
  const fixture = new URL(
    `../../../fixtures/external/parquet-testing/data/${name}`,
    import.meta.url,
  );
  it.skipIf(!existsSync(fixture))(
    `matches independently verified paired-fixture row digest for ${name}`,
    async () => {
      const store = memoryStore();
      await store.put(name, readFileSync(fixture));
      const lake = createParquetLake({ store });
      for (const rows of [
        await readParquetObjects(store, name),
        await lake.path(name).batchSize(127).run().toArray(),
      ]) {
        expect(rows).toHaveLength(count);
        // DuckDB JSON represents bigint and this fixture's ASCII binary column as strings.
        // Normalize only for comparison; public binary values remain Uint8Array.
        const json = JSON.stringify(rows, (_key, value) =>
          typeof value === "bigint"
            ? String(value)
            : value instanceof Uint8Array
              ? new TextDecoder("utf-8", { fatal: true }).decode(value)
              : value,
        );
        expect(createHash("sha256").update(json).digest("hex")).toBe(hash);
      }
    },
  );
}

function codec(scope: MemoryScope) {
  const decode = scopedParquetCompressors(scope).LZ4_RAW;
  if (decode === undefined) throw new Error("Missing LZ4 raw codec");
  return decode;
}

it("decodes Hadoop blocks and multiple independent chunks within exact output", () => {
  const bytes = Uint8Array.of(
    0,
    0,
    0,
    8,
    0,
    0,
    0,
    4,
    0x30,
    97,
    98,
    99,
    0,
    0,
    0,
    6,
    0x50,
    100,
    101,
    102,
    103,
    104,
  );
  expect(new TextDecoder().decode(decodeLegacyLz4Into(bytes, new Uint8Array(8)))).toBe("abcdefgh");
  const block = Uint8Array.of(0, 0, 0, 1, 0, 0, 0, 2, 0x10, 120);
  expect(
    new TextDecoder().decode(
      decodeLegacyLz4Into(Uint8Array.from([...block, ...block]), new Uint8Array(2)),
    ),
  ).toBe("xx");
  for (const invalid of [
    bytes.subarray(0, bytes.length - 1),
    Uint8Array.from([...bytes, 1]),
    Uint8Array.of(0, 0, 0, 8, 0, 0, 0, 0),
  ])
    expect(() => decodeLegacyLz4Into(invalid, new Uint8Array(8))).toThrow(/LZ4/);
  expect(() => decodeLegacyLz4Into(bytes, new Uint8Array(7))).toThrow(/LZ4/);
  expect(() => decodeLegacyLz4Into(bytes, new Uint8Array(9))).toThrow(/LZ4/);
  const scope = new MemoryScope(new MemoryBudget(8));
  const decode = scopedParquetCompressors(scope).LZ4;
  if (!decode) throw new Error("Missing legacy LZ4");
  expect(decode(bytes, 8)).toEqual(new TextEncoder().encode("abcdefgh"));
  expect(scope.budget.usedBytes).toBe(8);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
