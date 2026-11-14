import { existsSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { decodeVariantColumn } from "hyparquet/src/variant.js";
import { decimalFromParts, eq, isDecimalValue, MemoryBudget, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake, readParquetObjects } from "./index.js";

const metadata = new Uint8Array([1, 0, 0]);

it.each([
  [],
  [0],
  [10, 11, 12, 13],
  [255, 0, 128],
])("preserves typed binary bytes %j without interpreting them as variant headers", (...bytes) => {
  const binary = Uint8Array.from(bytes);
  expect(decodeVariantColumn({ metadata, typed_value: binary })).toBe(binary);
  expect(decodeVariantColumn({ metadata, typed_value: [{ typed_value: binary }] })).toEqual([
    binary,
  ]);
});

it("continues to decode encoded binary in the value wrapper", () => {
  // Variant primitive BINARY: type 15, four-byte little-endian length, payload.
  const payload = new Uint8Array([60, 4, 0, 0, 0, 10, 11, 12, 13]);
  expect(decodeVariantColumn({ metadata, value: payload })).toEqual(
    new Uint8Array([10, 11, 12, 13]),
  );
  expect(
    decodeVariantColumn({ metadata, typed_value: [{ value: payload }, { typed_value: null }] }),
  ).toEqual([new Uint8Array([10, 11, 12, 13]), null]);
});

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/shredded_variant/case-030.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(fixture))(
  "preserves the documented external binary variant in both readers",
  async () => {
    const store = memoryStore();
    await store.put("binary.parquet", readFileSync(fixture));
    const expected = [{ id: 1, var: new Uint8Array([10, 11, 12, 13]) }];
    expect(await readParquetObjects(store, "binary.parquet")).toEqual(expected);
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
    const output = await lake.path("binary.parquet").batchSize(1).run().toOwnedArray();
    try {
      expect(output.value).toEqual(expected);
    } finally {
      output.release();
    }
    expect(pool.usedBytes).toBe(0);
  },
);

for (const [name, path, expectedValue, json] of [
  ["binary", fixture, new Uint8Array([10, 11, 12, 13]), '[{"id":1,"var":"0a0b0c0d"}]'],
  [
    "decimal",
    new URL(
      "../../../fixtures/external/parquet-testing/shredded_variant/case-070.parquet",
      import.meta.url,
    ),
    decimalFromParts(123456789987654321n, 9),
    '[{"id":1,"var":"123456789.987654321"}]',
  ],
  [
    "shredded decimal 26",
    new URL(
      "../../../fixtures/external/parquet-testing/shredded_variant/case-026.parquet",
      import.meta.url,
    ),
    decimalFromParts(123456789987654321n, 9),
    '[{"id":1,"var":"123456789.987654321"}]',
  ],
  [
    "shredded decimal 27",
    new URL(
      "../../../fixtures/external/parquet-testing/shredded_variant/case-027.parquet",
      import.meta.url,
    ),
    decimalFromParts(-123456789987654321n, 9),
    '[{"id":1,"var":"-123456789.987654321"}]',
  ],
  [
    "shredded decimal 28",
    new URL(
      "../../../fixtures/external/parquet-testing/shredded_variant/case-028.parquet",
      import.meta.url,
    ),
    decimalFromParts(9876543210123456789n, 9),
    '[{"id":1,"var":"9876543210.123456789"}]',
  ],
  [
    "shredded decimal 29",
    new URL(
      "../../../fixtures/external/parquet-testing/shredded_variant/case-029.parquet",
      import.meta.url,
    ),
    decimalFromParts(-9876543210123456789n, 9),
    '[{"id":1,"var":"-9876543210.123456789"}]',
  ],
] as const) {
  it.skipIf(!existsSync(path)).each([false, true])(
    `reads ${name} variant over HTTP with ignored ranges=%s`,
    async (ignoreRange) => {
      const bytes = readFileSync(path);
      let reads = 0;
      const server = createServer((request, response) => {
        const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
        if (!match) {
          response.writeHead(400).end();
          return;
        }
        reads++;
        const start = ignoreRange ? 0 : Number(match[1]);
        const end = ignoreRange ? bytes.length : Math.min(bytes.length, Number(match[2]) + 1);
        response.writeHead(ignoreRange ? 200 : 206, {
          "Content-Length": end - start,
          ...(!ignoreRange ? { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` } : {}),
        });
        response.end(bytes.subarray(start, end));
      });
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const pool = new MemoryBudget(64 * 1024 * 1024);
      try {
        const address = server.address();
        if (!address || typeof address === "string") throw new Error("Missing HTTP address");
        const store = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` });
        const expected = [{ id: 1, var: expectedValue }];
        expect(await readParquetObjects(store, "binary.parquet")).toEqual(expected);
        const lake = createParquetLake({
          store,
          memoryPool: pool,
          scanRangeCache: { maxBytes: 0 },
        });
        const output = await lake.path("binary.parquet").batchSize(1).run().toOwnedArray();
        try {
          expect(output.value).toEqual(expected);
        } finally {
          output.release();
        }
        expect(await new Response(lake.path("binary.parquet").streamJson()).text()).toBe(json);
        if (isDecimalValue(expectedValue)) {
          for (const [literal, matches] of [
            [decimalFromParts(expectedValue.coefficient * 10n, expectedValue.scale + 1), true],
            [decimalFromParts(0n, 0), false],
          ] as const) {
            const filtered = await lake
              .path("binary.parquet")
              .where(eq("var", literal))
              .run()
              .toOwnedArray();
            try {
              expect(filtered.value).toEqual(matches ? expected : []);
            } finally {
              filtered.release();
            }
          }
        }
        expect(reads).toBeGreaterThan(0);
        expect(pool.usedBytes).toBe(0);
      } finally {
        await new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
      }
    },
  );
}
