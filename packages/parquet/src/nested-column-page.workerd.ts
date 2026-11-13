import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  MemoryBudget,
  MemoryScope,
  SharedMemoryCache,
  vectorLength,
  vectorValue,
} from "lakeql-core";
import { expect, it } from "vitest";
import { decodeNestedColumnPage } from "./nested-column-page.js";
import { gzipBytes, header, rawBytes, schema } from "./nested-worker-fixture.js";
import { lakeqlParquetParsers } from "./parsers.js";

for (const mib of [64, 128])
  for (const gzip of [false, true]) {
    it(`decodes cached real JSON pages concurrently in workerd (${mib} MiB, gzip=${gzip})`, async () => {
      const pool = new MemoryBudget(mib * 1024 * 1024);
      const cache = new SharedMemoryCache({ maxBytes: 512 * 1024, memoryBudget: pool });
      const encoded = gzip ? gzipBytes : rawBytes;
      cache.setAllocated("page", 256 * 1024, () => {
        const storage = new Uint8Array(256 * 1024);
        storage.set(encoded);
        return storage;
      });
      const cacheCharge = pool.usedBytes;
      const path = getSchemaPath(schema, ["json_field"]);
      const element = path.at(-1)?.element;
      if (!element) throw Error("Missing fixture leaf");
      const decoder: ColumnDecoder = {
        type: "BYTE_ARRAY",
        codec: gzip ? "GZIP" : "UNCOMPRESSED",
        element,
        schemaPath: path,
        pathInSchema: ["json_field"],
        parsers: lakeqlParquetParsers,
      };
      const pageHeader = { ...header, compressed_page_size: encoded.length };
      try {
        const jobs = Array.from({ length: 4 }, async () => {
          const lease = cache.acquire<Uint8Array>("page");
          if (!lease) throw Error("Missing cached page");
          const budget = pool.child(256 * 1024);
          try {
            for (let round = 0; round < 8; round++) {
              const scope = new MemoryScope(budget);
              try {
                const bytes = scope.allocate(c.view, "borrowed page view", () =>
                  lease.value.subarray(0, encoded.length),
                ).value;
                const page = decodeNestedColumnPage(bytes, pageHeader, decoder, undefined, scope);
                if (!page) throw Error("Unsupported fixture page");
                expect(
                  Array.from({ length: vectorLength(page.slots) }, (_, i) =>
                    vectorValue(page.slots, i),
                  ),
                ).toEqual([{ a: 1, b: null }, { a: 1, b: null }, [1, null, 3], null]);
                // Retain each decoded result while all four jobs reach this point.
                await Promise.resolve();
              } finally {
                scope.dispose();
              }
              expect(budget.usedBytes).toBe(0);
            }
          } finally {
            lease.release();
          }
        });
        cache.delete("page");
        expect(cache.get("page")).toBeUndefined();
        expect(pool.usedBytes).toBeGreaterThanOrEqual(cacheCharge);
        await Promise.all(jobs);
        expect(pool.usedBytes).toBe(0);
        expect(pool.peakBytes).toBeLessThanOrEqual(cacheCharge + 4 * 256 * 1024);
      } finally {
        cache.delete("page");
      }
    });
  }
