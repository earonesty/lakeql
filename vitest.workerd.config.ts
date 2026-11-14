import { existsSync, readFileSync } from "node:fs";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

const dictionaryFixture = new URL(
  "./fixtures/external/parquet-testing/data/nation.dict-malformed.parquet",
  import.meta.url,
);

const decimalFixture = new URL(
  "./fixtures/external/parquet-testing/shredded_variant/case-070.parquet",
  import.meta.url,
);

const shreddedDecimalFixture = new URL(
  "./fixtures/external/parquet-testing/shredded_variant/case-028.parquet",
  import.meta.url,
);

export default defineConfig({
  plugins: [
    cloudflareTest({
      miniflare: {
        r2Buckets: ["MEMORY_TEST_BUCKET"],
        bindings: {
          BSON_FIXTURE: readFileSync(
            new URL("./fixtures/external/parquet-testing/data/bson.parquet", import.meta.url),
          ).toString("base64"),
          ALP_FIXTURE: readFileSync(
            new URL(
              "./fixtures/external/parquet-testing/data/alp_extended.zstd.parquet",
              import.meta.url,
            ),
          ).toString("base64"),
          SHREDDED_DECIMAL_VARIANT_FIXTURE: existsSync(shreddedDecimalFixture)
            ? readFileSync(shreddedDecimalFixture).toString("base64")
            : "",
          DECIMAL_VARIANT_FIXTURE: existsSync(decimalFixture)
            ? readFileSync(decimalFixture).toString("base64")
            : "",
          DICTIONARY_EXTENT_FIXTURE: existsSync(dictionaryFixture)
            ? readFileSync(dictionaryFixture).toString("base64")
            : "",
        },
        compatibilityDate: "2026-06-13",
        compatibilityFlags: ["nodejs_compat"],
      },
    }),
  ],
  test: {
    include: ["packages/**/*.workerd.ts"],
  },
});
