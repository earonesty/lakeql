import { defineConfig } from "tsup";

// `lakeql` is published as a single self-contained package: the internal
// `lakeql-*` workspace sources are inlined, while the real runtime dependencies
// (Parquet/Avro/crypto/geo libraries) stay external and are declared in
// package.json so consumers install one package with a small dependency set.
export default defineConfig({
  entry: {
    index: "src/index.ts",
    fetch: "src/fetch.ts",
    node: "src/node.ts",
    cloudflare: "src/cloudflare.ts",
    // The CLI lives in the lakeql-cli workspace source and is bundled in so a
    // global install exposes a `lakeql` command. Shebang is preserved by tsup.
    bin: "../cli/src/bin.ts",
  },
  format: ["esm"],
  // Type declarations only for the library entries (the CLI bin needs none).
  // resolve inlines the lakeql-* workspace types so consumers don't need the
  // (unpublished) internal packages for types.
  dts: {
    resolve: true,
    entry: {
      index: "src/index.ts",
      fetch: "src/fetch.ts",
      node: "src/node.ts",
      cloudflare: "src/cloudflare.ts",
    },
  },
  clean: true,
  treeshake: true,
  sourcemap: false,
  // Inline the workspace packages; keep declared runtime deps external.
  noExternal: [/^lakeql-/],
  external: [
    "@turf/boolean-contains",
    "@turf/boolean-intersects",
    "avsc",
    "aws4fetch",
    "fast-xml-parser",
    "fzstd",
    "h3-js",
    "hyparquet",
    "hyparquet-writer",
  ],
});
