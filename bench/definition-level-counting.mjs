import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { readNumericPage } from "../packages/parquet/dist/numeric-page.js";

const require = createRequire(new URL("../packages/parquet/package.json", import.meta.url));
const { parquetWriteBuffer } = await import(pathToFileURL(require.resolve("hyparquet-writer")));
const { getSchemaPath } = await import(
  new URL("../packages/parquet/node_modules/hyparquet/src/schema.js", import.meta.url)
);
const { DEFAULT_PARSERS } = await import(
  new URL("../packages/parquet/node_modules/hyparquet/src/convert.js", import.meta.url)
);
const moduleUrl = new URL("../packages/parquet/dist/numeric-page.js", import.meta.url);
const source = await readFile(moduleUrl, "utf8");
const sourceSha256 = createHash("sha256").update(source).digest("hex");
const start = source.indexOf("    if (maximum > 0) {");
const end = source.indexOf("    if (v2) {", start);
assert.ok(start > 0 && end > start);
let baseline =
  source.slice(0, start) +
  `
    if (maximum > 0) {
      levels = scope.allocate(count * (maximum <= 255 ? 1 : 4), "baseline levels", () => maximum <= 255 ? new Uint8Array(count) : new Uint32Array(count)).value;
      readLevelRuns(reader, 32 - Math.clz32(maximum), levels, v2?.definition_levels_byte_length);
      present = 0;
      for (let i = 0; i < levels.length; i++) {
        const level = levels[i];
        if (level > maximum) throw new Error("Invalid definition level");
        if (level === maximum) present++;
        levels[i] = level === maximum ? 1 : 0;
      }
      if (present === count) { scope.releaseAllocation(levels); levels = undefined; }
    }
` +
  source.slice(end);
baseline = baseline.replace(
  /from "([^"]+)"/g,
  (_, spec) =>
    `from "${spec.startsWith(".") ? new URL(spec, moduleUrl).href : spec.startsWith("hyparquet/") ? new URL(`../packages/parquet/node_modules/${spec}`, import.meta.url).href : new URL("../packages/core/dist/index.js", import.meta.url).href}"`,
);
const { readNumericPage: buffered } = await import(
  `data:text/javascript;base64,${Buffer.from(baseline).toString("base64")}`
);
const fixture = parquetWriteBuffer({
  columnData: [{ name: "id", type: "INT32", data: [1, null] }],
});
const metadata = await readParquetMetadataFromFile({
  byteLength: fixture.byteLength,
  async slice(a, b) {
    return fixture.slice(a, b);
  },
});
const column = metadata.row_groups[0].columns[0].meta_data;
const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
const decoder = {
  ...column,
  codec: "UNCOMPRESSED",
  pathInSchema: column.path_in_schema,
  schemaPath,
  element: schemaPath.at(-1).element,
  parsers: DEFAULT_PARSERS,
};
function varint(value) {
  const out = [];
  do {
    const part = value % 128;
    value = Math.floor(value / 128);
    out.push(part | (value ? 128 : 0));
  } while (value);
  return out;
}
for (const count of [1024, 65536])
  for (const pattern of ["present", "null", "mixed"]) {
    const present = pattern === "present" ? count : pattern === "null" ? 0 : count / 2;
    const runs =
      pattern === "mixed"
        ? Uint8Array.from([...varint((count / 8) * 2 + 1), ...new Uint8Array(count / 8).fill(0x55)])
        : Uint8Array.from([...varint(count * 2), pattern === "present" ? 1 : 0]);
    const bytes = new Uint8Array(runs.length + present * 4);
    bytes.set(runs);
    const view = new DataView(bytes.buffer);
    for (let i = 0; i < present; i++) view.setInt32(runs.length + i * 4, i, true);
    const header = {
      type: "DATA_PAGE_V2",
      compressed_page_size: bytes.length,
      uncompressed_page_size: bytes.length,
      data_page_header_v2: {
        num_values: count,
        num_rows: count,
        num_nulls: count - present,
        encoding: "PLAIN",
        definition_levels_byte_length: runs.length,
        repetition_levels_byte_length: 0,
        is_compressed: false,
      },
    };
    for (const [strategy, decode] of [
      ["buffer-normalize", buffered],
      ["lazy-mask", readNumericPage],
    ]) {
      const samples = [];
      let peakBytes;
      for (let trial = 0; trial < 10; trial++) {
        const budget = new MemoryBudget(count * 5);
        const scope = new MemoryScope(budget);
        const start = performance.now();
        const result = decode(bytes, header, decoder, scope);
        const elapsed = performance.now() - start;
        assert.equal(result.values.length, present);
        for (let i = 0; i < present; i++) assert.equal(result.values[i], i);
        if (pattern === "present") assert.equal(result.definitionLevels, undefined);
        else
          for (let i = 0; i < count; i++)
            assert.equal(result.definitionLevels[i], pattern === "null" ? 0 : 1 - (i % 2));
        peakBytes = budget.peakBytes;
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
        if (trial >= 5) samples.push(elapsed);
      }
      samples.sort((a, b) => a - b);
      console.log(
        JSON.stringify({
          count,
          pattern,
          strategy,
          medianMs: samples[2],
          peakBytes,
          sourceSha256,
          scope:
            "Local uncompressed V2 decoder; borrowed page; baseline allocation strategy reconstructed using current run parser; excludes cloud, codecs and whole heap",
        }),
      );
    }
  }
