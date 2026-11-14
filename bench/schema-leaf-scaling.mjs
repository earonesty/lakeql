import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { schemaLeafIndex } from "../packages/parquet/dist/schema-leaves.js";
import { getSchemaPath } from "../packages/parquet/node_modules/hyparquet/src/schema.js";

const reports = [];
for (const width of [64, 256, 1024]) {
  for (const groups of [1, 8]) {
    const schema = [{ name: "root", num_children: width }];
    for (let i = 0; i < width; i++) schema.push({ name: `value.${i}`, type: "INT32" });
    const paths = schema.slice(1).map((element) => [element.name]);
    const samples = { tree: [], index: [] };
    for (let trial = 0; trial < 7; trial++) {
      for (const mode of trial % 2 ? ["index", "tree"] : ["tree", "index"]) {
        const started = performance.now();
        const index = mode === "index" ? schemaLeafIndex(schema) : undefined;
        for (let group = 0; group < groups; group++) {
          for (let column = 0; column < width; column++) {
            const element =
              index === undefined
                ? getSchemaPath(schema, paths[column]).at(-1).element
                : index.get(JSON.stringify(paths[column]));
            assert.equal(element, schema[column + 1]);
          }
        }
        if (trial >= 2) samples[mode].push(performance.now() - started);
      }
    }
    reports.push({
      width,
      groups,
      lookups: width * groups,
      samplesMs: samples,
      medianMs: Object.fromEntries(
        Object.entries(samples).map(([mode, values]) => [
          mode,
          [...values].sort((a, b) => a - b)[2],
        ]),
      ),
    });
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local Node schema lookup only; index construction included; two warmups/five samples with alternating order. Identity checked for every lookup. Not a full footer, cloud-query or heap benchmark.",
      node: process.version,
      indexSha256: createHash("sha256")
        .update(
          await readFile(new URL("../packages/parquet/dist/schema-leaves.js", import.meta.url)),
        )
        .digest("hex"),
      reports,
    },
    null,
    2,
  ),
);
