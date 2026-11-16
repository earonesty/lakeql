// Run after pnpm build. Uses bounded file ranges; never loads the dataset into memory.
import { open, readFile, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { openLanceDataset } from "../packages/lance/dist/index.js";

const root = resolve(process.argv[2] ?? "packages/lance/fixtures/spatial-default");
const expected = JSON.parse(await readFile(resolve(root, "expected.json"), "utf8"));
const dataRoot = resolve(root, expected.datasetPath ?? ".");
const store = {
  async head(path) {
    return { size: (await stat(resolve(dataRoot, path.replace(/^dataset\//u, "")))).size };
  },
  async getRange(path, { offset, length }) {
    const file = await open(resolve(dataRoot, path.replace(/^dataset\//u, "")), "r");
    try {
      const buffer = new Uint8Array(length);
      const { bytesRead } = await file.read(buffer, 0, length, offset);
      if (bytesRead !== length) throw new Error("Truncated range");
      return buffer;
    } finally {
      await file.close();
    }
  },
  async get() {
    throw new Error("Full-object read forbidden");
  },
  async put() {
    throw new Error("Read-only benchmark");
  },
  async delete() {
    throw new Error("Read-only benchmark");
  },
  async list() {
    throw new Error("Listing forbidden");
  },
};
const budget = {
  maxBytes: 512 * 1024,
  maxMemoryBytes: 512 * 1024,
  maxRangeRequests: 32,
  maxOutputRows: 100,
  maxRowsDecoded: 10000,
  maxConcurrentReads: 2,
};
const dataset = await openLanceDataset({
  store,
  path: "dataset",
  version: expected.datasetVersion,
  budget,
});
const trials = [];
for (let trial = 0; trial < 5; trial++) {
  global.gc?.();
  const before = process.memoryUsage();
  const started = performance.now();
  const result = await dataset.spatialRows({
    snapshotId: dataset.snapshotId,
    index: "geometry_idx",
    select: ["serial"],
    query: {
      predicate: "intersects",
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [10, 10],
            [14, 10],
            [14, 14],
            [10, 14],
            [10, 10],
          ],
        ],
      },
    },
  });
  const serials = result.rows.map((row) => row.serial).sort((a, b) => a - b);
  if (
    JSON.stringify(serials) !==
    JSON.stringify(
      expected.queries.intersects.map((row) => (typeof row === "number" ? row : row.serial)),
    )
  )
    throw new Error("Result differs from pylance ground truth");
  const after = process.memoryUsage();
  trials.push({
    elapsedMs: +(performance.now() - started).toFixed(2),
    heapDeltaBytes: after.heapUsed - before.heapUsed,
    rssDeltaBytes: after.rss - before.rss,
    rows: result.rows.length,
    pagesVisited: result.pagesVisited,
    candidatesExamined: result.candidatesExamined,
    bytes: result.stats.physicalBytesRequested,
    requests: result.stats.rangeRequests,
    peakAccountedMemoryBytes: result.stats.peakMemoryBytes,
  });
}
console.log(
  JSON.stringify(
    { producer: expected.producer, datasetRows: expected.rowCount, budget, trials },
    null,
    2,
  ),
);
