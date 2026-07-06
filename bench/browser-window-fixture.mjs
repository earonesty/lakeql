import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { writeParquet } from "../packages/parquet/dist/index.js";

const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const outputPath = "bench/generated/browser-r2/window-events.parquet";

const rows = buildRows();
const store = {
  async get() {
    return null;
  },
  async getRange() {
    throw new Error("fixture generation does not read ranges");
  },
  async put(path, body) {
    const fullPath = join(repoRoot, path);
    await mkdir(dirname(fullPath), { recursive: true });
    const bytes =
      body instanceof Uint8Array ? body : new Uint8Array(await new Response(body).arrayBuffer());
    await writeFile(fullPath, bytes);
  },
  async delete() {},
  async *list() {},
  async head() {
    return null;
  },
};

const result = await writeParquet(store, outputPath, {
  rowGroupSize: [48],
  schema: [
    { name: "root", num_children: 7 },
    { name: "event_id", type: "INT32", repetition_type: "OPTIONAL" },
    { name: "region", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
    { name: "account", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
    {
      name: "event_ts",
      type: "INT64",
      converted_type: "TIMESTAMP_MILLIS",
      logical_type: { type: "TIMESTAMP", isAdjustedToUTC: true, unit: "MILLIS" },
      repetition_type: "OPTIONAL",
    },
    { name: "category", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
    { name: "amount", type: "DOUBLE", repetition_type: "OPTIONAL" },
    { name: "score", type: "INT32", repetition_type: "OPTIONAL" },
  ],
  columnData: [
    { name: "event_id", data: rows.map((row) => row.event_id) },
    { name: "region", data: rows.map((row) => row.region) },
    { name: "account", data: rows.map((row) => row.account) },
    { name: "event_ts", data: rows.map((row) => row.event_ts) },
    { name: "category", data: rows.map((row) => row.category) },
    { name: "amount", data: rows.map((row) => row.amount) },
    { name: "score", data: rows.map((row) => row.score) },
  ],
});

console.log(`wrote ${result.path} (${result.byteSize} bytes, ${rows.length} rows)`);

function buildRows() {
  const regions = ["north", "south", "west"];
  const categories = ["purchase", "refund", "fee", "purchase"];
  const rows = [];
  let eventId = 1;
  for (const region of regions) {
    for (let accountIndex = 0; accountIndex < 4; accountIndex += 1) {
      const account = `${region}-${accountIndex + 1}`;
      for (let index = 0; index < 12; index += 1) {
        const day = 1 + Math.floor(index / 2);
        const hour = (index % 2) * 6 + accountIndex;
        const peerScore = 100 - day * 3 + (accountIndex % 2);
        rows.push({
          event_id: eventId,
          region,
          account,
          event_ts: new Date(
            `2026-02-${String(day).padStart(2, "0")}T${String(hour).padStart(2, "0")}:00:00.000Z`,
          ),
          category: categories[(index + accountIndex) % categories.length],
          amount: index % 5 === 4 ? null : 10 + accountIndex * 4 + index * 1.5,
          score: index % 3 === 0 ? peerScore : peerScore - (index % 3),
        });
        eventId += 1;
      }
    }
  }
  return rows;
}
