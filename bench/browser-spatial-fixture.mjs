import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { writeParquet } from "../packages/parquet/dist/index.js";

const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const outputPath = "site/public/spatial.parquet";

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
  rowGroupSize: [64],
  columnData: [
    { name: "id", type: "INT32", data: rows.map((row) => row.id) },
    { name: "name", type: "STRING", data: rows.map((row) => row.name) },
    { name: "lon", type: "DOUBLE", data: rows.map((row) => row.lon) },
    { name: "lat", type: "DOUBLE", data: rows.map((row) => row.lat) },
    { name: "geometry", type: "GEOMETRY", data: rows.map((row) => row.geometry) },
    { name: "geom", type: "STRING", data: rows.map((row) => row.geom) },
    { name: "wkt", type: "STRING", data: rows.map((row) => row.wkt) },
  ],
});

console.log(`wrote ${result.path} (${result.byteSize} bytes, ${rows.length} rows)`);

function buildRows() {
  const center = { lon: -118.2437, lat: 34.0522 };
  const rows = [];
  for (let index = 0; index < 256; index += 1) {
    const ring = index % 16;
    const spoke = Math.floor(index / 16);
    const close = index < 48;
    const radius = close ? 0.00012 * (1 + (ring % 4)) : 0.02 + ring * 0.004;
    const angle = (spoke / 16) * Math.PI * 2;
    const lon = center.lon + Math.cos(angle) * radius;
    const lat = center.lat + Math.sin(angle) * radius;
    rows.push({
      id: index + 1,
      name: close ? `near-${index + 1}` : `far-${index + 1}`,
      lon,
      lat,
      geometry: { type: "Point", coordinates: [lon, lat] },
      geom: JSON.stringify({ type: "Point", coordinates: [lon, lat] }),
      wkt: `POINT(${lon} ${lat})`,
    });
  }
  return rows;
}
