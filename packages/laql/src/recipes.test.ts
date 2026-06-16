import { execFile } from "node:child_process";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { fixturePath, GEO, H3, ICEBERG, SALES } from "@laql/fixtures";
import { describe, expect, it } from "vitest";
import { queryHttpParquet } from "../../../examples/http-parquet.js";
import { planR2Iceberg } from "../../../examples/r2-iceberg.js";
import { queryR2Parquet } from "../../../examples/r2-parquet.js";
import {
  col,
  createLake,
  eq,
  fn,
  lit,
  loadIcebergTable,
  loadTable,
  memoryStore,
  planFiles,
  scanRows,
  writePartitionedParquet,
} from "./index.js";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
const recipesDir = new URL("../../../docs/recipes/", import.meta.url);
const coveredRecipes = [
  "append-events.md",
  "bbox-search.md",
  "compact-small-files.md",
  "csv-export.md",
  "h3-place-search.md",
  "http-parquet-api.md",
  "ndjson-export.md",
  "r2-iceberg-api.md",
  "r2-parquet-api.md",
  "unified-engine.md",
] as const;

describe("docs recipes", () => {
  it("keeps every recipe covered by the fixture harness", async () => {
    await expect(readdir(recipesDir)).resolves.toEqual([...coveredRecipes].sort());
  });

  it("runs the bbox and H3 API recipes against committed fixtures", async () => {
    const store = memoryStore();
    await store.put(`data/${GEO.file}`, await readFile(fixturePath(GEO.file)));
    await store.put(`data/${H3.file}`, await readFile(fixturePath(H3.file)));
    const lake = createLake({ store });

    const queryBbox = fn("st_bbox", lit(-118.5), lit(34), lit(-118), lit(34.3));
    await expect(
      lake
        .path(`data/${GEO.file}`)
        .select(["id", "name"])
        .where(fn("st_intersects", col("geom"), queryBbox))
        .toArray(),
    ).resolves.toEqual([
      { id: 1, name: "downtown" },
      { id: 2, name: "valley" },
    ]);

    await expect(
      lake
        .path(`data/${H3.file}`)
        .select(["id"])
        .where(fn("h3_within", col("h3_8"), lit("8829a1d757fffff"), lit(1)))
        .toArray(),
    ).resolves.toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("runs export and R2 Parquet fixture-equivalent CLI recipes", async () => {
    const ndjson = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount where region = 'west' order by amount asc limit 2",
    ]);
    expect(ndjson.stdout).toBe(
      '{"store_id":"store-000","amount":0}\n{"store_id":"store-000","amount":36.28}\n',
    );

    const csv = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount where region = 'west' order by amount asc limit 2",
      "--format",
      "csv",
    ]);
    expect(csv.stdout).toBe("store_id,amount\nstore-000,0\nstore-000,36.28\n");

    const json = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount limit 2",
      "--format",
      "json",
    ]);
    expect(JSON.parse(json.stdout)).toEqual([
      { store_id: "store-000", amount: 0 },
      { store_id: "store-001", amount: 37.01 },
    ]);
  });

  it("runs the HTTP Parquet example recipe against committed fixtures", async () => {
    const bytes = await readFile(fixturePath(SALES.file));
    const rows = await queryHttpParquet({
      baseUrl: "https://data.example/lake/",
      objects: [{ path: SALES.file, size: bytes.byteLength }],
      fetch: async (input, init) => {
        const url = new URL(String(input));
        if (!url.pathname.endsWith(`/${SALES.file}`)) return new Response(null, { status: 404 });
        if (init?.method === "HEAD") {
          return new Response(null, {
            headers: {
              "content-length": String(bytes.byteLength),
              "content-type": "application/vnd.apache.parquet",
            },
          });
        }
        const range = new Headers(init?.headers).get("range");
        if (range === null) return new Response(bytes);
        const match = /^bytes=(\d+)-(\d+)$/u.exec(range);
        if (!match) return new Response(null, { status: 416 });
        const start = Number(match[1]);
        const end = Number(match[2]);
        return new Response(bytes.slice(start, end + 1), { status: 206 });
      },
    });

    expect(rows).toHaveLength(100);
    expect(rows[0]).toMatchObject({ store_id: "store-000", region: "west" });
  });

  it("runs the compact recipe against the sales fixture", async () => {
    const dir = await mkdtemp(join(tmpdir(), "laql-recipe-compact-"));
    try {
      const output = join(dir, "sales");
      const result = await runCli([
        "compact",
        "--path",
        fixturePath(SALES.file),
        "--output",
        output,
        "--max-rows-per-file",
        "75",
      ]);

      const body = JSON.parse(result.stdout) as { files: { path: string; rowCount: number }[] };
      expect(body.files.map((file) => file.rowCount)).toEqual([75, 25]);
    } finally {
      await rm(dir, { force: true, recursive: true });
    }
  });

  it("runs the append-events and R2 Iceberg fixture-equivalent recipes", async () => {
    const writeStore = memoryStore();
    const written = await writePartitionedParquet(writeStore, "events", {
      rows: [
        { id: 1, date: "2026-01-01", event: "view" },
        { id: 2, date: "2026-01-02", event: "click" },
      ],
      partitionBy: ["date"],
      jobId: "events_2026_01_01",
    });
    expect(written.files.map((file) => file.rowCount)).toEqual([1, 1]);

    const icebergStore = memoryStore();
    await icebergStore.put(ICEBERG.metadataFile, await readFile(fixturePath(ICEBERG.metadataFile)));
    await icebergStore.put(
      ICEBERG.manifestListFile,
      await readFile(fixturePath(ICEBERG.manifestListFile)),
    );
    for (const manifestFile of ICEBERG.manifestFiles) {
      await icebergStore.put(manifestFile, await readFile(fixturePath(manifestFile)));
    }

    const table = await loadIcebergTable({
      store: icebergStore,
      metadataPath: ICEBERG.metadataFile,
    });
    expect(table.planFiles({ ref: "main", readMode: "ignore-deletes" }).files.length).toBe(3);
  });

  it("runs the R2 Parquet and Iceberg example recipes against committed fixtures", async () => {
    const bucket = new RecipeR2Bucket();
    await bucket.put(SALES.file, await readFile(fixturePath(SALES.file)));
    await bucket.put(
      "warehouse/places/metadata/v2.metadata.json",
      await readFile(fixturePath(ICEBERG.metadataFile)),
    );
    await bucket.put(ICEBERG.metadataFile, await readFile(fixturePath(ICEBERG.metadataFile)));
    await bucket.put(
      ICEBERG.manifestListFile,
      await readFile(fixturePath(ICEBERG.manifestListFile)),
    );
    for (const manifestFile of ICEBERG.manifestFiles) {
      await bucket.put(manifestFile, await readFile(fixturePath(manifestFile)));
    }

    const parquetRows = await queryR2Parquet(bucket);
    const plan = await planR2Iceberg(bucket);

    expect(parquetRows).toHaveLength(100);
    expect(parquetRows[0]).toMatchObject({ store_id: "store-000", region: "west" });
    expect(plan).toMatchObject({ snapshotId: 2, filesPlanned: 2 });
    expect(plan.files.map((file) => file.partition.country)).toEqual(["US", "US"]);
  });

  it("runs the unified engine API recipe against committed fixtures", async () => {
    const store = memoryStore();
    await store.put(SALES.file, await readFile(fixturePath(SALES.file)));
    await store.put(ICEBERG.metadataFile, await readFile(fixturePath(ICEBERG.metadataFile)));
    await store.put(
      ICEBERG.manifestListFile,
      await readFile(fixturePath(ICEBERG.manifestListFile)),
    );
    for (const manifestFile of ICEBERG.manifestFiles) {
      await store.put(manifestFile, await readFile(fixturePath(manifestFile)));
    }
    for (const dataFile of ICEBERG.dataFiles) {
      await store.put(dataFile, await readFile(fixturePath(dataFile)));
    }
    await store.put(
      ICEBERG.equalityDeleteFile,
      await readFile(fixturePath(ICEBERG.equalityDeleteFile)),
    );
    await store.put(
      ICEBERG.positionDeleteFile,
      await readFile(fixturePath(ICEBERG.positionDeleteFile)),
    );

    const parquetRows = [];
    const parquetTable = await loadTable({
      format: "parquet",
      store,
      path: SALES.file,
    });
    for await (const row of scanRows(planFiles(parquetTable))) {
      parquetRows.push(row);
    }

    const icebergRows = [];
    const icebergTable = await loadTable({
      format: "iceberg",
      store,
      metadataPath: ICEBERG.metadataFile,
    });
    const icebergPlan = planFiles(icebergTable, {
      where: eq("country", "US"),
      select: ["id", "nation"],
    });
    for await (const row of scanRows(icebergPlan, { batchSize: 256, maxConcurrentReads: 4 })) {
      icebergRows.push(row);
    }

    expect(parquetRows).toHaveLength(SALES.rows);
    expect(icebergRows).toEqual([
      { id: 0, nation: "US" },
      { id: 2, nation: "US" },
      { id: 3, nation: "US" },
      { id: 200, nation: "US" },
      { id: 201, nation: "US" },
      { id: 202, nation: "US" },
      { id: 203, nation: "US" },
    ]);
  });
});

class RecipeR2Object {
  readonly size: number;
  readonly uploaded = new Date("2026-06-15T00:00:00Z");
  readonly httpMetadata = { contentType: "application/octet-stream" };

  constructor(
    readonly key: string,
    private readonly bytes: Uint8Array,
    readonly etag = "etag",
  ) {
    this.size = bytes.byteLength;
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    const out = new ArrayBuffer(this.bytes.byteLength);
    new Uint8Array(out).set(this.bytes);
    return out;
  }
}

class RecipeR2Bucket {
  private readonly objects = new Map<string, Uint8Array>();

  async get(key: string, options?: { range?: { offset: number; length: number } }) {
    const bytes = this.objects.get(key);
    if (bytes === undefined) return null;
    const ranged =
      options?.range === undefined
        ? bytes
        : bytes.slice(options.range.offset, options.range.offset + options.range.length);
    return new RecipeR2Object(key, ranged);
  }

  async head(key: string) {
    const bytes = this.objects.get(key);
    if (bytes === undefined) return null;
    return new RecipeR2Object(key, bytes);
  }

  async put(key: string, value: Uint8Array | ReadableStream<Uint8Array>) {
    if (value instanceof Uint8Array) {
      this.objects.set(key, value);
      return;
    }
    const chunks: Uint8Array[] = [];
    for await (const chunk of value) chunks.push(chunk);
    const length = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    const out = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    this.objects.set(key, out);
  }

  async delete(key: string) {
    this.objects.delete(key);
  }

  async list(options?: { prefix?: string; limit?: number; cursor?: string }) {
    const prefix = options?.prefix ?? "";
    const start = options?.cursor === undefined ? 0 : Number(options.cursor);
    const limit = options?.limit ?? Number.POSITIVE_INFINITY;
    const matching = [...this.objects.entries()]
      .filter(([key]) => key.startsWith(prefix))
      .sort(([left], [right]) => left.localeCompare(right));
    const page = matching.slice(start, start + limit);
    const next = start + page.length;
    return {
      objects: page.map(([key, bytes]) => new RecipeR2Object(key, bytes)),
      truncated: next < matching.length,
      cursor: next < matching.length ? String(next) : undefined,
    };
  }
}

async function runCli(args: string[]): Promise<{ stdout: string; stderr: string }> {
  const result = await execFileAsync(process.execPath, ["packages/cli/dist/bin.js", ...args], {
    cwd: repoRoot,
    encoding: "utf8",
  });
  return { stdout: result.stdout, stderr: result.stderr };
}
