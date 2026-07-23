# Examples

Each example starts from a fresh project with the public package installed:

```sh
npm init -y
npm install lakeql
```

Use the example paths as your own object keys or file paths. The returned values
below show the shape of the output, not a required fixture.

Examples using optional Lance or WebGPU packages include their additional
installation command in that section.

## Local Parquet With SQL

```ts
import { readFile } from "node:fs/promises";
import { createLake, memoryStore } from "lakeql/node";

const path = "sales.parquet";
const store = memoryStore();
await store.put(path, await readFile(path));

const lake = createLake({ store });
const rows = await lake
  .sql("select store_id, amount from input where amount > $1 order by amount desc limit 2", {
    path,
    parameters: [100],
  })
  .toArray();

console.log(rows);
```

Output shape:

```json
[
  { "store_id": "store-001", "amount": 923.79 },
  { "store_id": "store-002", "amount": 901.42 }
]
```

## Local Parquet With JavaScript

```ts
import { readFile } from "node:fs/promises";
import { createLake, gt, memoryStore } from "lakeql/node";

const path = "sales.parquet";
const store = memoryStore();
await store.put(path, await readFile(path));

const lake = createLake({ store });
const rows = await lake
  .path(path)
  .select(["store_id", "region", "amount"])
  .where(gt("amount", 100))
  .orderBy([{ column: "amount", direction: "desc" }])
  .limit(2)
  .toArray();

console.log(rows);
```

Output shape:

```json
[
  { "store_id": "store-001", "region": "west", "amount": 923.79 },
  { "store_id": "store-002", "region": "east", "amount": 901.42 }
]
```

## Remote HTTP Parquet From Node.js

```ts
import { createLake, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/lake" }),
  budget: { maxOutputRows: 1000, maxConcurrentReads: 4 },
});

const rows = await lake
  .path("sales.parquet")
  .select(["store_id", "region", "amount"])
  .limit(100)
  .toArray();

console.log(rows);
```

Output shape:

```json
[
  { "store_id": "store-001", "region": "west", "amount": 42.5 }
]
```

## Cloudflare Worker Reading R2

```ts
import { createLake, r2Store } from "lakeql/cloudflare";

export default {
  async fetch(_request: Request, env: { DATA: R2Bucket }) {
    const lake = createLake({
      store: r2Store(env.DATA),
      budget: { maxOutputRows: 1000, maxConcurrentReads: 4 },
    });

    const rows = await lake.path("sales.parquet").limit(100).toArray();
    return Response.json(rows);
  },
};
```

Output shape:

```json
[
  { "store_id": "store-001", "region": "west", "amount": 42.5 }
]
```

## Prefix Or Glob Of Parquet Files

```ts
import { createLake, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({
    baseUrl: "https://example.com/lake",
    objects: [
      { path: "sales/region=west/part-00000.parquet", size: 1234 },
      { path: "sales/region=east/part-00000.parquet", size: 1234 },
    ],
  }),
});

const rows = await lake
  .sql("select region, sum(amount) as revenue from read_parquet('sales/**/*.parquet') group by region")
  .toArray();

console.log(rows);
```

Output shape:

```json
[
  { "region": "west", "revenue": 2400.5 },
  { "region": "east", "revenue": 1805.25 }
]
```

## Iceberg Table

```ts
import { eq, loadIcebergTable, r2Store } from "lakeql/cloudflare";

export async function planUsFiles(env: { DATA: R2Bucket }) {
  const table = await loadIcebergTable({
    store: r2Store(env.DATA),
    metadataPath: "warehouse/places/metadata/v2.metadata.json",
  });

  return table.planFiles({
    ref: "main",
    where: eq("country", "US"),
    readMode: "ignore-deletes",
  });
}
```

Output shape:

```json
{
  "snapshotId": 2,
  "filesPlanned": 2,
  "files": [{ "path": "warehouse/places/data/part-00000.parquet" }]
}
```

## Lance Rows By Stable ID

Install the separate Lance reader:

```sh
npm install lakeql lakeql-lance
```

```ts
import { httpStore } from "lakeql/node";
import { openLanceDataset } from "lakeql-lance";

const dataset = await openLanceDataset({
  store: httpStore({ baseUrl: "https://example.com/data/" }),
  path: "catalog.lance",
  budget: {
    maxBytes: 8 * 1024 * 1024,
    maxRangeRequests: 128,
    maxMemoryBytes: 16 * 1024 * 1024,
    maxOutputRows: 32,
    maxConcurrentReads: 4,
    maxElapsedMs: 3_000,
  },
});

const result = await dataset.takeRows({
  snapshotId: dataset.snapshotId,
  rowIds: [42n, 7n, 99n],
  select: ["title", "owner_name", "source_url"],
});

console.log(result.rows, result.stats);
```

Persist `dataset.snapshotId` beside row IDs in an external index. LakeQL rejects
IDs coupled to another snapshot instead of returning unrelated records. See
[Querying Lance](./querying-lance.md) for BTree and IVF_FLAT examples.

## Browser Query With WebGPU Placement

Install the optional backend:

```sh
npm install lakeql lakeql-webgpu
```

```ts
import { createLake, gt, httpStore } from "lakeql/fetch";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";
import { browserWebGpuRuntime } from "lakeql-webgpu/browser";

if (navigator.gpu === undefined) {
  throw new Error("This browser does not expose WebGPU");
}

const runtime = browserWebGpuRuntime(navigator, { GPUBufferUsage, GPUMapMode });
const webgpu = new WebGpuPhysicalBackend(() => runtime);
const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data/" }),
  physicalExecution: {
    backends: [webgpu],
    acceleratorPolicy: "auto",
    replayOnCpu: true,
  },
  budget: {
    maxAcceleratorMemoryBytes: 64 * 1024 * 1024,
    maxAcceleratorUploadBytes: 64 * 1024 * 1024,
    maxAcceleratorReadbackBytes: 4 * 1024 * 1024,
    maxAcceleratorDispatches: 32,
  },
});

try {
  const result = lake
    .path("scores.parquet")
    .select(["item_id", "score"])
    .where(gt("score", 0.5))
    .limit(100)
    .run();

  console.log(await result.toArray());
  console.log((await result.explain()).text);
} finally {
  webgpu.close();
}
```

`auto` includes upload, dispatch, and readback costs and may choose CPU. Use
`required` only when failure is preferable to CPU placement. See
[WebGPU execution](./webgpu.md) for the supported operator shapes and benchmark
interpretation.

## Write Results To Parquet

```ts
import { memoryStore, writePartitionedParquet } from "lakeql/node";

const store = memoryStore();
const result = await writePartitionedParquet(store, "out/sales", {
  rows: [
    { store_id: "store-001", region: "west", amount: 42 },
    { store_id: "store-002", region: "east", amount: 55 },
  ],
  partitionBy: ["region"],
});

console.log(result.files);
```

Output shape:

```json
[
  { "path": "out/sales/region=east/part-data-00000.parquet", "rowCount": 1 },
  { "path": "out/sales/region=west/part-data-00001.parquet", "rowCount": 1 }
]
```

The repository also includes runnable versions under [`../examples`](../examples).
