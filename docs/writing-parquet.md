# Writing Parquet

Use `writePartitionedParquet` for deterministic partitioned Parquet output.

```ts
import { memoryStore } from "@laql/core";
import { writePartitionedParquet } from "@laql/parquet";

const store = memoryStore();
const result = await writePartitionedParquet(store, "out/sales", {
  rows,
  partitionBy: ["region"],
  maxRowsPerFile: 1000,
  jobId: "job_2026_01_01",
  taskId: "task_000",
  idempotencyKey: "attempt_001",
  writeMode: "create",
});
```

Output paths include partition values and deterministic file ordinals. `taskId` and
`idempotencyKey` are optional retry-scoped filename components; omit them to keep the
default `part-${jobId}-${ordinal}.parquet` shape. The returned file list can be converted
to output-manifest entries with `partitionedParquetOutputEntries`:

```ts
const entries = partitionedParquetOutputEntries(result, {
  taskId: "task_000",
  iceberg: true,
});
```

Each written file and output-manifest entry includes a deterministic `sha256:` content
hash computed from the exact Parquet bytes written to the object store.
When write tasks persist their `OutputManifestEntry` values in a checkpoint adapter,
`createOutputManifestFromCheckpoints` fans them into one sorted manifest for commit.

Use `writePartitionedParquetTask` when the write should advance the task checkpoint
state machine and record all output entries. Replaying a completed task with the same
idempotency key returns the checkpointed outputs without rewriting files:

```ts
import { memoryCheckpointAdapter } from "@laql/core";
import { writePartitionedParquetTask } from "@laql/parquet";

const checkpoints = memoryCheckpointAdapter();
const { entries } = await writePartitionedParquetTask(store, "out/sales", {
  checkpoints,
  rows,
  partitionBy: ["region"],
  jobId: "job_2026_01_01",
  taskId: "task_000",
  idempotencyKey: "attempt_001",
  writeMode: "create",
});
```

For CTAS-style flows, `createParquetTableAs` consumes a query result, writes the
rows through the checkpointed task path, and returns an output manifest:

```ts
import { createParquetTableAs } from "@laql/parquet";

const created = await createParquetTableAs(store, "out/west_sales", {
  query: lake.path("sales.parquet").where(eq("region", "west")),
  checkpoints,
  jobId: "job_2026_01_01",
  planFingerprint: "fp_job_2026_01_01",
  idempotencyKey: "attempt_001",
  partitionBy: ["region"],
  writeMode: "create",
});

await table.appendOutputManifest({ manifest: created.manifest });
```

Set `iceberg: true` when the manifest should carry data-file metadata for a later Iceberg append commit.

`writeMode: "create"` fails if an output object already exists; omit it or use `"overwrite"` when replacing existing output is intentional.
