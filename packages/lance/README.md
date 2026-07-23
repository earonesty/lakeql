# lakeql-lance

`lakeql-lance` performs bounded, projection-aware random reads from immutable
[Lance](https://lancedb.github.io/lance/) datasets in Cloudflare Workers, browsers, and Node.js.
It is intended for applications that already have stable Lance row IDs from a search index and
need to materialize the corresponding records without scanning a key column.

```ts
import { openLanceDataset } from "lakeql-lance";

const dataset = await openLanceDataset({
  store,
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
  rowIds: [42n, 7n, 42n],
  select: ["serial", "mark_text", "owner_name"],
});
```

Store `dataset.snapshotId` with every external index generation. `takeRows` requires it and returns
a typed `LAKEQL_LANCE_SNAPSHOT_MISMATCH` error before reading data if it does not match. Duplicate
IDs and caller order are preserved. Missing IDs throw by default; use `onMissing: "null"` to retain
their position as `null`.

## Compatibility

The current reader supports stable-row-ID datasets written with Lance storage version 2.0 and V2
manifest paths. The checked-in compatibility fixture was produced by the official `pylance 8.0.0`
writer. Supported projected leaf types are UTF-8 strings, binary values, booleans, signed and
unsigned integers, and 32/64-bit floats using uncompressed flat and nullable encodings.

The reader deliberately rejects unsupported storage versions, compression, nested fields,
deletion files, external row-ID sequences, and unknown encodings. It never silently scans a
column, file, or dataset, and it rejects a data range that would read an entire Lance data file.

All metadata and data access flows through LakeQL's `ObjectStore`, cancellation, concurrency,
cache, and query-budget contracts. The result reports snapshot/data metadata bytes, logical and
physical bytes, range requests, fragments/pages touched, cache activity, row counts, peak memory,
and elapsed time.

## Reproducing the fixture

Create an isolated Python environment with `pylance==8.0.0` and run:

```sh
python packages/lance/scripts/generate_fixture.py
```

The generator records producer and storage versions, expected projections, stable row IDs, and
SHA-256 hashes in `fixtures/take-v2.0.lance/expected.json`.
