# Product spec: `laql`

A lightweight TypeScript query engine for Iceberg + Parquet on object storage.

Runs in Cloudflare Workers, browsers, Node, Deno, Bun, and service workers.

Built on:

```txt
iceberg-js        (catalog + commit protocol)
icebird           (Iceberg metadata + manifest reads)
hyparquet         (Parquet reads)
hyparquet-writer  (Parquet writes)
avsc              (Avro for Iceberg manifests)
h3-js             (optional, via @laql/geo)
@turf/*           (optional, via @laql/geo)
```

Dependency check (light pass, 2026-06-12 — npm + README review, not a deep audit):

```txt
hyparquet 1.26     pure JS, snappy built in, zstd/gzip/lz4 via hyparquet-compressors;
                   HTTP range reads, column projection, row-group selection confirmed

hyparquet-writer   snappy by default, custom compressors pluggable (zstd = bring your own);
                   column statistics on by default; nested records; row-group sizing

icebird 0.8        reads Iceberg v1/v2/v3; position deletes and v3 deletion vectors
                   confirmed; experimental v2/v3 writes (create/append/delete);
                   equality-delete read coverage needs a closer look before relying on it

iceberg-js 1.0     Supabase REST catalog client, catalog + commit endpoints only —
                   no data ops; laql owns building commit requirements/updates

avsc 5.7           mature Avro; manifest read/write viable
```

Primary use case:

```txt
Query Iceberg/Parquet lakes from object storage without running DuckDB-WASM, Spark, Trino, ClickHouse, or a server.
```

Core promise:

```txt
DuckDB-like usefulness for object-store analytics.
Much smaller surface area.
Native Worker ergonomics.
Streaming results.
Resumable queries via serializable bookmarks.
Predicate pushdown.
Partition-aware reads and writes.
H3 and geospatial functions included.
No native bindings.
No virtual filesystem setup.
No WASM boot tax unless the user explicitly opts into one.
```

---

# Positioning

`laql` is not a database.

It is a query engine for lake files.

It gives users:

```txt
scan
filter
project
aggregate
group
sort
limit
write
compact
partition
inspect
```

It intentionally avoids pretending to be full SQL infrastructure.

It should feel easier than DuckDB-WASM for people who currently want:

```txt
read Parquet from R2/S3/HTTP
query Iceberg tables
filter by partitions
compute H3 cells
run simple geospatial filters
stream JSON/NDJSON results
write partitioned Parquet files
append to Iceberg tables
build APIs over lake data inside Workers
```

---

# Runtime model

`@laql/core` is runtime-agnostic.

It targets any short-lived, memory-constrained invocation: Cloudflare Workers, AWS Lambda, edge runtimes, browsers, Node, Deno, Bun, service workers.

Core never imports a runtime API. Everything environmental is a caller-supplied interface:

```txt
object storage       ObjectStore
caching              cache adapters
spill / checkpoints  spill adapter
commit coordination  committer
queueing             none — the engine never talks to a queue;
                     the caller moves bookmarks however it likes
                     (Cloudflare Queues, SQS, DO alarms, cron, a while loop)
state                bookmarks are plain serializable values;
                     where they live is the caller's choice
```

Runtime drivers wire these up:

```txt
laql/cloudflare    R2, Cache API, DO committer, Queues recipes
laql/lambda        S3, SQS recipes              (same core)
laql/node          filesystem / S3              (same core)
```

Cloudflare is the flagship driver, not a dependency.

---

# Package layout

```txt
@laql/core
  AST
  query planner
  expression evaluator
  execution engine
  streaming result model
  type system
  errors

@laql/parquet
  hyparquet reader adapter
  hyparquet-writer adapter
  row-group pruning
  column projection
  page/batch streaming

@laql/iceberg
  iceberg-js / icebird adapter
  table loading
  schema loading
  snapshot loading
  manifest planning
  partition pruning
  Iceberg append commits

@laql/r2
  Cloudflare R2 object store adapter
  range reads
  writes
  multipart-like file assembly where applicable
  object listing
  metadata reads

@laql/s3
  S3-compatible object store adapter
  optional portability package

@laql/http
  HTTP range-read adapter

@laql/geo
  h3_* functions
  st_* functions
  bbox helpers
  GeoJSON helpers

@laql/sql
  small SQL parser
  SQL-to-AST compiler
  explain output

@laql/react
  optional query hooks
  browser demos
  progressive result streaming helpers

@laql/cli
  inspect
  query
  write
  compact
  metadata
  schema
  explain
```

Default import for Worker users:

```ts
import { LaQL, eq, and, gt, h3Within, bbox } from "@laql/core";
import { r2Store } from "@laql/r2";
import { icebergCatalog } from "@laql/iceberg";
```

Convenience import:

```ts
import { createLake } from "laql/cloudflare";
```

---

# Basic Worker usage

```ts
import { createLake, eq, and, h3Within, bbox, stIntersects } from "laql/cloudflare";

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const lake = createLake({
      store: env.DATA_BUCKET,
      catalog: {
        type: "iceberg-rest",
        url: env.ICEBERG_REST_URL,
        token: env.ICEBERG_REST_TOKEN,
      },
    });

    const rows = lake
      .table("places")
      .select([
        "id",
        "name",
        "lat",
        "lon",
        "h3_8",
      ])
      .where(and(
        eq("country", "US"),
        eq("state", "CA"),
        h3Within("h3_8", "8829a1d757fffff", 2),
        stIntersects("geom", bbox(-118.35, 33.95, -118.15, 34.15)),
      ))
      .limit(100)
      .streamJson();

    return new Response(rows, {
      headers: {
        "content-type": "application/json",
      },
    });
  },
};
```

NDJSON streaming:

```ts
return new Response(
  lake.table("permits")
    .where(gt("amount", 100_000))
    .limit(10_000)
    .streamNdjson(),
  {
    headers: {
      "content-type": "application/x-ndjson",
    },
  },
);
```

---

# Public query styles

`laql` supports three equivalent query styles.

## 1. Fluent TypeScript API

```ts
const result = await lake.table("sales")
  .select([
    "store_id",
    "date",
    "amount",
  ])
  .where(and(
    eq("region", "west"),
    between("date", "2026-01-01", "2026-06-01"),
    gt("amount", 100),
  ))
  .orderBy("amount", "desc")
  .limit(500)
  .toArray();
```

## 2. JSON query API

```json
{
  "version": 1,
  "from": "sales",
  "select": ["store_id", "date", "amount"],
  "where": {
    "and": [
      { "eq": ["region", "west"] },
      { "between": ["date", "2026-01-01", "2026-06-01"] },
      { "gt": ["amount", 100] }
    ]
  },
  "order_by": [{ "column": "amount", "direction": "desc" }],
  "limit": 500
}
```

The format carries an explicit version field so the stable API can evolve.

Worker route:

```ts
const query = await req.json();

const result = lake.query(query).streamNdjson();

return new Response(result, {
  headers: {
    "content-type": "application/x-ndjson",
  },
});
```

## 3. Small SQL dialect

```sql
from sales
select store_id, date, amount
where region = 'west'
  and date between '2026-01-01' and '2026-06-01'
  and amount > 100
order by amount desc
limit 500
```

Function syntax:

```sql
from places
select id, name, lat, lon
where country = 'US'
  and state = 'CA'
  and h3_within(h3_8, '8829a1d757fffff', 2)
  and st_intersects(geom, st_bbox(-118.35, 33.95, -118.15, 34.15))
limit 100
```

The SQL dialect compiles to the same AST as the fluent and JSON APIs.

---

# Language scope

## Query clauses

```txt
from
select
where
group by
aggregate
having
order by
limit
offset
sample
write to
partition by
```

Clause notes:

```txt
aggregate    fluent/JSON name for aggregate expressions; in SQL, aggregates
             appear in select alongside group by
having       filters aggregate results, e.g. having n > 100
sample       row-fraction sampling, e.g. sample 0.01
```

## Supported operators

```txt
=
!=
<
<=
>
>=
in
not in
between
is null
is not null
and
or
not
like
ilike
```

## Supported scalar functions

```txt
lower(value)
upper(value)
trim(value)
substr(value, start, length)
replace(value, search, replacement)

coalesce(a, b, ...)
nullif(a, b)

cast(value, type)

year(timestamp)
month(timestamp)
day(timestamp)
hour(timestamp)
date_trunc(part, timestamp)

round(number, places)
floor(number)
ceil(number)
abs(number)
least(a, b, ...)
greatest(a, b, ...)
```

## Aggregates

```txt
count()
count(column)
count_distinct(column)
sum(column)
avg(column)
min(column)
max(column)
first(column)
last(column)
any(column)
```

`first` and `last` follow scan order, which the planner makes deterministic per snapshot (see Resumable execution). `any` is explicitly nondeterministic.

## Geospatial functions

Geometry format is GeoJSON by default.

WKB and WKT are optional codecs.

```txt
st_point(lon, lat)
st_bbox(minx, miny, maxx, maxy)
st_x(point)
st_y(point)

st_intersects(a, b)
st_contains(a, b)
st_within(a, b)
st_disjoint(a, b)

st_distance(a, b)
st_area(a)
st_length(a)

st_centroid(geom)
st_envelope(geom)

st_as_geojson(geom)
st_from_geojson(value)
st_from_wkt(value)
st_as_wkt(value)
```

## H3 functions

```txt
h3_cell(lat, lon, res)
h3_parent(cell, res)
h3_children(cell, res)
h3_resolution(cell)

h3_grid_disk(cell, k)
h3_grid_distance(a, b)

h3_boundary(cell)
h3_center(cell)

h3_within(column, origin, k)
h3_intersects_cell(geom, cell)
h3_polygon_to_cells(geom, res)
```

Special pushdown-aware predicates:

```txt
h3_within(h3_8, '8829a1d757fffff', 2)
h3_parent_is(h3_8, '8729a1d75ffffff')
h3_in(h3_8, [...])
```

These compile into partition pruning when the H3 column is part of the table partition spec.

---

# Type system

Internal types:

```txt
boolean
int32
int64
float32
float64
decimal
string
binary
date
time
timestamp
json
struct
list
map
geometry
h3
```

JS value mapping:

```txt
int64      bigint in rows; JSON/NDJSON emits a number when within
           Number.MAX_SAFE_INTEGER, otherwise a string
decimal    string in rows and in output
timestamp  Date in rows; ISO-8601 string in output
binary     Uint8Array in rows; base64 string in output
```

Type inference comes from:

```txt
Iceberg schema
Parquet schema
explicit JSON query schema
manual table registration
```

Expression analysis validates:

```txt
unknown columns
invalid function names
invalid argument counts
invalid type combinations
unsafe casts
unsupported pushdowns
ambiguous aliases
```

Example error:

```txt
LAQL_TYPE_ERROR

st_intersects(left, right) expects geometry-compatible values.

left:
  column: "price"
  type: int64

right:
  st_bbox(...)
  type: geometry

query:
  where st_intersects(price, st_bbox(-118, 33, -117, 34))
```

---

# Execution model

The engine is streaming-first.

Every query can produce:

```ts
AsyncIterable<Row>
ReadableStream<Uint8Array>
Promise<Row[]>
Promise<ArrowLikeBatch[]>
Promise<QueryStats>
Promise<SliceResult>
```

APIs:

```ts
query.rows()
query.batches()
query.toArray()
query.first()
query.count()
query.streamJson()
query.streamNdjson()
query.streamCsv()
query.writeParquet(...)
query.explain()
query.stats()
query.run({ slice })
query.resumableBatches({ bookmarkEvery })
lake.resume(bookmark)
```

Example:

```ts
for await (const row of lake.table("events").where(eq("type", "click")).rows()) {
  console.log(row);
}
```

Batch streaming:

```ts
for await (const batch of lake.table("events").select(["user_id", "ts"]).batches({ size: 4096 })) {
  await processBatch(batch);
}
```

---

# Resumable execution

Every query position is addressable.

Lake files are immutable and reads are pinned to a snapshot, so an in-flight query can be serialized, shipped anywhere, and resumed.

The primitive is the bookmark.

```ts
type Bookmark = {
  version: 1;
  planFingerprint: string;
  snapshot: string;
  position: {
    fileIndex: number;
    rowGroup: number;
    rowOffset: number;
  };
  operatorState?: {
    limitEmitted?: number;
    groupBy?: SerializedHashTable | { spillRef: string };
    topK?: SerializedHeap;
    sketches?: Record<string, Uint8Array>;
  };
};

type SliceResult = {
  rows: Row[];
  bookmark?: Bookmark;
};
```

## Determinism

The planner orders data files deterministically for a given query and snapshot.

```txt
planFingerprint = hash(normalized AST, snapshot id, schema id, engine version)
```

Resume re-plans and validates the fingerprint.

Mismatch fails loudly:

```txt
LAQL_BOOKMARK_STALE
```

## Slice API

```ts
const { rows, bookmark } = await lake.table("events")
  .where(eq("type", "click"))
  .run({
    slice: {
      maxMs: 20_000,
      maxFiles: 50,
    },
  });
```

`bookmark` is absent when the query completed.

Resume:

```ts
const next = await lake.resume(bookmark).run({
  slice: { maxMs: 20_000 },
});
```

## Periodic bookmarks while streaming

Long-running clients checkpoint from time to time:

```ts
for await (const { batch, bookmark } of query.resumableBatches({ bookmarkEvery: "30s" })) {
  await processBatch(batch);
  await saveCheckpoint(bookmark);
}
```

Crash, restart, resume from the last saved bookmark.

## Operator classes

```txt
position-only
  scan, filter, project, limit, offset
  bookmark is a file/row-group/row position plus counters

serialized state
  group by, top-k sort, approximate sketches
  operator state serializes into or alongside the bookmark

spill-backed
  global sort, hash join
  spill files are the checkpoint
  the bookmark holds references
```

The spill adapter and the checkpoint store are the same abstraction.

Large operator state is externalized so bookmarks stay small enough for a queue message, a KV value, or a URL:

```txt
_laql/tmp/query-<id>/state/
```

## Worker slicing

Each slice fits one invocation. The bookmark rides the queue.

```ts
export default {
  async queue(batch: MessageBatch<{ bookmark: Bookmark }>, env: Env) {
    for (const msg of batch.messages) {
      const { rows, bookmark } = await lake.resume(msg.body.bookmark).run({
        slice: { maxMs: 20_000 },
      });

      await appendToSink(env, rows);

      if (bookmark) {
        await env.LAKE_JOBS.send({ bookmark });
      }
    }
  },
};
```

The same loop works from a Durable Object alarm, a Lambda + SQS consumer, or any scheduler that can hold a small JSON value. The engine only produces and consumes bookmarks; the transport is the caller's.

## Cursor pagination

In HTTP server mode the bookmark doubles as a pagination token.

```json
{
  "rows": [...],
  "nextToken": "..."
}
```

Next page:

```json
{
  "resume": "<nextToken>"
}
```

The bookmark pins a snapshot, so pagination is consistent even while the table changes.

Tokens that cross a trust boundary are HMAC-signed or stored server-side by id.

Forged or corrupted positions fail loudly:

```txt
LAQL_BOOKMARK_INVALID
```

## Resumable writes

Write jobs bookmark the same way.

```txt
data files fully written so far
in-flight multipart upload state (uploadId, completed part etags)
partition writer positions
```

The Iceberg commit is the atomic finalize.

A long CTAS or compaction is a chain of slices ending in one commit.

---

# Query planner

Planner phases:

```txt
parse
normalize
analyze
bind schema
constant fold
split predicates
classify pushdowns
plan Iceberg snapshot
plan manifests
plan data files
plan Parquet row groups
plan projected columns
plan residual evaluation
plan aggregation
plan output format
```

Predicate classes:

```ts
type PredicatePlan = {
  partition?: Expr;
  manifest?: Expr;
  fileStats?: Expr;
  rowGroupStats?: Expr;
  residual?: Expr;
};
```

Example query:

```sql
from permits
select id, address, amount
where state = 'CA'
  and county = 'ventura'
  and issued_date between '2026-01-01' and '2026-06-01'
  and amount > 100000
  and st_intersects(geom, st_bbox(-119.6, 33.9, -118.5, 34.5))
limit 100
```

Planner output:

```txt
partition predicates
  state = 'CA'
  county = 'ventura'
  issued_date_day >= '2026-01-01'
  issued_date_day <= '2026-06-01'

file stats predicates
  amount > 100000

row group predicates
  amount > 100000

column projection
  id
  address
  amount
  geom

residual predicates
  issued_date between '2026-01-01' and '2026-06-01'
  st_intersects(geom, bbox)

limit
  100
```

---

# Explain output

Human format:

```ts
const plan = await lake.sql(`
  from places
  select id, name
  where state = 'CA'
    and h3_within(h3_8, '8829a1d757fffff', 2)
  limit 100
`).explain();

console.log(plan.text);
```

Output:

```txt
LaQL Plan

table
  places

snapshot
  482719204991

projection
  id
  name
  h3_8
  state

partition pruning
  state = 'CA'
  h3_8 in 19 cells

manifest pruning
  enabled

data file pruning
  enabled

row group pruning
  enabled

residual filter
  none

estimated scan
  manifests: 2 / 91
  data files: 17 / 18,240
  row groups: 42 / 91,882
  columns: 4 / 37
```

JSON format:

```ts
const plan = await query.explainJson();
```

---

# Object store abstraction

Interface:

```ts
export interface ObjectStore {
  get(path: string): Promise<Uint8Array | null>;

  getRange(path: string, range: {
    offset: number;
    length: number;
  }): Promise<Uint8Array>;

  put(path: string, body: Uint8Array | ReadableStream<Uint8Array>, options?: PutOptions): Promise<void>;

  delete(path: string): Promise<void>;

  list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo>;

  head(path: string): Promise<ObjectHead | null>;
}
```

R2 adapter:

```ts
const store = r2Store(env.BUCKET);
```

HTTP adapter:

```ts
const store = httpStore({
  baseUrl: "https://example.com/data/",
  headers: {
    authorization: `Bearer ${token}`,
  },
});
```

S3 adapter:

```ts
const store = s3Store({
  endpoint: "https://...",
  bucket: "lake",
  region: "auto",
  accessKeyId,
  secretAccessKey,
});
```

---

# Iceberg support

Supported table sources:

```txt
Iceberg REST catalog
R2 object layout catalog
S3 object layout catalog
static metadata.json URL
local metadata file in Node
```

Supported reads:

```txt
latest snapshot
snapshot by id
snapshot by timestamp
branch
tag
schema evolution
partition evolution
manifest lists
position delete files
equality delete files
```

Delete handling modes:

```txt
strict                       (default) snapshots containing delete file types
                             the engine cannot apply fail loudly instead of
                             silently returning deleted rows

ignore-deletes               skip all delete files; raw scan

ignore-unsupported-deletes   apply supported delete files, skip the rest
```

```ts
lake.table("events", { readMode: "ignore-unsupported-deletes" })
```

Strict failure:

```txt
LAQL_UNSUPPORTED_DELETE_FILES

Snapshot 482719204991 contains equality delete files.
This engine build applies position deletes only.

Set readMode: "ignore-unsupported-deletes" to scan anyway.
```

Delete support is phased in, easy first:

```txt
1. position delete files        (icebird supports today)
2. v3 deletion vectors          (icebird supports today)
3. equality delete files        (later; needs research)
```

Strict mode makes the phasing safe: a snapshot containing a not-yet-supported delete type fails loudly instead of silently returning deleted rows, so correctness never depends on which phase shipped.

The finished product applies all Iceberg delete file types.

Strict mode then only triggers on delete formats from newer Iceberg spec versions than the engine knows.

The opt-outs remain as escape hatches for intentional raw scans.

Supported writes:

```txt
append data files
overwrite partitions
create table
replace table
schema evolution
partition evolution
snapshot metadata writes
commit retry through catalog
single-writer commit coordinator
```

Cloudflare Worker commit coordinator:

```ts
export class LakeCommitCoordinator extends DurableObject {
  async append(table: string, files: DataFile[]): Promise<CommitResult> {
    return this.lake.commitAppend(table, files);
  }
}
```

Public Worker helper:

```ts
const lake = createLake({
  store: env.DATA_BUCKET,
  catalog: icebergRestCatalog(...),
  commits: durableObjectCommitter(env.LAKE_COMMITS),
});
```

Append:

```ts
await lake.table("events").insert(rows, {
  partitionBy: ["date", "h3_7"],
});
```

Overwrite partition:

```ts
await lake.table("events").overwritePartitions(rows, {
  partitionBy: ["date", "h3_7"],
});
```

Create table:

```ts
await lake.createTable("events", {
  schema: {
    id: "string",
    ts: "timestamp",
    lat: "float64",
    lon: "float64",
    h3_7: "string",
    date: "date",
    payload: "json",
  },
  partitionBy: [
    "date",
    "h3_7",
  ],
});
```

---

# Parquet support

Reads:

```txt
HTTP/R2/S3 range reads
footer reads
schema reads
column projection
row group pruning
dictionary filtering where available
statistics filtering
nested fields
batch decoding
streaming row iteration
```

Writes:

```txt
Parquet file creation
column encoding selection
compression selection
row group sizing
statistics writing
dictionary writing where supported
nested records
Iceberg data file metadata output
```

Write API:

```ts
await lake.writeParquet("tmp/events.parquet", rows, {
  schema,
  compression: "zstd",
  rowGroupSize: 50_000,
});
```

Partitioned write API:

```ts
await lake.writePartitioned("events", rows, {
  path: "events/",
  partitionBy: [
    "date",
    "h3_7",
  ],
  maxRowsPerFile: 100_000,
  maxBytesPerFile: 128 * 1024 * 1024,
});
```

Generated layout:

```txt
events/date=2026-06-10/h3_7=8729a1d75ffffff/part-01JY9M6R2C4S.parquet
events/date=2026-06-10/h3_7=8729a1d76ffffff/part-01JY9M6R2C4T.parquet
```

---

# Write semantics

`laql` distinguishes three write modes.

## 1. Plain partitioned Parquet

```ts
await lake.files("events/")
  .insert(rows)
  .partitionBy(["date", "h3_7"])
  .write();
```

No Iceberg metadata.

Useful for simple lake directories.

## 2. Iceberg append

```ts
await lake.table("events")
  .insert(rows)
  .partitionBy(["date", "h3_7"])
  .commit();
```

Writes Parquet files and commits Iceberg metadata.

## 3. CTAS

```ts
await lake.createTableAs("ca_places",
  lake.table("places")
    .where(eq("state", "CA"))
    .select(["id", "name", "lat", "lon", "h3_8"]),
  {
    partitionBy: ["h3_8"],
  },
);
```

SQL equivalent:

```sql
create table ca_places
partition by h3_8
as
from places
select id, name, lat, lon, h3_8
where state = 'CA'
```

---

# Geospatial design

Geometry columns are represented internally as:

```ts
type GeometryValue =
  | GeoJSON.Geometry
  | Wkb
  | Wkt
  | BBox;
```

Tables can declare geometry metadata:

```ts
await lake.registerTable("places", {
  path: "places/",
  format: "parquet",
  schema,
  geometry: {
    column: "geom",
    encoding: "geojson",
    srid: 4326,
    bboxColumns: {
      minx: "minx",
      miny: "miny",
      maxx: "maxx",
      maxy: "maxy",
    },
  },
});
```

When bbox columns exist, `st_intersects` gets pushdown.

Query:

```sql
from parcels
select parcel_id, owner
where st_intersects(geom, st_bbox(-118.9, 34.1, -118.6, 34.3))
```

Planner rewrite:

```txt
file/row-group pushdown
  maxx >= -118.9
  minx <= -118.6
  maxy >= 34.1
  miny <= 34.3

residual
  st_intersects(geom, bbox)
```

---

# H3 design

H3 columns are treated as typed strings with metadata.

Register:

```ts
await lake.registerTable("places", {
  path: "places/",
  format: "iceberg",
  h3: {
    columns: {
      h3_7: { resolution: 7 },
      h3_8: { resolution: 8 },
    },
  },
});
```

Generated columns:

```ts
await lake.table("places").insert(rows, {
  generated: {
    h3_7: h3Cell("lat", "lon", 7),
    h3_8: h3Cell("lat", "lon", 8),
  },
  partitionBy: ["h3_7"],
});
```

SQL:

```sql
from places
select id, name
where h3_within(h3_8, h3_cell(34.0522, -118.2437, 8), 2)
```

Planner rewrite:

```txt
h3_within(h3_8, origin, 2)
  -> h3_8 in [cell1, cell2, ...]
```

If table is partitioned by `h3_8`, this becomes partition pruning.

If table is partitioned by `h3_7`, planner can rewrite using parents:

```txt
h3_8 in cells
  -> h3_7 in distinct parents
```

Residual filter remains exact.

---

# Index sidecars

Finished product includes optional sidecar indexes.

These are not required for correctness.

Supported sidecars:

```txt
_laql/index/bbox
_laql/index/h3
_laql/index/bloom
_laql/index/minmax
_laql/index/textgram
```

Index metadata:

```json
{
  "version": 1,
  "table": "places",
  "snapshot": "482719204991",
  "indexes": [
    {
      "type": "h3",
      "column": "h3_8",
      "path": "_laql/index/h3/h3_8/",
      "granularity": "file"
    },
    {
      "type": "bbox",
      "columns": ["minx", "miny", "maxx", "maxy"],
      "path": "_laql/index/bbox/geom/",
      "granularity": "row_group"
    }
  ]
}
```

Usage:

```ts
await lake.table("places").createIndex("h3_8", {
  type: "h3",
});

await lake.table("parcels").createIndex("geom", {
  type: "bbox",
});
```

Planner automatically uses sidecars when present.

---

# Caching

Built-in cache layers:

```txt
schema cache
Iceberg metadata cache
manifest cache
Parquet footer cache
row group stats cache
small object cache
H3 expansion cache
parsed query cache
compiled expression cache
```

Cloudflare-compatible cache adapters:

```ts
memoryCache()
cacheApiCache(caches.default)
kvCache(env.LAKE_CACHE)
durableObjectCache(env.LAKE_CACHE_DO)
```

Config:

```ts
const lake = createLake({
  store,
  catalog,
  cache: {
    metadata: cacheApiCache(caches.default),
    footers: cacheApiCache(caches.default),
    expressions: memoryCache(),
  },
});
```

Cache keys include:

```txt
object path
etag
snapshot id
query hash
schema id
partition spec id
```

---

# Security model

The JSON query API is safe by construction.

No arbitrary JS execution.

No query-supplied functions. Extensions are registered by the developer at configuration time, never from query input.

No `eval`.

No filesystem access.

No network access except configured object stores/catalogs.

Policy layer:

```ts
const lake = createLake({
  store,
  catalog,
  policy: {
    tables: {
      places: {
        allowColumns: ["id", "name", "lat", "lon", "category"],
        denyColumns: ["email", "phone"],
        maxLimit: 1000,
        requireWhere: true,
      },
    },
  },
});
```

Row-level policy:

```ts
policy: {
  tables: {
    permits: {
      rowFilter: ctx => eq("tenant_id", ctx.tenantId),
    },
  },
}
```

Context is supplied per request:

```ts
const rows = await lake.withContext({ tenantId })
  .table("permits")
  .limit(100)
  .toArray();
```

Query budget:

```ts
budget: {
  maxFiles: 500,
  maxBytes: 512 * 1024 * 1024,
  maxRowsDecoded: 2_000_000,
  maxOutputRows: 10_000,
  timeoutMs: 25_000,
}
```

If exceeded:

```txt
LAQL_BUDGET_EXCEEDED

Query would scan 14,822 files.
Budget allows 500 files.

Add a partition filter, date filter, h3 filter, or limit.
```

---

# DuckDB-WASM replacement features

These are the “people using DuckDB-WASM today will switch” features.

## 1. No WASM boot ceremony

DuckDB-WASM requires bundle selection, worker setup, database instantiation, file registration, and virtual filesystem choices.

`laql`:

```ts
const lake = createLake({ store: env.BUCKET });
const rows = await lake.path("events/*.parquet").where(eq("type", "click")).limit(100).toArray();
```

## 2. Object store native

No manual file registration.

No fake local files.

No virtual filesystem.

```ts
lake.path("s3://bucket/events/date=2026-06-10/*.parquet")
lake.path("r2://events/date=2026-06-10/*.parquet")
lake.path("https://example.com/events/*.parquet")
```

## 3. Iceberg native

```ts
lake.table("events").snapshot("latest")
lake.table("events").snapshot(482719204991)
lake.table("events").asOf("2026-06-01T00:00:00Z")
```

## 4. Worker-native streaming

```ts
return new Response(query.streamNdjson());
```

## 5. H3 built in

```sql
where h3_within(h3_8, h3_cell(34.0522, -118.2437, 8), 2)
```

## 6. ST functions built in

```sql
where st_intersects(geom, st_bbox(-118.35, 33.95, -118.15, 34.15))
```

## 7. Direct partitioned writes

```ts
await lake.table("events").insert(rows, {
  partitionBy: ["date", "h3_7"],
});
```

## 8. Query budget built in

```ts
await query.withBudget({
  maxBytes: 100_000_000,
  maxOutputRows: 1000,
}).toArray();
```

## 9. Explain that talks in object-store terms

```txt
files skipped
manifests skipped
row groups skipped
columns loaded
bytes requested
range requests made
```

## 10. API-first JSON queries

DuckDB is SQL-first.

`laql` is API-first.

The JSON query format is stable, safe, serializable, cacheable, and easy for LLMs/UI builders to generate.

---

# Filesystem/path API

Simple Parquet paths:

```ts
const rows = await lake.path("events/date=2026-06-10/*.parquet")
  .select(["user_id", "ts", "event"])
  .where(eq("event", "signup"))
  .limit(100)
  .toArray();
```

Hive partition discovery:

```ts
const table = lake.hive("events/", {
  partitionColumns: ["date", "country"],
});
```

Query:

```ts
await table
  .where(and(
    eq("date", "2026-06-10"),
    eq("country", "US"),
  ))
  .toArray();
```

Glob support:

```txt
events/*.parquet
events/date=*/part-*.parquet
events/date=2026-06-*/country=US/*.parquet
```

---

# Catalog API

Register a Parquet directory:

```ts
await lake.catalog.createExternalTable("events_raw", {
  format: "parquet",
  path: "events/",
  partitionStyle: "hive",
  partitionColumns: ["date", "country"],
});
```

Register an Iceberg table:

```ts
await lake.catalog.registerIcebergTable("events", {
  metadataPath: "warehouse/events/metadata/00012.metadata.json",
});
```

List tables:

```ts
const tables = await lake.catalog.listTables();
```

Describe:

```ts
const desc = await lake.table("events").describe();
```

Schema:

```ts
const schema = await lake.table("events").schema();
```

Partitions:

```ts
const partitions = await lake.table("events").partitions();
```

Snapshots:

```ts
const snapshots = await lake.table("events").snapshots();
```

---

# CLI

Install:

```sh
npm install -g laql
```

Query Iceberg:

```sh
laql query \
  --catalog iceberg-rest \
  --catalog-url "$ICEBERG_REST_URL" \
  --table places \
  --sql "from places select id, name where state = 'CA' limit 10"
```

Query Parquet path:

```sh
laql query \
  --path "r2://data/events/date=2026-06-10/*.parquet" \
  --sql "select user_id, ts where event = 'signup' limit 100"
```

Explain:

```sh
laql explain \
  --table places \
  --sql "from places select id where h3_within(h3_8, '8829a1d757fffff', 2)"
```

Inspect Parquet:

```sh
laql parquet inspect r2://data/events/part-0001.parquet
```

Inspect Iceberg:

```sh
laql iceberg inspect places
```

Create table:

```sh
laql create-table events \
  --schema schema.json \
  --partition-by date,h3_7
```

Compact:

```sh
laql compact events \
  --target-file-size 128mb \
  --partition "date=2026-06-10"
```

---

# HTTP server mode

`laql` can expose a safe query endpoint.

```ts
import { laqlHandler } from "laql/cloudflare";

export default {
  fetch: laqlHandler({
    store: env.DATA_BUCKET,
    catalog: icebergRestCatalog({
      url: env.ICEBERG_REST_URL,
      token: env.ICEBERG_REST_TOKEN,
    }),
    policy: {
      maxLimit: 5000,
      requireLimit: true,
      denyColumns: ["ssn", "email"],
    },
  }),
};
```

Request:

```http
POST /query
content-type: application/json
```

```json
{
  "from": "places",
  "select": ["id", "name", "lat", "lon"],
  "where": {
    "eq": ["state", "CA"]
  },
  "limit": 100
}
```

Response:

```json
{
  "rows": [
    {
      "id": "p1",
      "name": "Example",
      "lat": 34.05,
      "lon": -118.24
    }
  ],
  "stats": {
    "filesRead": 3,
    "filesSkipped": 421,
    "bytesRead": 812944,
    "rowsDecoded": 1984,
    "rowsReturned": 100
  }
}
```

Streaming endpoint:

```http
POST /query.ndjson
```

Response:

```txt
{"id":"p1","name":"Example"}
{"id":"p2","name":"Example 2"}
```

---

# Result formats

Supported output formats:

```txt
json
ndjson
csv
parquet
arrow-like batches
async rows
```

JSON:

```ts
await query.toArray();
```

NDJSON:

```ts
query.streamNdjson();
```

CSV:

```ts
query.streamCsv({
  header: true,
});
```

Parquet:

```ts
await query.writeParquet("exports/result.parquet", {
  compression: "zstd",
});
```

---

# Aggregation behavior

Small aggregations run inside the Worker.

```ts
await lake.table("events")
  .where(eq("date", "2026-06-10"))
  .groupBy(["event"])
  .aggregate({
    count: count(),
    users: countDistinct("user_id"),
  })
  .toArray();
```

SQL:

```sql
from events
select event, count() as n, count_distinct(user_id) as users
where date = '2026-06-10'
group by event
having n > 100
order by n desc
```

Memory-safe aggregation requires a budget:

```ts
.groupBy(["event"], {
  maxGroups: 10_000,
})
```

If exceeded:

```txt
LAQL_GROUP_LIMIT_EXCEEDED

Aggregation produced more than 10,000 groups.
Use a lower-cardinality group key or increase maxGroups.
```

Approximate aggregates:

```txt
approx_count_distinct
top_k
histogram
quantile
```

Example:

```sql
from events
select approx_count_distinct(user_id) as users
where date = '2026-06-10'
```

---

# Sort behavior

Sorts are explicit about memory.

Allowed without special config:

```txt
order by after limit
order by aggregate result
order by small bounded scan
```

Example:

```sql
from events
select event, count() as n
where date = '2026-06-10'
group by event
order by n desc
limit 20
```

Large unbounded global sort requires a spill adapter.

```ts
const lake = createLake({
  store,
  spill: r2Spill(env.TEMP_BUCKET),
});
```

Then:

```ts
await lake.table("events")
  .orderBy("ts")
  .limit(100_000)
  .toArray();
```

Spill files are temporary and namespaced:

```txt
_laql/tmp/query-<id>/
```

---

# Joins

Finished product supports constrained joins, because real DuckDB-WASM users expect them.

Supported joins:

```txt
hash join
semi join
anti join
lookup join
broadcast join
```

Primary Worker-safe join pattern:

```ts
await lake.table("events")
  .join(lake.table("users").where(eq("country", "US")).select(["user_id", "plan"]), {
    on: "user_id",
    type: "inner",
    strategy: "broadcast",
    maxRightRows: 100_000,
  })
  .limit(1000)
  .toArray();
```

SQL:

```sql
from events
join users on events.user_id = users.user_id
select events.ts, users.plan, events.event
where users.country = 'US'
limit 1000
```

The planner rejects unsafe joins unless a spill adapter or explicit budget is configured.

---

# Text search

Basic text operations:

```txt
like
ilike
contains
starts_with
ends_with
regexp_match
```

Optional textgram sidecar index:

```ts
await lake.table("businesses").createIndex("name", {
  type: "textgram",
  minGram: 3,
  maxGram: 5,
});
```

Query:

```sql
from businesses
select id, name
where text_contains(name, 'plumber')
limit 100
```

---

# Schema evolution

Iceberg schema evolution support:

```ts
await lake.table("events").alterSchema(schema => schema
  .addColumn("session_id", "string")
  .renameColumn("user", "user_id")
  .dropColumn("legacy_field")
);
```

Parquet directory schema merge:

```ts
const schema = await lake.path("events/**/*.parquet").mergeSchema();
```

Strict mode:

```ts
await lake.writeParquet(path, rows, {
  schema,
  onSchemaMismatch: "error",
});
```

Permissive mode:

```ts
await lake.writeParquet(path, rows, {
  schema,
  onSchemaMismatch: "coerce-null",
});
```

---

# Data validation

Built-in validation:

```ts
await lake.table("events").insert(rows, {
  validate: {
    required: ["id", "ts", "event"],
    unique: ["id"],
    ranges: {
      lat: [-90, 90],
      lon: [-180, 180],
    },
    enum: {
      event: ["view", "click", "signup", "purchase"],
    },
  },
});
```

Validation failures:

```txt
LAQL_VALIDATION_ERROR

Column "lat" must be between -90 and 90.

row:
  18422

value:
  213.991
```

---

# Observability

Every query returns stats.

```ts
const q = lake.table("events")
  .where(eq("date", "2026-06-10"))
  .limit(100);

const rows = await q.toArray();
const stats = await q.stats();
```

A query handle retains the stats from its most recent execution. `stats()` reads them without re-running the query.

Stats:

```ts
type QueryStats = {
  queryId: string;
  elapsedMs: number;

  manifestsRead: number;
  manifestsSkipped: number;

  filesPlanned: number;
  filesRead: number;
  filesSkipped: number;

  rowGroupsRead: number;
  rowGroupsSkipped: number;

  columnsRead: string[];

  bytesRequested: number;
  rangeRequests: number;

  rowsDecoded: number;
  rowsMatched: number;
  rowsReturned: number;

  cacheHits: number;
  cacheMisses: number;
};
```

Logging hook:

```ts
const lake = createLake({
  store,
  catalog,
  log: event => console.log(JSON.stringify(event)),
});
```

Events:

```txt
query.start
query.plan
query.file.read
query.rowgroup.skip
query.cache.hit
query.cache.miss
query.finish
query.error
```

---

# Error model

Errors are structured.

```ts
try {
  await query.toArray();
} catch (e) {
  if (e instanceof LaQLError) {
    console.error(e.code);
    console.error(e.message);
    console.error(e.details);
  }
}
```

Codes:

```txt
LAQL_PARSE_ERROR
LAQL_TYPE_ERROR
LAQL_UNKNOWN_TABLE
LAQL_UNKNOWN_COLUMN
LAQL_UNSUPPORTED_PUSHDOWN
LAQL_BUDGET_EXCEEDED
LAQL_OBJECT_NOT_FOUND
LAQL_CATALOG_ERROR
LAQL_ICEBERG_COMMIT_CONFLICT
LAQL_UNSUPPORTED_DELETE_FILES
LAQL_PARQUET_READ_ERROR
LAQL_PARQUET_WRITE_ERROR
LAQL_VALIDATION_ERROR
LAQL_BOOKMARK_STALE
LAQL_BOOKMARK_INVALID
```

---

# Configuration

```ts
const lake = createLake({
  store,
  catalog,

  defaults: {
    limit: 1000,
    maxLimit: 100_000,
    output: "ndjson",
  },

  budget: {
    maxFiles: 1000,
    maxBytes: 1024 * 1024 * 1024,
    maxRowsDecoded: 10_000_000,
    maxOutputRows: 100_000,
  },

  parquet: {
    batchSize: 4096,
    footerCache: true,
    rowGroupPruning: true,
    dictionaryFiltering: true,
  },

  iceberg: {
    metadataCache: true,
    manifestCache: true,
    branch: "main",
    readMode: "strict",
  },

  geo: {
    defaultSrid: 4326,
    geometryEncoding: "geojson",
  },

  h3: {
    maxGridDiskK: 16,
  },
});
```

---

# Extension system

Function extension:

```ts
lake.functions.register("domain", {
  args: ["string"],
  returns: "string",
  deterministic: true,
  pushdown: false,
  eval: value => value.split("@")[1] ?? null,
});
```

Aggregate extension:

```ts
lake.aggregates.register("median_approx", {
  input: "float64",
  state: () => new QuantileSketch(),
  add: (state, value) => state.add(value),
  merge: (a, b) => a.merge(b),
  serialize: state => state.toBytes(),
  deserialize: bytes => QuantileSketch.fromBytes(bytes),
  finish: state => state.quantile(0.5),
});
```

`serialize` and `deserialize` make custom aggregates bookmark-safe.

Codec extension:

```ts
lake.codecs.register("wkb", {
  decode: bytes => decodeWkb(bytes),
  encode: geom => encodeWkb(geom),
});
```

Store extension:

```ts
lake.stores.register("my-store", myObjectStore);
```

---

# Documentation structure

```txt
docs/
  introduction.md
  why-not-duckdb-wasm.md
  cloudflare-workers.md
  querying-parquet.md
  querying-iceberg.md
  writing-parquet.md
  writing-iceberg.md
  partitioning.md
  h3.md
  geospatial.md
  query-language.md
  json-query-api.md
  sql-dialect.md
  performance.md
  security.md
  cache.md
  errors.md
  cli.md
  recipes/
    r2-parquet-api.md
    r2-iceberg-api.md
    h3-place-search.md
    bbox-search.md
    ndjson-export.md
    csv-export.md
    append-events.md
    compact-small-files.md
    tenant-safe-query-api.md
```

---

# Examples

## Places API on R2

```ts
export default {
  async fetch(req: Request, env: Env) {
    const url = new URL(req.url);

    const lat = Number(url.searchParams.get("lat"));
    const lon = Number(url.searchParams.get("lon"));
    const k = Number(url.searchParams.get("k") ?? 2);

    const lake = createLake({
      store: env.DATA_BUCKET,
      catalog: icebergCatalog({
        metadataPath: "warehouse/places/metadata/current.metadata.json",
      }),
    });

    const origin = h3Cell(lat, lon, 8);

    return new Response(
      lake.table("places")
        .select(["id", "name", "category", "lat", "lon"])
        .where(h3Within("h3_8", origin, k))
        .limit(100)
        .streamJson(),
      {
        headers: {
          "content-type": "application/json",
        },
      },
    );
  },
};
```

## Append events

```ts
const rows = await req.json();

await lake.table("events").insert(rows, {
  generated: {
    date: dateTrunc("day", col("ts")),
    h3_7: h3Cell(col("lat"), col("lon"), 7),
  },
  partitionBy: ["date", "h3_7"],
  maxRowsPerFile: 50_000,
});
```

## Export filtered result as Parquet

```ts
await lake.table("permits")
  .where(and(
    eq("county", "ventura"),
    between("issued_date", "2026-01-01", "2026-06-01"),
  ))
  .select(["id", "address", "amount", "issued_date"])
  .writeParquet("exports/ventura-permits-2026.parquet");
```

---

# Build plan (very high level)

Rough dependency order. Each phase is shippable on its own.

```txt
1. core read path
   AST, expression evaluator, planner skeleton
   @laql/http + @laql/parquet (hyparquet adapter)
   scan / filter / project / limit over plain Parquet paths
   streaming rows and NDJSON

2. pruning
   predicate split and classification
   row-group stats pruning, column projection
   Hive partition discovery and partition pruning

3. Iceberg reads
   icebird adapter: metadata, snapshots, manifests, manifest pruning
   delete handling phased in, easy first: position deletes,
   then deletion vectors, equality deletes later
   strict mode fails loudly on whatever isn't supported yet,
   so each phase ships correct
   (research: equality-delete coverage in icebird)

4. R2/S3 adapters + Worker ergonomics
   createLake, budgets, stats, explain, policy layer

5. aggregation, sort, bookmarks
   group by, aggregates, top-k sort
   slice API and bookmark serialization
   (research: operator state serialization format, plan fingerprint stability)

6. writes
   hyparquet-writer adapter, partitioned writes
   (research: zstd write codec — pluggable compressor)
   Iceberg append commit via iceberg-js + Durable Object commit coordinator
   (research: commit requirements/updates logic — laql owns it)
   resumable writes

7. the rest, additive on the core
   geo + H3, SQL dialect, sidecar indexes, CLI, joins, spill
```

---

# Polished product checklist

The finished OSS product includes:

```txt
TypeScript-first fluent API
stable JSON query API
small SQL dialect
Cloudflare Worker adapter
R2 object store adapter
HTTP range-read adapter
S3-compatible adapter
Iceberg table reads
Iceberg position delete reads
Iceberg equality delete reads
Iceberg table writes
plain Parquet reads
plain Parquet writes
Hive partition discovery
partitioned directory writes
H3 functions
ST functions
bbox pushdown
H3 partition pushdown
Parquet column projection
Parquet row-group pruning
Iceberg manifest pruning
streaming JSON
streaming NDJSON
streaming CSV
Parquet export
query explain
query stats
resumable execution bookmarks
cursor pagination tokens
structured errors
safe query budgets
policy layer
CLI
docs
recipes
test fixtures
benchmark suite
conformance tests
```

---

# What the README says

````md
# LaQL

LaQL is a lightweight TypeScript query engine for Parquet and Iceberg on object storage.

It runs in Cloudflare Workers, browsers, Node, Deno, Bun, and service workers.

It is designed for API builders who want to query lake data directly from R2, S3, or HTTP without running DuckDB-WASM, Spark, Trino, or a database server.

```ts
const rows = await lake.table("places")
  .select(["id", "name", "lat", "lon"])
  .where(and(
    eq("state", "CA"),
    h3Within("h3_8", "8829a1d757fffff", 2),
  ))
  .limit(100)
  .toArray();
```

LaQL supports:

* Iceberg tables
* Parquet files
* partition pruning
* row-group pruning
* column projection
* streaming results
* H3 functions
* geospatial functions
* partitioned writes
* Iceberg appends
* safe JSON query APIs
* resumable queries with bookmarks
* Worker-native R2 access

````

---

# The key design rule

Every feature has to answer one of these questions:

```txt
Can it skip files?
Can it skip row groups?
Can it skip columns?
Can it stream results safely?
Can it write lake-compatible files?
Can it make Worker APIs simpler than DuckDB-WASM?
```

If yes, it belongs.

If no, it belongs outside the core.
