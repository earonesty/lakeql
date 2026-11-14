# lakeql-parquet

Parquet support for LakeQL. This package reads Parquet files from object stores,
plans row groups, validates supported schemas, fills missing columns for
compatible multi-file reads, and writes Parquet output.

Most applications should import from `lakeql`, `lakeql/node`, or
`lakeql/cloudflare`. Use `lakeql-parquet` directly when you need lower-level
Parquet metadata, row-group planning, or writer APIs.

## Main Exports

- `createParquetLake` creates a queryable lake over Parquet files.
- `parquetScanner` connects Parquet files to the core query engine.
- `readParquetObjects` and `readParquetObjectBatches` read Parquet rows from an
  object store.
- `readParquetMetadata` reads footer metadata through ranged object-store
  access.
- `encodedParquetMetadataCache` adapts persistent byte caches to parsed metadata
  without losing bigint or byte fields.
- `planRowGroups` and `planRowGroupsFromMetadata` expose row-group pruning.
- `rejectUnsupportedParquetSchema` rejects unsupported nested schema features
  before rows are returned.
- `writeParquet`, `writePartitionedParquet`, and task/checkpoint helpers write
  Parquet output and manifests.

See [Parquet types](../../docs/parquet-types.md) for supported Parquet types and
nested-column behavior.

Multi-file object planning uses `planOwnedObjects` when called by LakeQL. It
admits schema topology, descriptor/signature construction, compatibility maps,
shared column names, and per-file presence sets under the query allocation budget.
The returned plan retains its input owner; release it after its objects are no
longer used. Direct legacy `planObjects` calls still return caller-managed metadata.
Object planning observes `options.signal` between files and after metadata reads
settle. These reservations do not represent a total runtime heap limit.

When `cache` is enabled, the default planning cache shares the lake's memory pool
and inherits `cache.maxBytes` and `cache.ttlMs`. Discovery entries are reclaimable
under shared-pool pressure; zero capacity disables retention. An explicit
`planningCache` continues to override the default. No second pool configuration is
needed for default discovery caching.

With `cache` enabled, the factory also passes the existing pool and cache capacity
as defaults to stores that support `withDefaultMemoryBudget`. HTTP probe caching
therefore needs no duplicate pool setting. Explicit adapter pool/cache options
remain authoritative, and a zero factory cache capacity disables default probe
retention. URI, cache and read-control wrappers preserve this capability; rebound
read-control stores share the original semaphore and cancellation signal.
