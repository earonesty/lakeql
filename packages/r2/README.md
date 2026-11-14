# lakeql-r2

Cloudflare R2 object-store adapter for Lakeql.

## Ownership

This package adapts a Cloudflare R2 bucket binding to the `lakeql-core` `ObjectStore` contract. It is
intended for Workers and workerd-compatible runtimes.

## Public Surface

- `r2Store(bucket)` creates an `ObjectStore` backed by an R2 bucket binding.
- `R2ObjectStore` implements `get`, `getRange`, `head`, `put`, `delete`, and paginated `list`.
- `R2BucketLike` and `R2ObjectBody` describe the minimal R2 API shape used by the adapter.

Range reads are mapped to R2 ranged `get` calls. Object metadata is surfaced through `head` and
`list` where R2 provides size, ETag, upload time, and content type.

Allocation-aware callers can use `R2ObjectStore.getRangeOwned(path, range, scope)`
with a core `MemoryScope`. It reserves returned range buffers against the scope,
streams the response, and rejects a body exceeding the requested range length.
Closing the scope cancels body consumption; the R2 binding's pending `get()` itself
has no abort-signal parameter. Partial reads at the object tail reserve only the
remaining bytes. Custom `R2BucketLike` implementations must expose `body` for this
method; the adapter does not fall back to unbounded `arrayBuffer()` materialization.
The existing `getRange()` API retains its byte-returning behavior. These buffer
charges do not include runtime-owned stream chunks or decoded query objects.

`putOwned(path, body, options)` retains the supplied byte owner through R2 upload
settlement and admits the adapter's options records in that owner's budget. It
passes the existing view directly to the R2 binding without an adapter payload
copy. Metadata strings/maps are borrowed from the immutable caller options;
R2 runtime upload buffering remains outside the adapter's accounting. Owned JSON
cache writes automatically use this capability.
