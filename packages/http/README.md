# lakeql-http

HTTP range-read object-store adapter for Lakeql.

## Ownership

This package adapts HTTP-addressable objects to the `lakeql-core` `ObjectStore` contract. It is
useful for public buckets, signed URLs, local fixture servers, and services that support byte-range
requests.

## Public Surface

- `httpStore(options)` creates an `ObjectStore`.
- `HttpObjectStore` implements `get`, `getRange`, `head`, `put`, `delete`, and indexed `list`.
- `HttpStoreOptions` configures `baseUrl`, optional headers, optional custom `fetch`, and an optional object index for `list`.

`getRange` sends standard HTTP `Range` requests. `list` requires the caller to provide an object
index because generic HTTP servers do not expose portable object listings.

## Response and fallback limits

Responses are consumed incrementally. `maxResponseBytes` caps decoded payloads
at 16 MiB by default, including explicit `get`, size probes, and full-response
fallback when a server ignores ranges. Oversized responses are cancelled with
`LAKEQL_BUDGET_EXCEEDED`, even when their content length is missing or misleading.
Set a larger limit explicitly when an application needs larger individual reads.

`maxFullObjectCacheBytes` bounds retained full-response fallback payloads at
16 MiB by default. The cache evicts entries automatically; setting this limit to
zero disables payload retention. These limits replace unbounded response buffering
and fallback caching. They do not bound the sum of concurrent reads or total runtime
memory. Buffer growth and final compaction can temporarily hold two buffers, and
the Fetch implementation owns its incoming chunks. Shared query allocation
accounting must also cover those lifetimes to provide an engine-wide cap.

`HttpObjectStore.headOwned(path, scope)` admits range-probe buffers, full-response
fallback buffers, metadata records and retained header strings in the supplied
scope. Compressed range probes use an admitted full-response fallback. Returned
metadata is releasable ownership. Probe bodies are released after metadata
construction; they are not inserted into the legacy full-response cache. Runtime
network chunks remain external to these engine allocation reservations.

Pass `memoryBudget` in `HttpStoreOptions` to retain full owned probe responses in
the same shared allocation pool as queries. Owned range reads reuse matching-pool
entries with independent copies. Cache entry/key overhead is charged alongside
payload; the existing `maxFullObjectCacheBytes` limit and shared-pool reclamation
bound retention. Insufficient capacity simply declines caching. Without a supplied
pool, owned probe bodies release after metadata construction. This makes pool
ownership explicit rather than retaining query allocations in an unrelated cache.
`clearFullObjectCache()` releases retained responses; successful PUT/DELETE also
invalidate the affected entry. Existing returned ranges remain valid.
