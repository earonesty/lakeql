# @laql/r2

Cloudflare R2 object-store adapter for LaQL.

## Ownership

This package adapts a Cloudflare R2 bucket binding to the `@laql/core` `ObjectStore` contract. It is
intended for Workers and workerd-compatible runtimes.

## Public Surface

- `r2Store(bucket)` creates an `ObjectStore` backed by an R2 bucket binding.
- `R2ObjectStore` implements `get`, `getRange`, `head`, `put`, `delete`, and paginated `list`.
- `R2BucketLike` and `R2ObjectBody` describe the minimal R2 API shape used by the adapter.

Range reads are mapped to R2 ranged `get` calls. Object metadata is surfaced through `head` and
`list` where R2 provides size, ETag, upload time, and content type.
