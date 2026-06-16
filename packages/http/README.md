# @laql/http

HTTP range-read object-store adapter for LaQL.

## Ownership

This package adapts HTTP-addressable objects to the `@laql/core` `ObjectStore` contract. It is
useful for public buckets, signed URLs, local fixture servers, and services that support byte-range
requests.

## Public Surface

- `httpStore(options)` creates an `ObjectStore`.
- `HttpObjectStore` implements `get`, `getRange`, `head`, `put`, `delete`, and indexed `list`.
- `HttpStoreOptions` configures `baseUrl`, optional headers, optional custom `fetch`, and an optional object index for `list`.

`getRange` sends standard HTTP `Range` requests. `list` requires the caller to provide an object
index because generic HTTP servers do not expose portable object listings.
