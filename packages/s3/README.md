# @laql/s3

S3-compatible SigV4 object-store adapter for LaQL.

## Ownership

This package adapts AWS S3 and S3-compatible object stores to the `@laql/core`
`ConditionalObjectStore` contract. It owns SigV4 request signing, safe key encoding, paginated
`ListObjectsV2` parsing, ranged reads, and conditional writes.

## Public Surface

- `s3Store(options)` creates an object-store adapter for AWS S3 and S3-compatible endpoints.
- `S3ObjectStore` implements `ObjectStore` plus `ConditionalObjectStore`.
- Reads support full-object `get`, ranged `getRange`, `head`, paginated `list`, `put`, and `delete`.
- `conditionalPut` maps create-if-absent and ETag compare-and-swap writes onto S3 precondition headers.

The adapter signs requests with `aws4fetch`, parses `ListObjectsV2` XML with `fast-xml-parser`, and rejects
absolute or traversal-style object keys before signing.

SigV4 coverage uses AWS-published S3 documentation vectors for explicit-payload signatures and the
provider lane runs real S3-compatible operations against MinIO. The generic
`@saibotsivad/aws-sig-v4-test-suite` package was not vendored because its published license is not
Apache-2.0 compatible.

## Dependency Size

S3 hardening replaced bespoke signing/XML code with focused runtime dependencies:

| Dependency | Purpose | Installed size | Distributed JS artifact |
|---|---:|---:|---:|
| `aws4fetch@1.0.20` | SigV4 signing | 72 KB unpacked | 11.3 KB ESM |
| `fast-xml-parser@5.9.0` | `ListObjectsV2` XML parsing | 1.4 MB unpacked | 75.9 KB minified bundle |

These are runtime package sizes from the pinned lockfile, not application-specific bundled output.
