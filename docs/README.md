# LakeQL Docs

Start with the guide that matches what you want to do, then use the reference
pages when you need exact behavior.

## Start Here

- [Introduction](./introduction.md): install LakeQL and run a first query.
- [Examples](./examples.md): copy complete Node.js and Cloudflare Worker
  examples.
- [Querying Parquet](./querying-parquet.md): query one file, many files, or
  named tables.
- [Querying Lance](./querying-lance.md): materialize stable row IDs and use
  supported scalar or vector indexes without native bindings.
- [SQL guide](./sql-dialect.md): supported SQL syntax and result helpers.

## Guides

- [Cloudflare Workers](./cloudflare-workers.md)
- [Querying Iceberg](./querying-iceberg.md)
- [WebGPU execution](./webgpu.md)
- [Writing Parquet](./writing-parquet.md)
- [Writing Iceberg](./writing-iceberg.md)
- [Partitioning](./partitioning.md)
- [Cache configuration](./cache.md)
- [CLI](./cli.md)
- [Performance](./performance.md)
- [Security](./security.md)

## Reference

- [Compatibility matrix](./compatibility.md)
- [Unsupported features](./unsupported.md)
- [Error codes](./errors.md)
- [Parquet types](./parquet-types.md)
- [Iceberg catalogs](./catalogs.md)
- [JSON query API](./json-query-api.md)
- [Query language objects](./query-language.md)
- [Conformance](./conformance.md)
- [Lance benchmark](./lance-random-read-benchmark.md)

## Recipes

- [HTTP Parquet API](./recipes/http-parquet-api.md)
- [R2 Parquet API](./recipes/r2-parquet-api.md)
- [R2 Iceberg API](./recipes/r2-iceberg-api.md)
- [Append events](./recipes/append-events.md)
- [Compact small files](./recipes/compact-small-files.md)
- [CSV export](./recipes/csv-export.md)
- [NDJSON export](./recipes/ndjson-export.md)
- [Bounding-box search](./recipes/bbox-search.md)
- [H3 place search](./recipes/h3-place-search.md)

## Plans

Plans and follow-up trackers are contributor notes. They describe design work
that may already be implemented, superseded, or awaiting another pass.

- [DX follow-ups](./dx-followups.md)
- [Lance vector search contributor plan](./lance-edge-vector-search.md)
- [WebGPU-accelerated execution contributor plan](./webgpu-accelerated-execution.md)
