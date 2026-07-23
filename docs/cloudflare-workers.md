# Cloudflare Workers

Use the `lakeql/cloudflare` subpath for Worker-oriented imports. It provides
`r2Store` for native R2 bindings and also exposes the portable `httpStore` and
`s3Store` adapters for HTTP and S3-compatible object storage.

```ts
import { createLake } from "lakeql/cloudflare";
import { r2Store } from "lakeql-r2";

export default {
  async fetch(_request: Request, env: { DATA: R2Bucket }) {
    const lake = createLake({ store: r2Store(env.DATA) });
    const rows = await lake.path("sales.parquet").limit(10).toArray();
    return Response.json(rows);
  },
};
```

For queue-sized work, use `query.run({ slice })` and pass the returned bookmark through your queue or durable state. New bookmarks include the query shape, so a later invocation can call `lake.resume(bookmark).run({ slice: { maxRows } })`.

Keep Worker reads bounded with query budgets:

```ts
const controller = new AbortController();
const lake = createLake({
  store: r2Store(env.DATA),
  budget: {
    maxBufferedRows: 4096,
    maxConcurrentReads: 4,
    maxRangeRequests: 128,
    signal: controller.signal,
  },
});
```

lakeql reads Parquet metadata and row groups with range requests. Peak row buffering is controlled by
`maxBufferedRows`; object-read fanout is controlled by `maxConcurrentReads`; cancellation rejects at
await boundaries with `LAKEQL_ABORTED`.

## Deployable example

`examples/worker/` contains a Worker that reads `sales.parquet` from R2 at `/parquet` and plans an
Iceberg table from R2 metadata at `/iceberg`. It is covered by the `test:workerd` lane so the example
keeps compiling and running in the same runtime model as a deployed Worker.

Configure `examples/worker/wrangler.toml` with your R2 bucket binding, then deploy with Wrangler:

```sh
cd examples/worker
pnpm exec wrangler deploy
```
