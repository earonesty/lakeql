# Cloudflare Workers

Use the `laql/cloudflare` subpath for Worker-oriented imports and `@laql/r2` for R2-backed object access.

```ts
import { createLake } from "laql/cloudflare";
import { r2Store } from "@laql/r2";

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

LaQL reads Parquet metadata and row groups with range requests. Peak row buffering is controlled by
`maxBufferedRows`; object-read fanout is controlled by `maxConcurrentReads`; cancellation rejects at
await boundaries with `LAQL_ABORTED`.

## Deployable example

`examples/worker/` contains a Worker that reads `sales.parquet` from R2 at `/parquet` and plans an
Iceberg table from R2 metadata at `/iceberg`. It is covered by the `test:workerd` lane so the example
keeps compiling and running in the same runtime model as a deployed Worker.

Configure `examples/worker/wrangler.toml` with your R2 bucket binding, then deploy with Wrangler:

```sh
cd examples/worker
pnpm exec wrangler deploy
```
