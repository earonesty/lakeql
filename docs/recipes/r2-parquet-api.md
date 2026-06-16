# Recipe: R2 Parquet API

This Worker reads Parquet rows from R2 and returns JSON.

```ts
import { queryR2Parquet } from "../../examples/r2-parquet";

export default {
  async fetch(_request: Request, env: { DATA: R2Bucket }) {
    return Response.json(await queryR2Parquet(env.DATA));
  },
};
```

Fixture equivalent:

```sh
pnpm build
node packages/cli/dist/bin.js query --path fixtures/data/sales.parquet --sql "select store_id, amount limit 2" --format json
```
