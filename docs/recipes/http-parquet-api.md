# Recipe: HTTP Parquet API

Read a Parquet object from an HTTP endpoint that supports byte-range requests:

```ts
import { queryHttpParquet } from "../../examples/http-parquet";

const rows = await queryHttpParquet({
  baseUrl: "https://data.example/lake/",
  objects: [{ path: "sales.parquet", size: 1234 }],
});
```

The example uses `laql/node` and `@laql/http` public exports. Generic HTTP servers do not expose
portable listings, so pass an `objects` index when code needs `list()`.
