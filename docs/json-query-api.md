# JSON Query API

`lake.query()` accepts versioned JSON query objects.

```ts
const query = lake.query({
  version: 1,
  from: "sales.parquet",
  select: ["store_id", "amount"],
  where: { gt: ["amount", 100] },
  orderBy: [{ column: "amount", direction: "desc" }],
  limit: 10,
});
```

The `where` and `qualify` fields accept the shorthand predicate form shown above or the
expression AST used by the TypeScript builder:

```ts
const topStores = lake.query({
  version: 1,
  from: "sales.parquet",
  select: ["region", "store_id", "amount", "rank_in_region"],
  windows: {
    rank_in_region: {
      fn: "row_number",
      args: [],
      over: {
        partitionBy: [{ kind: "column", name: "region" }],
        orderBy: [
          { expr: { kind: "column", name: "amount" }, direction: "desc" },
          { expr: { kind: "column", name: "store_id" }, direction: "asc" },
        ],
      },
    },
  },
  qualify: {
    kind: "compare",
    op: "lte",
    left: { kind: "column", name: "rank_in_region" },
    right: { kind: "literal", value: 3 },
  },
});
```

Invalid shapes throw typed `LAKEQL_PARSE_ERROR` or `LAKEQL_TYPE_ERROR` errors depending on where validation fails.
