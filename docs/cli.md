# CLI

Build first so the `laql` bin points at compiled files:

```sh
pnpm build
```

Commands:

```sh
node packages/cli/dist/bin.js schema --path fixtures/data/sales.parquet
node packages/cli/dist/bin.js inspect --path fixtures/data/sales.parquet
node packages/cli/dist/bin.js explain --path fixtures/data/sales.parquet --sql "select amount where amount > 900"
node packages/cli/dist/bin.js query --path fixtures/data/sales.parquet --sql "select store_id, amount limit 2"
node packages/cli/dist/bin.js query --path fixtures/data/sales.parquet --sql "select store_id, amount limit 2" --format csv
node packages/cli/dist/bin.js write --path fixtures/data/sales.parquet --sql "select store_id, region, amount limit 2" --output /tmp/laql-out
node packages/cli/dist/bin.js compact --path fixtures/data/sales.parquet --output /tmp/laql-compact --max-rows-per-file 75
```
