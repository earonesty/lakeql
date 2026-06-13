# Recipe: CSV Export

Run from the repository root:

```sh
pnpm build
node packages/cli/dist/bin.js query \
  --path fixtures/data/sales.parquet \
  --sql "select store_id, amount where region = 'west' order by amount asc limit 2" \
  --format csv
```

Expected fixture output:

```csv
store_id,amount
store-000,0
store-000,36.28
```
