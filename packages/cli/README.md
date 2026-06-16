# @laql/cli

Command-line interface for querying, inspecting, writing, and compacting LaQL lake data.

## Ownership

This package owns the `laql` executable. It is a Node-only interface over the core query engine,
the Parquet adapter, and the small SQL dialect parser.

## Commands

- `query --path <file.parquet> --sql <query> [--format csv|json|ndjson]` reads a local Parquet file.
- `explain --path <file.parquet> --sql <query>` prints the query plan.
- `inspect --path <file.parquet>` prints row-group and column counts.
- `schema --path <file.parquet>` prints basic schema metadata.
- `write --path <file.parquet> --sql <query> --output <prefix>` writes query output as Parquet.
- `compact --path <file.parquet> --output <prefix>` rewrites rows into one or more Parquet files.

The CLI currently targets local fixture-style workflows. Object-store production reads should use
the library packages directly.
