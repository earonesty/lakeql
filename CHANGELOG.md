# Changelog

## Unreleased

- No changes yet.

## 0.1.6

- Improve cached Parquet scan performance with decoded page-vector reuse, lower-copy vector slicing, and faster grouped aggregate selection paths.

## 0.1.5

- Fix vector group-by partial merging across JSON work-unit boundaries.

## 0.1.4

- Add the browser compare page against DuckDB-WASM on the public flights Parquet dataset.
- Improve warm-cache vector Parquet performance for sparse Top-K and grouped aggregate queries.

## 0.1.3

- Initial public package set is under active development.
- Release the current public `lakeql` wrapper package.
- Refresh the browser demo to display and copy the current npm version.
