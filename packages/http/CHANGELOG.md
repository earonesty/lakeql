# lakeql-http

## 0.0.10

### Patch Changes

- Updated dependencies [f7e1c58]
- Updated dependencies [f8a4d39]
  - lakeql-core@0.6.0

## 0.0.9

### Patch Changes

- Updated dependencies
  - lakeql-core@0.5.0

## 0.0.8

### Patch Changes

- Updated dependencies [5e21864]
  - lakeql-core@0.4.0

## 0.0.7

### Patch Changes

- 8d687ff: Detect static hosts that serve byte ranges over compressed assets and fall back to cached full-object slicing so Parquet footer reads use decoded byte offsets.
- Updated dependencies
  - lakeql-core@0.3.0

## 0.0.6

### Patch Changes

- Updated dependencies [87aec8a]
  - lakeql-core@0.2.0

## 0.0.5

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.3

## 0.0.4

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.2

## 0.0.3

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.1

## 0.0.2

### Patch Changes

- 7402210: Bind the default global `fetch` to `globalThis` in `httpStore`. Browsers throw
  `TypeError: Illegal invocation` when `fetch` is invoked as a method with a
  non-global `this`; Node and workerd tolerated it, so this only surfaced when
  running Lakeql directly in a browser (e.g. the playground). A caller-supplied
  `fetch` is still used as-is.
- ec16676: Make `httpStore` robust against static hosts that compress responses and treat
  `Range` as advisory (e.g. GitHub Pages), which previously corrupted Parquet
  reads with "footer != PAR1":

  - `head()` now derives object size from a 1-byte ranged GET's `content-range`
    total instead of a `HEAD`'s `content-length`. Under transparent compression a
    `HEAD` reports the _compressed_ size, but range reads operate on the
    uncompressed object — using the compressed size made the reader scan the
    wrong region.
  - `getRange()` slices the response client-side when a server ignores `Range` and
    returns `200` with the full body, instead of returning the whole object as if
    it were the requested window.

- Updated dependencies [08c94d5]
- Updated dependencies [6547014]
  - lakeql-core@0.1.0
