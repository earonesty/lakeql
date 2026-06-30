# Browser R2 Benchmark

This benchmark compares Lakeql and DuckDB-Wasm in the browser against the same remote Parquet
object. Native DuckDB is not part of the measured lane.

Open `site/compare.html` through the Vite dev server or the built site. The default source is the
public R2 flights Parquet object used by the existing compare page. A spatial R2 fixture can be
selected without rebuilding:

```text
compare.html?kind=spatial&source=https%3A%2F%2Fexample.r2.dev%2Fspatial.parquet&key=spatial.parquet&size=123456789
```

Measured lanes:

- `lakeql-on-r2`: browser Lakeql reads the configured object through `lakeql-http` with HTTP range
  requests.
- `duck-wasm-on-r2`: browser DuckDB-Wasm registers the same remote object URL and reads it through
  DuckDB-Wasm's browser HTTP file path.

Cold DuckDB-Wasm runs include WASM startup, file registration, file statistics collection, and
spatial extension install/load when `kind=spatial`. Warm DuckDB-Wasm runs reuse the instantiated
database. Lakeql fresh runs recreate the Lake runtime; warm runs reuse Lakeql caches within the
selected memory budget.

The browser service worker proxies the configured source URL only to count browser HTTP requests and
bytes. It preserves incoming `Range` headers and fetches the configured HTTPS source, so both engines
exercise the same browser-to-remote-object path.

Spatial parity status:

- Comparable now: browser-to-R2 `st_dwithin`, `st_within`, and `st_contains` over the fixture
  `geometry` column. Lakeql reads GeoParquet `GEOMETRY`/`GEOGRAPHY` columns as WKB bytes; the
  DuckDB-Wasm lane rewrites the same predicates through `ST_GeomFromWKB(geometry)`.
- Still useful to expand: larger public Overture/GeoParquet fixtures and bbox-column pruning once
  the public benchmark target is fixed.

Smoke gate:

```sh
pnpm bench:browser-r2
```

That command builds the packages and browser benchmark surface. Timing numbers must be collected in
the browser page.
