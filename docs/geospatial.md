# Geospatial

The geospatial functions operate over GeoJSON strings, BBox JSON strings, WKT point strings such as
`POINT(-118.24 34.05)`, and WKB bytes (`Uint8Array`). GeoParquet `GEOMETRY` and `GEOGRAPHY`
byte-array columns are read as WKB bytes so the same expressions work directly over Parquet scans:

```ts
fn("st_point", lit(-118.24), lit(34.05));
fn("st_x", col("geom"));
fn("st_y", col("geom"));
fn("st_intersects", col("bbox"), lit(JSON.stringify({ type: "BBox", minx: 0, miny: 0, maxx: 10, maxy: 10 })));
fn("st_contains", col("geom"), lit(JSON.stringify({ type: "Point", coordinates: [1, 2] })));
fn("st_within", col("geom"), col("bbox"));
fn("st_within", col("geometry"), fn("st_bbox", lit(-119), lit(33), lit(-117), lit(35)));
fn("st_disjoint", col("geom"), col("bbox"));
fn("st_dwithin", col("geom"), fn("st_point", lit(-118.24), lit(34.05)), lit(0.01));
fn("st_distance", col("geom"), col("bbox"));
fn("st_area", col("geom"));
fn("st_length", col("geom"));
fn("st_centroid", col("geom"));
fn("st_envelope", col("geom"));
```

## Predicates are exact

`st_intersects`, `st_contains`, `st_within`, `st_disjoint`, and `st_dwithin` return exact
geometry answers, computed with [Turf](https://turfjs.org) (`@turf/boolean-intersects`,
`@turf/boolean-contains`). Bounding boxes are used only as a cheap prefilter: a
few float comparisons decide the obvious non-matches without parsing full
geometry, and Turf runs only on the candidates whose envelopes overlap. Two
polygons whose bounding boxes overlap but whose shapes do not touch correctly
report `st_intersects = false`.

`st_contains`, `st_within`, and `st_dwithin` have vectorized paths for string, WKB binary, and
dictionary-encoded geometry columns when one side of the predicate is a constant geometry.
`st_dwithin` uses the same exact intersection backend for zero-distance cases, then computes planar
point/segment/ring distance for supported Point, LineString, Polygon, and BBox geometries.
`st_distance` is still an envelope (bounding-box) operation. `st_area`, `st_length`, and
`st_centroid` operate on the parsed geometry directly (`st_centroid` returns the envelope center).

## File pruning

Sidecar bbox indexes prune files for `st_intersects` when a file-level bounding
box cannot overlap the query bounding box — the same prefilter, applied one
level up so non-matching files are never read at all.
