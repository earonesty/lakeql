# Geospatial

The current geospatial functions operate over GeoJSON or BBox JSON strings:

```ts
fn("st_point", lit(-118.24), lit(34.05));
fn("st_x", col("geom"));
fn("st_y", col("geom"));
fn("st_intersects", col("bbox"), lit(JSON.stringify({ type: "BBox", minx: 0, miny: 0, maxx: 10, maxy: 10 })));
fn("st_contains", col("geom"), lit(JSON.stringify({ type: "Point", coordinates: [1, 2] })));
fn("st_within", col("geom"), col("bbox"));
fn("st_disjoint", col("geom"), col("bbox"));
fn("st_distance", col("geom"), col("bbox"));
fn("st_area", col("geom"));
fn("st_length", col("geom"));
fn("st_centroid", col("geom"));
fn("st_envelope", col("geom"));
```

## Predicates are exact

`st_intersects`, `st_contains`, `st_within`, and `st_disjoint` return exact
geometry answers, computed with [Turf](https://turfjs.org) (`@turf/boolean-intersects`,
`@turf/boolean-contains`). Bounding boxes are used only as a cheap prefilter: a
few float comparisons decide the obvious non-matches without parsing full
geometry, and Turf runs only on the candidates whose envelopes overlap. Two
polygons whose bounding boxes overlap but whose shapes do not touch correctly
report `st_intersects = false`.

`st_distance` is still an envelope (bounding-box) operation. `st_area`,
`st_length`, and `st_centroid` operate on the parsed geometry directly
(`st_centroid` returns the envelope center).

## File pruning

Sidecar bbox indexes prune files for `st_intersects` when a file-level bounding
box cannot overlap the query bounding box — the same prefilter, applied one
level up so non-matching files are never read at all.
