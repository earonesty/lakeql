# Geospatial

The current geospatial functions operate over GeoJSON or BBox JSON strings:

```ts
fn("st_point", lit(-118.24), lit(34.05));
fn("st_x", col("geom"));
fn("st_y", col("geom"));
fn("st_intersects", col("bbox"), lit(JSON.stringify([0, 0, 10, 10])));
fn("st_contains", col("geom"), lit(JSON.stringify({ type: "Point", coordinates: [1, 2] })));
fn("st_within", col("geom"), col("bbox"));
fn("st_disjoint", col("geom"), col("bbox"));
fn("st_distance", col("geom"), col("bbox"));
fn("st_area", col("geom"));
fn("st_length", col("geom"));
fn("st_centroid", col("geom"));
fn("st_envelope", col("geom"));
```

Sidecar bbox indexes can prune files for `st_intersects` when a file-level bbox cannot overlap the query bbox.
