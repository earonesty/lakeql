# @laql/geo

Geospatial and H3 expression builders for LaQL.

## Ownership

This package owns typed helpers for geospatial and H3-flavored expressions. It builds core `Expr`
values and lightweight geometry objects; it does not decode geometry from Parquet by itself.

## Public Surface

- Geometry helpers: `stPoint`, `stBBox`, `stEnvelope`, `stIntersects`, `stDisjoint`, `stContains`, `stWithin`, `stX`, `stY`, `stAsGeojson`, and `stFromGeojson`.
- Expression helpers: `stIntersectsExpr`, `h3Cell`, `h3Within`, `h3Parent`, and `h3In`.
- Types: `Position`, `PointGeometry`, `PolygonGeometry`, `Geometry`, `BBox`, and `GeometryLike`.

See `docs/geospatial.md` and `docs/h3.md` in the repository root for query examples.
