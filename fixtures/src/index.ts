import { fileURLToPath } from "node:url";

export const fixtureDataDir = fileURLToPath(new URL("../data/", import.meta.url));
export const fixtureExternalDir = fileURLToPath(new URL("../external/", import.meta.url));

export function fixturePath(name: string): string {
  return fileURLToPath(new URL(`../data/${name}`, import.meta.url));
}

export function externalFixturePath(name: string): string {
  return fileURLToPath(new URL(`../external/${name}`, import.meta.url));
}

/** Shapes the generator guarantees; tests assert against these. */
export const SALES = {
  file: "sales.parquet",
  rows: 100,
  rowGroupSize: 40, // 100 rows -> row groups of 40, 40, 20
  regions: ["west", "east", "north", "south"],
} as const;

export const TYPES = {
  file: "types.parquet",
  rows: 10,
} as const;

export const WIDE = {
  file: "wide.parquet",
  rows: 24,
  columns: 32,
} as const;

export const STATS = {
  file: "stats.parquet",
  rows: 30,
  rowGroupSize: 10,
} as const;

export const GROUPBY = {
  file: "groupby.parquet",
  rows: 8,
  groups: 4,
} as const;

export const GEO = {
  file: "geo.parquet",
  rows: 3,
  rowGroupSize: 1,
} as const;

export const H3 = {
  file: "h3.parquet",
  rows: 4,
  rowGroupSize: 1,
} as const;

export const WRITE = {
  file: "write-golden/plain.parquet",
  rows: 3,
} as const;

export const MANIFESTS = {
  taskManifest: "manifests/task-manifest.golden.json",
  parquetTaskManifest: "manifests/parquet-task-manifest.golden.json",
  outputManifest: "manifests/output-manifest.golden.json",
  bookmark: "manifests/bookmark.golden.json",
  retryLog: "manifests/retry-log.golden.json",
} as const;

export const HIVE = {
  files: [
    "hive/date=2026-01-01/country=US/part-000.parquet",
    "hive/date=2026-01-02/country=CA/part-000.parquet",
    "hive/date=2026-01-02/country=US/part-000.parquet",
  ],
  rowsPerFile: 4,
} as const;

export const ICEBERG = {
  tableLocation: "iceberg/warehouse/places",
  metadataFile: "iceberg/warehouse/places/metadata/v2.metadata.json",
  manifestRefMetadataFile: "iceberg/warehouse/places/metadata/v2.manifest-ref.metadata.json",
  manifestListMetadataFile: "iceberg/warehouse/places/metadata/v2.manifest-list.metadata.json",
  v1MetadataFile: "iceberg/warehouse/places/metadata/v1.metadata.json",
  v1ManifestListFile: "iceberg/warehouse/places/metadata/snap-1.v1.manifest-list.avro",
  v1ManifestFile: "iceberg/warehouse/places/metadata/manifest-1.v1.avro",
  manifestListFile: "iceberg/warehouse/places/metadata/snap-2.manifest-list.avro",
  legacyManifestListFile: "iceberg/warehouse/places/metadata/snap-2.manifest-list.json",
  multiManifestMetadataFile: "iceberg/warehouse/places/metadata/v2.multi-manifest.metadata.json",
  manifestFiles: [
    "iceberg/warehouse/places/metadata/manifest-1.avro",
    "iceberg/warehouse/places/metadata/manifest-2-data.avro",
    "iceberg/warehouse/places/metadata/manifest-2-deletes.avro",
    "iceberg/warehouse/places/metadata/manifest-2-us.avro",
    "iceberg/warehouse/places/metadata/manifest-2-ca.avro",
  ],
  legacyManifestFiles: [
    "iceberg/warehouse/places/metadata/manifest-1.json",
    "iceberg/warehouse/places/metadata/manifest-2.json",
  ],
  plannedFilesGolden: "iceberg/warehouse/places/plans/current-us.golden.json",
  plannedTasksGolden: "iceberg/warehouse/places/plans/current-us-row-groups.golden.json",
  dataFiles: [
    "iceberg/warehouse/places/data/date=2026-01-01/country=US/part-000.parquet",
    "iceberg/warehouse/places/data/date=2026-01-02/country=CA/part-000.parquet",
    "iceberg/warehouse/places/data/date=2026-01-02/country=US/part-000.parquet",
  ],
  equalityDeleteFile: "iceberg/warehouse/places/deletes/country-ca.eq.parquet",
  positionDeleteFile: "iceberg/warehouse/places/deletes/us-second.pos.parquet",
  snapshots: [1, 2],
} as const;

export const EXTERNAL_CONFORMANCE = {
  parquetTestingDir: "parquet-testing",
  icebergReferenceDir: "iceberg-reference",
} as const;

export const EXTERNAL_PLOTLY = {
  flights2015: "plotly/2015_flights.parquet",
  flights2015Rows: 5819079,
  flights2015Size: 25_238_218,
} as const;
