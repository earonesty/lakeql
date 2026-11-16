import {
  type BBox,
  type Expr,
  ensureGeoBackendForExprs,
  envelopeOf,
  evaluate,
  type GeoJsonGeometry,
  LakeqlError,
  parseGeometry,
  toGeometry,
} from "lakeql";
import { inspectLanceFile, withInspectedNumericRange } from "./file.js";
import type { LanceReadContext } from "./io.js";
import type { LanceIndexMetadata, LanceManifest } from "./proto.js";
import { roaringContains } from "./roaring.js";
import { loadLanceIndexMetadata } from "./scalar.js";

export interface LanceSpatialIndexInfo {
  name: string;
  uuid: string;
  column: string;
  indexVersion: number;
}

export interface LanceSpatialLimits {
  /** Maximum tree pages visited, independent of the number of matching rows. */
  maxPagesVisited: number;
  /** Maximum leaf entries examined, including false positive envelopes. */
  maxCandidatesExamined: number;
}

export interface LanceSpatialQuery {
  geometry: GeoJsonGeometry;
  predicate: "intersects" | "within" | "contains";
}

type Source = {
  context: LanceReadContext;
  root: string;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  indexMetadata?: readonly LanceIndexMetadata[];
};

export async function loadSpatialIndexes(options: Source): Promise<LanceSpatialIndexInfo[]> {
  const metadata = options.indexMetadata ?? (await loadLanceIndexMetadata(options));
  return metadata.filter(isRTree).map((index) => info(options.manifest, index));
}

/**
 * R-tree traversal is envelope pruning. Exact predicates on native XY points
 * are evaluated against the indexed point (its envelope contains that exact point).
 * Other geometry shapes require a geometry decoder before they can be refined.
 */
export async function searchSpatialIndex(
  options: Source & {
    indexName: string;
    query: LanceSpatialQuery;
    limits?: Partial<LanceSpatialLimits>;
  },
): Promise<{
  index: LanceSpatialIndexInfo;
  rowIds: bigint[];
  pagesVisited: number;
  candidatesExamined: number;
}> {
  const limits = {
    maxPagesVisited: options.limits?.maxPagesVisited ?? 1024,
    maxCandidatesExamined: options.limits?.maxCandidatesExamined ?? 100_000,
  };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value <= 0) validation(`Invalid spatial limit ${name}`);
  }
  if (!["intersects", "within", "contains"].includes(options.query.predicate)) {
    validation("Unknown Lance spatial predicate");
  }
  // Use the core geometry validator before loading any index data.
  const geometry = toGeometry(
    parseGeometry(JSON.stringify(options.query.geometry), "spatialRows"),
    "spatialRows",
  );
  const literal = JSON.stringify(geometry);
  const bounds = envelopeOf(geometry);
  const metadata = options.indexMetadata ?? (await loadLanceIndexMetadata(options));
  const selected = metadata.find((index) => index.name === options.indexName && isRTree(index));
  if (selected === undefined) validation(`Unknown Lance R-tree index ${options.indexName}`);
  const index = info(options.manifest, selected);
  if (index.indexVersion !== 0)
    unsupported("Unsupported Lance R-tree row-ID domain", { version: index.indexVersion });
  if (selected.fragmentBitmap === undefined)
    unsupported("R-tree has no fragment coverage metadata");
  const covers = roaringContains(selected.fragmentBitmap);
  if (options.manifest.fragments.some((fragment) => !covers(Number(fragment.id)))) {
    unsupported(
      "R-tree does not cover all active fragments; rebuild the index after appending or rewriting data",
    );
  }
  const field = options.manifest.fields.find((field) => field.id === selected.fields[0]);
  const extension = new TextDecoder().decode(field?.metadata?.["ARROW:extension:name"]);
  const children = options.manifest.fields.filter((child) => child.parentId === field?.id);
  if (
    extension !== "geoarrow.point" ||
    field?.logicalType !== "struct" ||
    children.length !== 2 ||
    children[0]?.name !== "x" ||
    children[1]?.name !== "y" ||
    children.some((child) => child.logicalType !== "double")
  ) {
    unsupported("Exact Lance spatial predicates require a native GeoArrow XY point column", {
      column: index.column,
      extension,
    });
  }
  const expr: Expr = {
    kind: "call",
    fn: `st_${options.query.predicate}`,
    args: [
      { kind: "column", name: "point" },
      { kind: "literal", value: literal },
    ],
  };
  await ensureGeoBackendForExprs([expr]);
  const fileMetadata = selected.files.find((file) => file.path === "page_data.lance");
  if (fileMetadata === undefined) corrupt("R-tree page_data.lance is missing");
  const file = await inspectLanceFile(
    options.context,
    `${options.root}/_indices/${index.uuid}/page_data.lance`,
    fileMetadata.sizeBytes,
  );
  const schema = [
    ["bbox", "struct"],
    ["xmin", "double"],
    ["ymin", "double"],
    ["xmax", "double"],
    ["ymax", "double"],
    ["id", "uint64"],
  ];
  if (
    file.fields.length !== schema.length ||
    file.columns.length !== schema.length ||
    schema.some(
      ([name, type], i) => file.fields[i]?.name !== name || file.fields[i]?.logicalType !== type,
    )
  ) {
    unsupported("Unsupported Lance R-tree page schema");
  }
  if (
    file.fields.slice(1, 5).some((child) => child.parentId !== file.fields[0]?.id) ||
    file.fields[0]?.parentId !== -1 ||
    file.fields[5]?.parentId !== -1 ||
    file.columns[0]?.pages.some((page) => page.encoding.kind !== "struct")
  ) {
    corrupt("Invalid R-tree bounding-box structure");
  }
  const numberMetadata = (key: string): number => {
    const value = new TextDecoder().decode(file.schemaMetadata[key]);
    if (!/^(0|[1-9][0-9]*)$/u.test(value)) corrupt(`Invalid R-tree ${key}`);
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed)) corrupt(`Invalid R-tree ${key}`);
    return parsed;
  };
  const size = numberMetadata("page_size");
  const items = numberMetadata("num_items");
  const pages = numberMetadata("num_pages");
  if (size < 2) corrupt("Invalid R-tree page size");
  // O(log N) level descriptors; never allocate a table with one entry per page.
  const levels: { start: number; firstPage: number; items: number; pages: number }[] = [];
  let levelItems = items;
  let start = 0;
  let firstPage = 0;
  while (levelItems > 0) {
    const count = Math.ceil(levelItems / size);
    levels.push({ start, firstPage, items: levelItems, pages: count });
    start += levelItems;
    firstPage += count;
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(firstPage))
      corrupt("R-tree layout overflow");
    if (count === 1) break;
    levelItems = count;
  }
  if (firstPage !== pages || start !== file.rowCount)
    corrupt("R-tree page counts disagree with file rows");
  const rowIds: bigint[] = [];
  const maxRows = options.context.budget.maxOutputRows ?? Infinity;
  let pagesVisited = 0;
  let candidatesExamined = 0;
  const selections = file.fields.flatMap((field, columnIndex) =>
    columnIndex === 0 ? [] : [{ field, columnIndex }],
  );
  const visit = async (levelIndex: number, page: number): Promise<void> => {
    options.context.check();
    if (++pagesVisited > limits.maxPagesVisited)
      exceeded("spatial pages visited", limits.maxPagesVisited);
    const level = levels[levelIndex];
    if (level === undefined || page < level.firstPage || page >= level.firstPage + level.pages)
      corrupt("R-tree child page is outside its level");
    const local = (page - level.firstPage) * size;
    const length = Math.min(size, level.items - local);
    const next: number[] = [];
    const matches: bigint[] = [];
    // Admission covers retained child IDs and per-entry transient refinement
    // state. Encoded columns are leased directly, with no row-object expansion.
    const stackLease = options.context.leaseDecodedMemory(length * 64);
    try {
      await withInspectedNumericRange(
        {
          context: options.context,
          file,
          selections,
          rowStart: level.start + local,
          rowCount: length,
        },
        (value) => {
          for (let entry = 0; entry < length; entry++) {
            options.context.check();
            const row = level.start + local + entry;
            const bbox: BBox = {
              minx: Number(value(0, row)),
              miny: Number(value(1, row)),
              maxx: Number(value(2, row)),
              maxy: Number(value(3, row)),
            };
            const id = value(4, row);
            if (
              Object.values(bbox).some((v) => !Number.isFinite(v)) ||
              bbox.minx > bbox.maxx ||
              bbox.miny > bbox.maxy
            )
              corrupt("Invalid R-tree entry");
            if (levelIndex === 0 && ++candidatesExamined > limits.maxCandidatesExamined)
              exceeded("spatial candidates examined", limits.maxCandidatesExamined);
            if (levelIndex > 0) {
              const child = Number(id);
              const below = levels[levelIndex - 1];
              if (
                !Number.isSafeInteger(child) ||
                child !== (below?.firstPage ?? 0) + local + entry
              ) {
                corrupt("R-tree branch does not reference its packed child");
              }
              if (overlaps(bbox, bounds)) next.push(child);
            } else {
              if (bbox.minx !== bbox.maxx || bbox.miny !== bbox.maxy)
                corrupt("Point R-tree contains a non-point envelope");
              if (!overlaps(bbox, bounds)) continue;
              const point = JSON.stringify({ type: "Point", coordinates: [bbox.minx, bbox.miny] });
              if (evaluate(expr, { point }) === true) {
                if (rowIds.length + matches.length >= maxRows) exceeded("output rows", maxRows);
                matches.push(BigInt(id));
              }
            }
          }
        },
      );
      options.context.accountDecodedMemory(matches.length * 64);
      for (const id of matches) rowIds.push(id);
      for (const child of next) await visit(levelIndex - 1, child);
    } finally {
      stackLease.release();
    }
  };
  if (levels.length > 0) await visit(levels.length - 1, pages - 1);
  return { index, rowIds, pagesVisited, candidatesExamined };
}

function overlaps(a: BBox, b: BBox): boolean {
  return a.minx <= b.maxx && a.maxx >= b.minx && a.miny <= b.maxy && a.maxy >= b.miny;
}
function isRTree(index: LanceIndexMetadata): boolean {
  return index.detailsTypeUrl.toLowerCase() === "/lance.index.pb.rtreeindexdetails";
}
function info(manifest: LanceManifest, metadata: LanceIndexMetadata): LanceSpatialIndexInfo {
  const field = manifest.fields.find((field) => field.id === metadata.fields[0]);
  if (metadata.fields.length !== 1 || field === undefined)
    corrupt("R-tree must reference one known field");
  return {
    name: metadata.name,
    uuid: metadata.uuid,
    column: field.name,
    indexVersion: metadata.indexVersion ?? 0,
  };
}
function validation(message: string): never {
  throw new LakeqlError("LAKEQL_VALIDATION_ERROR", message);
}
function corrupt(message: string): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message);
}
function unsupported(message: string, details = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}
function exceeded(metric: string, limit: number): never {
  throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", `Lance read exceeded ${metric} budget`, {
    metric,
    limit,
  });
}
