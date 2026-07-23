import type { Expr } from "./expr.js";
import type { WindowExpr } from "./window.js";

export type Row = Record<string, unknown>;

export interface BookmarkQuery {
  source: string;
  select?: string[];
  projections?: Record<string, Expr>;
  where?: Expr;
  distinct?: boolean;
  windows?: Record<string, WindowExpr>;
  qualify?: Expr;
  orderBy?: {
    column: string;
    direction?: "asc" | "desc";
    nulls?: "first" | "last";
  }[];
  limit?: number;
  offset?: number;
  batchSize?: number;
  hive?: boolean;
}

/**
 * A serializable position in a running query. Bookmarks are plain values:
 * the engine only produces and consumes them, and the caller moves them
 * over whatever transport it likes (queue, KV, URL, cron state).
 */
export interface Bookmark {
  version: 1;
  planFingerprint: string;
  snapshot: string;
  query?: BookmarkQuery;
  position: {
    fileIndex: number;
    rowGroup: number;
    rowOffset: number;
    taskId?: string;
    outputManifestCursor?: number;
  };
  writeState?: {
    taskState?: "planned" | "running" | "output-written" | "manifest-recorded" | "complete";
    idempotencyKey?: string;
    multipart?: {
      uploadId: string;
      path: string;
      parts: {
        partNumber: number;
        etag: string;
        byteSize: number;
      }[];
    };
  };
  operatorState?: {
    limitEmitted?: number;
    groupBy?: Uint8Array | { spillRef: string };
    topK?: Uint8Array | { spillRef: string };
    sort?: Uint8Array | { spillRef: string };
    sketches?: Record<string, Uint8Array>;
  };
}

export interface SliceResult {
  rows: Row[];
  /** Absent when the query completed. */
  bookmark?: Bookmark;
}

export interface QueryStats {
  queryId: string;
  elapsedMs: number;
  planningMs?: number;
  footerFetchMs?: number;
  objectStoreWaitMs?: number;
  decodeMs?: number;

  manifestsRead: number;
  manifestsSkipped: number;

  filesPlanned: number;
  filesRead: number;
  filesSkipped: number;

  rowGroupsRead: number;
  rowGroupsSkipped: number;
  rowGroupsPlanned?: number;
  rowGroupsTotal?: number;

  columnsRead: string[];

  bytesRequested: number;
  /** Bytes transferred by ranged ObjectStore reads, excluding logical file-size charges. */
  physicalBytesRequested?: number;
  rangeRequests: number;

  rowsDecoded: number;
  rowsMatched: number;
  rowsReturned: number;

  cacheHits: number;
  cacheMisses: number;

  physicalFragments?: number;
  acceleratorFragments?: number;
  acceleratorUploadBytes?: number;
  acceleratorReadbackBytes?: number;
  acceleratorDispatches?: number;
  physicalReplays?: number;
}
