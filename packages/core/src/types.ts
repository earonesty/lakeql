export type Row = Record<string, unknown>;

/**
 * A serializable position in a running query. Bookmarks are plain values:
 * the engine only produces and consumes them, and the caller moves them
 * over whatever transport it likes (queue, KV, URL, cron state).
 */
export interface Bookmark {
  version: 1;
  planFingerprint: string;
  snapshot: string;
  position: {
    fileIndex: number;
    rowGroup: number;
    rowOffset: number;
    taskId?: string;
    outputManifestCursor?: number;
  };
  operatorState?: {
    limitEmitted?: number;
    groupBy?: Uint8Array | { spillRef: string };
    topK?: Uint8Array;
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

  manifestsRead: number;
  manifestsSkipped: number;

  filesPlanned: number;
  filesRead: number;
  filesSkipped: number;

  rowGroupsRead: number;
  rowGroupsSkipped: number;

  columnsRead: string[];

  bytesRequested: number;
  rangeRequests: number;

  rowsDecoded: number;
  rowsMatched: number;
  rowsReturned: number;

  cacheHits: number;
  cacheMisses: number;
}
