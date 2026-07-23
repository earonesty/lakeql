import { httpStore } from "../packages/lakeql/src/node.js";
import { openLanceDataset } from "../packages/lance/src/index.js";

export interface LanceHttpExampleOptions {
  baseUrl: string;
  path: string;
  snapshotId: string;
  rowIds: readonly bigint[];
}

export async function materializeLanceRows(options: LanceHttpExampleOptions) {
  const dataset = await openLanceDataset({
    store: httpStore({ baseUrl: options.baseUrl }),
    path: options.path,
    budget: {
      maxBytes: 8 * 1024 * 1024,
      maxRangeRequests: 128,
      maxMemoryBytes: 16 * 1024 * 1024,
      maxOutputRows: 32,
      maxConcurrentReads: 4,
      maxElapsedMs: 3_000,
    },
  });

  return await dataset.takeRows({
    snapshotId: options.snapshotId,
    rowIds: options.rowIds,
    select: ["title", "owner_name", "source_url"],
  });
}
