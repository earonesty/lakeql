import { memoryStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { WORKERD_FIXTURE_BASE64 } from "./fixture.generated.js";
import { openLanceDataset } from "./index.js";

const DATASET_PATH = "fixtures/take-v2.0.lance";

describe("lakeql-lance workerd runtime", () => {
  it("materializes scattered projected rows through the real Workers runtime", async () => {
    expect((globalThis as Record<string, unknown>).WebSocketPair).toBeTypeOf("function");
    const store = memoryStore();
    for (const [path, encoded] of Object.entries(WORKERD_FIXTURE_BASE64)) {
      await store.put(path, decodeBase64(encoded));
    }

    const dataset = await openLanceDataset({
      store,
      path: DATASET_PATH,
      budget: {
        maxBytes: 32_000,
        maxRangeRequests: 64,
        maxMemoryBytes: 32_000,
        maxOutputRows: 32,
        maxConcurrentReads: 2,
        maxElapsedMs: 3_000,
      },
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [31n, 0n, 47n, 9n],
      select: ["serial", "mark_text", "active"],
    });

    expect(result.rows).toEqual([
      { serial: 10_000_031, mark_text: "MARK 031", active: false },
      { serial: 10_000_000, mark_text: "MARK 000", active: true },
      { serial: 10_000_047, mark_text: "MARK 047", active: false },
      { serial: 10_000_009, mark_text: null, active: false },
    ]);
    expect(result.stats).toMatchObject({
      fragmentsTouched: 3,
      rowsRequested: 4,
      rowsMaterialized: 4,
      selectedColumns: ["serial", "mark_text", "active"],
    });
    expect(result.stats.physicalBytesRequested).toBeLessThan(16_000);
  });
});

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}
