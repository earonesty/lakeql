import assert from "node:assert/strict";
import { test } from "node:test";
import { summarizeHeapProfile } from "./heap-profile-summary.mjs";

const frame = (functionName, lineNumber) => ({
  functionName,
  scriptId: "3",
  url: "script-0",
  lineNumber,
  columnNumber: 0,
});

test("aggregates direct sites, preserves callers and accounts for omitted samples", () => {
  const parent = frame("parent", 1),
    child = frame("child", 3);
  const result = summarizeHeapProfile(
    {
      head: {
        callFrame: parent,
        selfSize: 10,
        children: [
          { callFrame: child, selfSize: 20, children: [] },
          { callFrame: child, selfSize: 5, children: [] },
        ],
      },
    },
    "// packages/core/dist/a.js\nfunction parent() {}\n// ordinary comment\nfunction child() {}",
    {
      limit: 1,
      includeCollected: true,
    },
  );
  assert.equal(result.estimatedBytes, 35);
  assert.equal(result.totalSites, 2);
  assert.equal(result.sites[0].estimatedBytes, 25);
  assert.equal(result.sites[0].bundleSource, "packages/core/dist/a.js");
  assert.equal(result.sites[0].caller.functionName, "parent");
  assert.equal(result.omittedEstimatedBytes, 10);
  assert.equal(result.includesCollectedObjects, true);
});

test("handles empty samples without inventing allocation sites", () => {
  const result = summarizeHeapProfile(
    { head: { callFrame: frame("root", 0), selfSize: 0, children: [] } },
    "",
  );
  assert.equal(result.estimatedBytes, 0);
  assert.deepEqual(result.sites, []);
  assert.equal(result.omittedEstimatedBytes, 0);
  assert.equal(result.includesCollectedObjects, false);
});
