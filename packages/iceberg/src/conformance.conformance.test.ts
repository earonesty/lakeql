import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, join, relative } from "node:path";
import { memoryStore } from "@laql/core";
import { EXTERNAL_CONFORMANCE, externalFixturePath } from "@laql/fixtures";
import { describe, expect, it } from "vitest";
import { loadIcebergTable } from "./index.js";

const icebergReferenceDir = externalFixturePath(EXTERNAL_CONFORMANCE.icebergReferenceDir);
const allFiles = existsSync(icebergReferenceDir) ? listFiles(icebergReferenceDir) : [];
const jsonFiles = allFiles.filter((path) => path.endsWith(".json"));
const caseManifests = jsonFiles.filter((path) => basename(path) === "manifest.json");
const looseMetadataFiles = jsonFiles.filter(
  (path) => path.endsWith(".metadata.json") || basename(path) === "metadata.json",
);
const cases =
  caseManifests.length > 0
    ? caseManifests.map(readCaseManifest)
    : looseMetadataFiles.map((metadataFile) => ({
        name: relative(icebergReferenceDir, metadataFile),
        rootDir: icebergReferenceDir,
        metadataPath: relative(icebergReferenceDir, metadataFile),
        snapshots: [],
        files: [],
        expectedRecordCount: undefined,
      }));
const requiredExternalCases = [
  "v1-table",
  "v2-table",
  "v2-position-deletes",
  "v2-equality-deletes",
  "partition-evolution",
  "schema-evolution",
  "snapshot-history",
] as const;
const requiresExternalIceberg = process.env.LAQL_REQUIRE_EXTERNAL_ICEBERG === "1";
const describeExternal =
  process.env.LAQL_CONFORMANCE === "1" && cases.length > 0 ? describe : describe.skip;
const describeMissingRequired =
  process.env.LAQL_CONFORMANCE === "1" && requiresExternalIceberg && cases.length === 0
    ? describe
    : describe.skip;

describeMissingRequired("iceberg reference warehouse conformance fixtures", () => {
  it("requires vendored external Iceberg reference fixtures", () => {
    throw new Error(
      `No external Iceberg reference cases found under ${icebergReferenceDir}. Run pnpm fixtures:iceberg and commit fixtures/external/iceberg-reference plus refreshed checksums.`,
    );
  });
});

describeExternal("iceberg reference warehouse conformance", () => {
  if (requiresExternalIceberg) {
    it("includes the required Spark/PyIceberg reference case matrix", () => {
      const names = new Set(cases.map((fixtureCase) => fixtureCase.name));
      expect([...names].sort()).toEqual(expect.arrayContaining([...requiredExternalCases]));
      expect(caseManifests).toHaveLength(cases.length);
      for (const fixtureCase of cases) {
        expect(fixtureCase.expectedRecordCount).toEqual(expect.any(Number));
      }
    });

    it("includes snapshot history expectations for time-travel proof", () => {
      const history = cases.find((fixtureCase) => fixtureCase.name === "snapshot-history");
      expect(history?.snapshots.length).toBeGreaterThanOrEqual(3);
      for (const snapshot of history?.snapshots ?? []) {
        expect(snapshot.snapshotId ?? snapshot.asOfTimestampMs).toEqual(expect.any(Number));
        expect(snapshot.expectedRecordCount).toEqual(expect.any(Number));
        expect(snapshot.expectedFiles?.length).toBeGreaterThan(0);
      }
    });

    it("includes per-case file checksums in every manifest", () => {
      for (const fixtureCase of cases) {
        expect(fixtureCase.files.length).toBeGreaterThan(0);
        const checksummedPaths = new Set(fixtureCase.files.map((file) => file.path));
        const expectedFiles = listFiles(fixtureCase.rootDir)
          .filter((path) => basename(path) !== "manifest.json")
          .map((path) => relative(fixtureCase.rootDir, path).split("\\").join("/"))
          .sort();
        expect(fixtureCase.files.map((file) => file.path).sort()).toEqual(expectedFiles);
        for (const file of fixtureCase.files) {
          expect(sha256File(join(fixtureCase.rootDir, file.path))).toBe(file.sha256);
        }
        for (const snapshot of fixtureCase.snapshots) {
          for (const file of snapshot.expectedFiles ?? []) {
            expect(checksummedPaths.has(file), `${fixtureCase.name} expectedFiles ${file}`).toBe(
              true,
            );
          }
        }
      }
    });
  }

  it.each(cases)("loads and plans $name", async (fixtureCase) => {
    const store = memoryStore();
    for (const file of listFiles(fixtureCase.rootDir)) {
      await store.put(
        relative(fixtureCase.rootDir, file).split("\\").join("/"),
        readFileSync(file),
      );
    }
    for (const file of allFiles) {
      await store.put(relative(icebergReferenceDir, file), readFileSync(file));
    }

    const table = await loadIcebergTable({
      store,
      metadataPath: fixtureCase.metadataPath,
    });

    expect(table.planFiles().snapshotId).toBe(table.metadata["current-snapshot-id"]);
    for (const snapshot of fixtureCase.snapshots) {
      const plan = table.planFiles(snapshot.planOptions);
      if (snapshot.expectedRecordCount !== undefined) {
        expect(sumRecordCounts(plan.files)).toBe(snapshot.expectedRecordCount);
      }
      if (snapshot.expectedFiles !== undefined) {
        expect(plan.files.map((file) => file.path).sort()).toEqual(
          [...snapshot.expectedFiles].sort(),
        );
      }
    }
  });
});

interface ExternalIcebergSnapshotExpectation {
  snapshotId?: number;
  asOfTimestampMs?: number;
  ref?: string;
  expectedRecordCount?: number;
  expectedFiles?: string[];
  planOptions: {
    snapshotId?: number;
    asOfTimestampMs?: number;
    ref?: string;
    readMode: "ignore-deletes";
  };
}

interface ExternalIcebergCase {
  name: string;
  rootDir: string;
  metadataPath: string;
  snapshots: ExternalIcebergSnapshotExpectation[];
  files: ExternalIcebergFileChecksum[];
  expectedRecordCount?: number;
}

interface ExternalIcebergFileChecksum {
  path: string;
  sha256: string;
}

function readCaseManifest(manifestFile: string): ExternalIcebergCase {
  const manifest = JSON.parse(readFileSync(manifestFile, "utf8")) as Record<string, unknown>;
  const rootDir = dirname(manifestFile);
  const relativeRoot = relative(icebergReferenceDir, rootDir);
  const metadataPath =
    typeof manifest.metadataPath === "string" ? manifest.metadataPath : "metadata.json";
  const name =
    typeof manifest.case === "string"
      ? manifest.case
      : typeof manifest.name === "string"
        ? manifest.name
        : relativeRoot;
  return {
    name,
    rootDir,
    metadataPath: join(relativeRoot, metadataPath).split("\\").join("/"),
    snapshots: readSnapshotExpectations(manifest.snapshots),
    files: readFileChecksums(manifest.files),
    expectedRecordCount: optionalNumber(
      manifest.expectedRecordCount ?? manifest["expected-record-count"],
      "expectedRecordCount",
    ),
  };
}

function readSnapshotExpectations(value: unknown): ExternalIcebergSnapshotExpectation[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry, index) => {
    if (!entry || typeof entry !== "object") {
      throw new Error(`Invalid external Iceberg snapshot expectation at ${index}`);
    }
    const snapshot = entry as Record<string, unknown>;
    const snapshotId = optionalNumber(snapshot.snapshotId ?? snapshot["snapshot-id"], "snapshotId");
    const asOfTimestampMs = optionalNumber(
      snapshot.asOfTimestampMs ?? snapshot["as-of-timestamp-ms"],
      "asOfTimestampMs",
    );
    const ref = optionalString(snapshot.ref, "ref");
    const expectedRecordCount = optionalNumber(
      snapshot.expectedRecordCount ?? snapshot["expected-record-count"],
      "expectedRecordCount",
    );
    const expectedFiles = optionalStringArray(
      snapshot.expectedFiles ?? snapshot["expected-files"],
      "expectedFiles",
    );
    return {
      ...(snapshotId !== undefined ? { snapshotId } : {}),
      ...(asOfTimestampMs !== undefined ? { asOfTimestampMs } : {}),
      ...(ref !== undefined ? { ref } : {}),
      ...(expectedRecordCount !== undefined ? { expectedRecordCount } : {}),
      ...(expectedFiles !== undefined ? { expectedFiles } : {}),
      planOptions: {
        ...(snapshotId !== undefined ? { snapshotId } : {}),
        ...(asOfTimestampMs !== undefined ? { asOfTimestampMs } : {}),
        ...(ref !== undefined ? { ref } : {}),
        readMode: "ignore-deletes",
      },
    };
  });
}

function optionalNumber(value: unknown, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`External Iceberg manifest ${name} must be a finite number`);
  }
  return value;
}

function optionalString(value: unknown, name: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") {
    throw new Error(`External Iceberg manifest ${name} must be a string`);
  }
  return value;
}

function optionalStringArray(value: unknown, name: string): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
    throw new Error(`External Iceberg manifest ${name} must be a string array`);
  }
  return value as string[];
}

function readFileChecksums(value: unknown): ExternalIcebergFileChecksum[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) {
    throw new Error("External Iceberg manifest files must be an array");
  }
  return value.map((entry, index) => {
    if (!entry || typeof entry !== "object") {
      throw new Error(`Invalid external Iceberg file checksum at ${index}`);
    }
    const record = entry as Record<string, unknown>;
    const path = requiredRelativePath(record.path, `files[${index}].path`);
    if (typeof record.sha256 !== "string" || !/^[a-f0-9]{64}$/u.test(record.sha256)) {
      throw new Error(
        `External Iceberg manifest files[${index}].sha256 must be a SHA-256 hex digest`,
      );
    }
    return { path, sha256: record.sha256 };
  });
}

function requiredRelativePath(value: unknown, name: string): string {
  if (typeof value !== "string") {
    throw new Error(`External Iceberg manifest ${name} must be a string`);
  }
  const normalized = value.replaceAll("\\", "/").replace(/^\.?\//u, "");
  if (normalized === "" || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new Error(`External Iceberg manifest ${name} must be a relative path`);
  }
  return normalized;
}

function sumRecordCounts(files: { recordCount: number }[]): number {
  return files.reduce((total, file) => total + file.recordCount, 0);
}

function listFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir).sort()) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...listFiles(path));
    else out.push(path);
  }
  return out;
}

function sha256File(path: string): string {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}
