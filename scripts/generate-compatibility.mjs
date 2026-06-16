import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { inspect } from "node:util";

const root = resolve(new URL("..", import.meta.url).pathname);
const sourcePath = resolve(root, "docs/compatibility.json");
const targetPath = resolve(root, "docs/compatibility.md");

const rows = JSON.parse(readFileSync(sourcePath, "utf8"));
validateRows(rows);

const markdown = render(rows);
if (process.argv.includes("--check")) {
  const current = readFileSync(targetPath, "utf8");
  if (current !== markdown) {
    console.error("docs/compatibility.md is out of date. Run `pnpm docs:compatibility`.");
    process.exit(1);
  }
} else {
  writeFileSync(targetPath, markdown);
}

function validateRows(value) {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error("compatibility.json must be a non-empty array");
  }
  const validStatuses = new Set(["supported+tested", "supported", "detected+rejected", "planned"]);
  for (const [index, row] of value.entries()) {
    if (
      !row ||
      typeof row.area !== "string" ||
      typeof row.feature !== "string" ||
      typeof row.status !== "string" ||
      typeof row.notes !== "string"
    ) {
      throw new Error(`Invalid compatibility row at ${index}: ${inspect(row)}`);
    }
    if (!validStatuses.has(row.status)) {
      throw new Error(`Invalid compatibility status at ${index}: ${row.status}`);
    }
    validateEvidence(row, index);
  }
}

function validateEvidence(row, index) {
  const requiresEvidence = row.status === "supported+tested" || row.status === "detected+rejected";
  if (!requiresEvidence) return;
  if (!Array.isArray(row.evidence) || row.evidence.length === 0) {
    throw new Error(
      `Compatibility row at ${index} (${row.area}: ${row.feature}) requires evidence`,
    );
  }
  for (const [evidenceIndex, evidence] of row.evidence.entries()) {
    if (
      !evidence ||
      typeof evidence.file !== "string" ||
      typeof evidence.pattern !== "string" ||
      evidence.pattern.length === 0
    ) {
      throw new Error(
        `Invalid evidence at row ${index}, evidence ${evidenceIndex}: ${inspect(evidence)}`,
      );
    }
    const evidencePath = resolve(root, evidence.file);
    if (!existsSync(evidencePath)) {
      throw new Error(`Compatibility evidence file does not exist: ${evidence.file}`);
    }
    const content = readFileSync(evidencePath, "utf8");
    if (!content.includes(evidence.pattern)) {
      throw new Error(
        `Compatibility evidence pattern not found for ${row.area}: ${row.feature}: ${evidence.file} :: ${evidence.pattern}`,
      );
    }
  }
}

function render(rows) {
  return `${[
    "# Compatibility Matrix",
    "",
    "This file is generated from `docs/compatibility.json`. Run `pnpm docs:compatibility` after editing the source of truth.",
    "",
    "Legend: supported+tested = covered by tests; supported = implemented with narrower coverage; detected+rejected = fails with a typed `LaQLError`; planned = not yet a compatibility promise.",
    "",
    "| Area | Feature | Status | Notes |",
    "| --- | --- | --- | --- |",
    ...rows.map(
      (row) =>
        `| ${cell(row.area)} | ${cell(row.feature)} | ${cell(row.status)} | ${cell(row.notes)} |`,
    ),
  ].join("\n")}\n`;
}

function cell(value) {
  return String(value).replaceAll("|", "\\|").replaceAll("\n", " ");
}
