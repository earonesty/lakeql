import { memoryStore } from "@laql/core";
import { describe, expect, it } from "vitest";
import { icebergRestCatalog } from "./index.js";

const catalogUrl = process.env.LAQL_REST_CATALOG_URL;
const namespacePrefix = process.env.LAQL_REST_CATALOG_NAMESPACE ?? "laql_provider";
const requireProviders = process.env.LAQL_REQUIRE_PROVIDERS === "1";

const describeRestCatalog = catalogUrl === undefined ? describe.skip : describe;
const describeMissingRestCatalog =
  catalogUrl === undefined && requireProviders ? describe : describe.skip;

describeMissingRestCatalog("Iceberg REST catalog provider conformance", () => {
  it("requires Iceberg REST catalog provider environment", () => {
    throw new Error("LAQL_REST_CATALOG_URL is required when LAQL_REQUIRE_PROVIDERS=1");
  });
});

describeRestCatalog("Iceberg REST catalog provider conformance", () => {
  it("creates, lists, and loads a table through a reference REST catalog", async () => {
    const namespace = `${namespacePrefix}_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const tableName = "places";
    await createNamespace(namespace);
    const createResponse = await createTable(namespace, tableName);
    expect(createResponse.metadata).toBeTruthy();

    const catalog = icebergRestCatalog({
      url: catalogUrl as string,
      namespace,
      table: tableName,
    });
    await expect(catalog.listTables()).resolves.toContainEqual({
      namespace: [namespace],
      name: tableName,
    });

    const table = await catalog.loadTable(memoryStore());
    expect(table.metadata["format-version"]).toBe(2);
    expect(table.metadata.schemas[0]?.fields.map((field) => field.name)).toContain("id");
  });
});

async function createNamespace(namespace: string): Promise<void> {
  const response = await fetch(restUrl(["v1", "namespaces"]), {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ namespace: [namespace], properties: {} }),
  });
  if (response.ok || response.status === 409) return;
  throw new Error(
    `failed to create REST catalog namespace: ${response.status} ${await response.text()}`,
  );
}

async function createTable(namespace: string, tableName: string): Promise<{ metadata?: unknown }> {
  const response = await fetch(restUrl(["v1", "namespaces", namespace, "tables"]), {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({
      name: tableName,
      schema: {
        type: "struct",
        "schema-id": 0,
        fields: [{ id: 1, name: "id", required: true, type: "int" }],
      },
      "partition-spec": { "spec-id": 0, fields: [] },
      "write-order": { "order-id": 0, fields: [] },
      properties: { "format-version": "2" },
      "stage-create": false,
    }),
  });
  if (!response.ok && response.status !== 409) {
    throw new Error(
      `failed to create REST catalog table: ${response.status} ${await response.text()}`,
    );
  }
  if (response.status === 409) {
    return (await fetchJson(restUrl(["v1", "namespaces", namespace, "tables", tableName]))) as {
      metadata?: unknown;
    };
  }
  return (await response.json()) as { metadata?: unknown };
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url, { headers: { accept: "application/json" } });
  if (!response.ok) {
    throw new Error(`REST catalog request failed: ${response.status} ${await response.text()}`);
  }
  return await response.json();
}

function restUrl(segments: string[]): string {
  const url = new URL(ensureSlash(catalogUrl as string));
  url.pathname = `${url.pathname.replace(/\/$/u, "")}/${segments.map(encodeURIComponent).join("/")}`;
  return String(url);
}

function ensureSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}
