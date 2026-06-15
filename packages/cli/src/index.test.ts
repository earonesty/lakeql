import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { memoryStore, readOutputManifest } from "@laql/core";
import { fixturePath, SALES } from "@laql/fixtures";
import { createParquetLake } from "@laql/parquet";
import { describe, expect, it } from "vitest";
import { COMMANDS, runCli, usage } from "./index.js";

describe("usage", () => {
  it("lists every command", () => {
    const text = usage();
    for (const cmd of COMMANDS) {
      expect(text).toContain(cmd);
    }
  });

  it("matches the CLI help snapshot", () => {
    expect(usage()).toMatchInlineSnapshot(`
      "usage: laql <command> [options]

      commands:
        compact --path <file.parquet> --output <prefix> [--max-rows-per-file n]
        query   --path <file.parquet> --sql <query> [--format csv|json|ndjson]
        explain --path <file.parquet> --sql <query>
        inspect --path <file.parquet>
        write   --path <file.parquet> --sql <query> --output <prefix> [--partition-by a,b] [--max-rows-per-file n] [--manifest <path>] [--job-id id]
        schema  --path <file.parquet>"
    `);
  });
});

describe("runCli", () => {
  it("queries a local Parquet path as NDJSON", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount where region = 'west' order by amount asc limit 2",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(result.stdout).toBe(
      '{"store_id":"store-000","amount":0}\n{"store_id":"store-000","amount":36.28}\n',
    );
  });

  it("queries a local Parquet path as JSON", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "from input select region where amount > 900 limit 1",
      "--format",
      "json",
    ]);

    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual([{ region: "east" }]);
  });

  it("accepts select-first SQL with FROM while still using the --path source", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount from ignored_source where region = 'west' order by amount asc limit 1",
      "--format",
      "json",
    ]);

    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-000", amount: 0 }]);
  });

  it("queries a local Parquet path as CSV", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount where region = 'west' order by amount asc limit 2",
      "--format",
      "csv",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(result.stdout).toBe("store_id,amount\nstore-000,0\nstore-000,36.28\n");
  });

  it("explains, inspects, and reads schema for a local Parquet path", async () => {
    const explain = await runCli([
      "explain",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id where amount > 900 limit 1",
    ]);
    const inspect = await runCli(["inspect", "--path", fixturePath(SALES.file)]);
    const schema = await runCli(["schema", "--path", fixturePath(SALES.file)]);

    expect(explain.stdout).toContain("files planned: 1");
    expect(JSON.parse(inspect.stdout)).toMatchObject({
      rows: SALES.rows,
      rowGroups: 3,
      columns: 4,
    });
    expect(schema.exitCode).toBe(0);
    expect(JSON.parse(schema.stdout)).toMatchObject({
      rows: SALES.rows,
      columns: expect.arrayContaining([expect.objectContaining({ name: "amount" })]),
    });
  });

  it("matches CLI explain, inspect, and schema snapshots", async () => {
    const explain = await runCli([
      "explain",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id where amount > 900 limit 1",
    ]);
    const inspect = JSON.parse(
      (await runCli(["inspect", "--path", fixturePath(SALES.file)])).stdout,
    ) as Record<string, unknown>;
    const schema = JSON.parse(
      (await runCli(["schema", "--path", fixturePath(SALES.file)])).stdout,
    ) as Record<string, unknown>;
    inspect.path = "<fixture:sales.parquet>";
    schema.path = "<fixture:sales.parquet>";

    expect(explain.stdout).toMatchInlineSnapshot(`
      "files planned: 1
      files skipped: 0
      projected columns: amount, store_id
      "
    `);
    expect(inspect).toMatchInlineSnapshot(`
      {
        "columns": 4,
        "path": "<fixture:sales.parquet>",
        "rowGroups": 3,
        "rows": 100,
      }
    `);
    expect(schema).toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "name": "store_id",
            "type": "BYTE_ARRAY",
          },
          {
            "name": "date",
            "type": "BYTE_ARRAY",
          },
          {
            "name": "amount",
            "type": "DOUBLE",
          },
          {
            "name": "region",
            "type": "BYTE_ARRAY",
          },
        ],
        "path": "<fixture:sales.parquet>",
        "rows": 100,
      }
    `);
  });

  it("writes query results to local Parquet files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "laql-cli-"));
    const output = join(dir, "west");
    const result = await runCli([
      "write",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, region, amount where region = 'west' order by amount asc limit 2",
      "--output",
      output,
      "--max-rows-per-file",
      "1",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const body = JSON.parse(result.stdout) as { files: { path: string; rowCount: number }[] };
    expect(body.files.map((file) => file.rowCount)).toEqual([1, 1]);

    const store = memoryStore();
    for (const file of body.files) await store.put(file.path, await readFile(file.path));
    await expect(
      createParquetLake({ store }).path(`${output}/*.parquet`).toArray(),
    ).resolves.toEqual([
      { amount: 0, region: "west", store_id: "store-000" },
      { amount: 36.28, region: "west", store_id: "store-000" },
    ]);
  });

  it("writes output manifests for local write commands", async () => {
    const dir = await mkdtemp(join(tmpdir(), "laql-cli-manifest-"));
    const output = join(dir, "west");
    const manifestPath = join(dir, "manifest.json");
    const result = await runCli([
      "write",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, region, amount where region = 'west' order by amount asc limit 2",
      "--output",
      output,
      "--max-rows-per-file",
      "1",
      "--manifest",
      manifestPath,
      "--job-id",
      "job_cli_write",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const body = JSON.parse(result.stdout) as {
      files: { path: string; rowCount: number }[];
      manifest: string;
    };
    expect(body.manifest).toBe(manifestPath);

    const store = memoryStore();
    await store.put(manifestPath, await readFile(manifestPath));
    const manifest = await readOutputManifest(store, manifestPath);
    expect(manifest).toMatchObject({
      jobId: "job_cli_write",
      entries: [
        {
          taskId: "job_cli_write-task-000000",
          outputPath: body.files[0]?.path,
          rowCount: 1,
        },
        {
          taskId: "job_cli_write-task-000000",
          outputPath: body.files[1]?.path,
          rowCount: 1,
        },
      ],
    });
    expect(manifest.planFingerprint).toMatch(/^fp_[0-9a-f]{16}$/u);
  });

  it("compacts a local Parquet file into rewritten output files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "laql-cli-compact-"));
    const output = join(dir, "sales");
    const result = await runCli([
      "compact",
      "--path",
      fixturePath(SALES.file),
      "--output",
      output,
      "--max-rows-per-file",
      "75",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const body = JSON.parse(result.stdout) as { files: { path: string; rowCount: number }[] };
    expect(body.files.map((file) => file.rowCount)).toEqual([75, 25]);

    const store = memoryStore();
    for (const file of body.files) await store.put(file.path, await readFile(file.path));
    await expect(createParquetLake({ store }).path(`${output}/*.parquet`).count()).resolves.toBe(
      SALES.rows,
    );
  });

  it("returns typed failures for unsupported commands and bad arguments", async () => {
    await expect(runCli([])).resolves.toMatchObject({
      exitCode: 0,
      stdout: expect.stringContaining("usage:"),
    });
    await expect(runCli(["--help"])).resolves.toMatchObject({
      exitCode: 0,
      stdout: expect.stringContaining("usage:"),
    });
    await expect(runCli(["nope"])).resolves.toMatchObject({ exitCode: 2 });
    await expect(
      runCli(["write", "--path", fixturePath(SALES.file), "--sql", "select id"]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--output"),
    });
    await expect(runCli(["query", "--path"])).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("LAQL_PARSE_ERROR"),
    });
    await expect(runCli(["query", "--nope"])).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("Unknown argument"),
    });
    await expect(
      runCli(["query", "--path", "/definitely/missing.parquet", "--sql", "select id"]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("ENOENT"),
    });
    await expect(
      runCli(["query", "--path", fixturePath(SALES.file), "--format", "xml"]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--format"),
    });
  });
});
