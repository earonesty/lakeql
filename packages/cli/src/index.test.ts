import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { memoryStore } from "@laql/core";
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
      runCli(["query", "--path", fixturePath(SALES.file), "--format", "csv"]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--format"),
    });
  });
});
