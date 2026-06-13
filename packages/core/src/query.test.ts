import { describe, expect, it } from "vitest";
import { LaQLError } from "./errors.js";
import { and, between, col, eq, fn, gt, isIn, isNull, like, lit, not, or } from "./expr.js";
import { memoryStore } from "./memory-store.js";
import {
  Lake,
  parseHivePartitions,
  parseJsonQuery,
  type ScanAdapter,
  type ScanOptions,
} from "./query.js";
import type { Row } from "./types.js";

class FakeScanner implements ScanAdapter {
  readonly requestedColumns: (string[] | undefined)[] = [];

  constructor(private readonly rowsByPath: Record<string, Row[]>) {}

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    this.requestedColumns.push(options.columns);
    options.stats.rangeRequests += 1;
    const rows = this.rowsByPath[path] ?? [];
    for (let offset = 0; offset < rows.length; offset += options.batchSize) {
      yield rows.slice(offset, offset + options.batchSize).map((row) => {
        if (!options.columns) return row;
        const out: Row = {};
        for (const column of options.columns) {
          if (column in row) out[column] = row[column];
        }
        return out;
      });
    }
  }
}

async function makeLake(config: {
  rowsByPath: Record<string, Row[]>;
  budget?: ConstructorParameters<typeof Lake>[0]["budget"];
  now?: () => number;
}) {
  const store = memoryStore();
  for (const path of Object.keys(config.rowsByPath)) {
    await store.put(path, new Uint8Array([1, 2, 3]));
  }
  const scanner = new FakeScanner(config.rowsByPath);
  const lake = new Lake({
    store,
    scanner,
    budget: config.budget,
    now: config.now,
    queryId: () => "q_test",
  });
  return { lake, scanner };
}

describe("Lake query runtime", () => {
  it("expands globs, filters, offsets, limits, projects, and records stats", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "data/b.parquet": [{ id: 3, region: "west" }],
        "data/a.parquet": [
          { id: 1, region: "west" },
          { id: 2, region: "east" },
        ],
      },
    });

    const result = lake
      .path("data/*.parquet")
      .select(["id"])
      .where(eq("region", "west"))
      .offset(1)
      .limit(1)
      .batchSize(1)
      .run();

    expect(await result.toArray()).toEqual([{ id: 3 }]);
    expect(result.stats).toMatchObject({
      queryId: "q_test",
      filesPlanned: 2,
      filesRead: 2,
      rowsDecoded: 3,
      rowsMatched: 2,
      rowsReturned: 1,
      rangeRequests: 2,
    });
    expect(scanner.requestedColumns[0]).toEqual(["id", "region"]);
  });

  it("supports first, count, batches, NDJSON, and JSON streams", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        "data/types.parquet": [
          { id: 1, big: 12n },
          { id: 2, big: 9007199254740993n },
        ],
      },
    });

    expect(await lake.path("data/types.parquet").first()).toEqual({ id: 1, big: 12n });
    expect(await lake.path("data/types.parquet").count()).toBe(2);
    expect(await lake.path("missing*.parquet").first()).toBeUndefined();

    const batches: Row[][] = [];
    for await (const batch of lake.path("data/types.parquet").batchSize(1).batches()) {
      batches.push(batch);
    }
    expect(batches).toHaveLength(2);

    await expect(
      new Response(lake.path("data/types.parquet").limit(2).streamNdjson()).text(),
    ).resolves.toBe('{"id":1,"big":12}\n{"id":2,"big":"9007199254740993"}\n');

    await expect(
      new Response(lake.path("data/types.parquet").limit(1).streamJson()).text(),
    ).resolves.toBe('[{"id":1,"big":12}]');

    await lake.path("data/types.parquet").streamNdjson().cancel();
    await lake.path("data/types.parquet").streamJson().cancel();
  });

  it("parses JSON query v1 operators", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, region: "west", value: 10, maybe: null },
          { id: 2, region: "east", value: 20, maybe: "x" },
          { id: 3, region: "north", value: 30, maybe: "x" },
        ],
      },
    });

    const rows = await lake
      .query({
        version: 1,
        from: "table",
        select: ["id"],
        where: {
          or: [
            { and: [{ in: ["region", ["west", "east"]] }, { gte: ["value", 20] }] },
            { and: [{ isNull: "maybe" }, { not: { lt: ["value", 10] } }] },
          ],
        },
      })
      .toArray();

    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("collects predicate columns from every expression family for scanner projection", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [{ id: 1, a: 2, b: 3, c: "Los Angeles", d: "x" }],
      },
    });

    await lake
      .path("table")
      .select(["id"])
      .where(isIn(fn("lower", col("c")), [fn("lower", col("d")), "los angeles"]))
      .toArray();
    await lake
      .path("table")
      .where(between("a", col("b"), 4))
      .toArray();
    await lake.path("table").where(like("c", "Los%")).toArray();

    expect(scanner.requestedColumns).toEqual([["c", "d", "id"], ["a", "b"], ["c"]]);
  });

  it("plans hive partition tasks, prunes files, and explains the plan", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "lake/date=2026-01-01/country=US/a.parquet": [{ id: 1, amount: 10 }],
        "lake/date=2026-01-02/country=CA/b.parquet": [{ id: 2, amount: 20 }],
        "lake/date=2026-01-02/country=US/c.parquet": [{ id: 3, amount: 30 }],
      },
    });

    const query = lake
      .hive("lake/**/*.parquet")
      .select(["id"])
      .where(and(eq("country", "US"), gt("amount", 15)));

    expect(await query.toArray()).toEqual([{ id: 3 }]);
    expect(scanner.requestedColumns).toEqual([
      ["amount", "id"],
      ["amount", "id"],
    ]);

    const result = query.run();
    const tasks = await result.planTasks();
    expect(tasks.map((task) => task.path)).toEqual([
      "lake/date=2026-01-01/country=US/a.parquet",
      "lake/date=2026-01-02/country=US/c.parquet",
    ]);
    expect(tasks[0]?.partitionValues).toEqual({ country: "US", date: "2026-01-01" });
    expect(tasks[0]?.projectedColumns).toEqual(["amount", "country", "id"]);
    expect(tasks[0]?.residualPredicate).toMatchObject({ kind: "logical" });

    const explain = await result.explain();
    expect(explain.json).toMatchObject({
      filesPlanned: 2,
      filesSkipped: 1,
      projectedColumns: ["amount", "country", "id"],
    });
    expect(explain.text).toContain("files skipped: 1");
  });

  it("parses hive partition segments and preserves non-hive path pieces", () => {
    expect(
      parseHivePartitions("lake/date=2026-01-01/country=United%20States/file.parquet"),
    ).toEqual({
      country: "United States",
      date: "2026-01-01",
    });
    expect(parseHivePartitions("lake/=bad/empty=/plain/file.parquet")).toEqual({});
  });

  it("keeps hive pruning conservative for not/or/unknown partition expressions", async () => {
    const rowsByPath = {
      "lake/country=US/a.parquet": [{ id: 1, amount: 10 }],
      "lake/country=CA/b.parquet": [{ id: 2, amount: 20 }],
    };

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(not(eq("country", "CA")))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=US/a.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(or(eq("country", "CA"), eq("country", "MX")))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(or(eq("country", "CA"), gt("amount", 0)))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet", "lake/country=US/a.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(lit(true))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet", "lake/country=US/a.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(col("country"))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet", "lake/country=US/a.parquet"]);
  });

  it("supports all simple JSON comparison forms", async () => {
    expect(parseJsonQuery({ version: 1, from: "t", where: { ne: ["a", 1] } }).where).toMatchObject({
      kind: "compare",
      op: "ne",
    });
    expect(parseJsonQuery({ version: 1, from: "t", limit: 0, offset: 0 })).toMatchObject({
      limit: 0,
      offset: 0,
    });
    expect(parseJsonQuery({ version: 1, from: "t", where: { lte: ["a", 1] } }).where).toMatchObject(
      {
        kind: "compare",
        op: "lte",
      },
    );
    expect(parseJsonQuery({ version: 1, from: "t", where: { gt: ["a", 1] } }).where).toMatchObject({
      kind: "compare",
      op: "gt",
    });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { notIn: ["a", [1]] } }).where,
    ).toMatchObject({ kind: "in", negated: true });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { between: ["a", 1, 2] } }).where,
    ).toMatchObject({ kind: "between" });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { isNotNull: "a" } }).where,
    ).toMatchObject({
      kind: "null-check",
      negated: true,
    });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { like: ["a", "%x%"] } }).where,
    ).toMatchObject({
      kind: "like",
      caseInsensitive: false,
    });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { ilike: ["a", "%x%"] } }).where,
    ).toMatchObject({
      kind: "like",
      caseInsensitive: true,
    });
  });

  it("throws typed parse and validation errors", async () => {
    expect(() => parseJsonQuery(null)).toThrowError(LaQLError);
    expect(() => parseJsonQuery({ version: 2, from: "t" })).toThrow(/version must be 1/u);
    expect(() => parseJsonQuery({ version: 1, from: 3 })).toThrow(/from must be a string/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", select: [1] })).toThrow(/select must be/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", limit: -1 })).toThrow(/limit/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", offset: 1.5 })).toThrow(/offset/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { nope: 1 } })).toThrow(
      /Unsupported/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { eq: ["a"] } })).toThrow(
      /2-item/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { in: ["a", 1] } })).toThrow(
      /values must/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { like: ["a", 1] } })).toThrow(
      /pattern/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { eq: ["a", {}] } })).toThrow(
      /scalar/u,
    );

    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1 }] } });
    expect(() => lake.path("table").limit(-1).run()).toThrow(/limit/u);
    expect(() => lake.path("table").offset(-1).run()).toThrow(/offset/u);
    expect(() => lake.path("table").batchSize(0).run()).toThrow(/batchSize/u);
  });

  it("throws typed runtime errors for missing objects, unknown columns, and budgets", async () => {
    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1, value: 3 }] } });

    await expect(lake.path("missing").toArray()).rejects.toMatchObject({
      code: "LAQL_OBJECT_NOT_FOUND",
    });
    await expect(lake.path("table").select(["missing"]).toArray()).rejects.toMatchObject({
      code: "LAQL_UNKNOWN_COLUMN",
    });
    await expect(lake.path("table").where(gt("missing", 1)).toArray()).rejects.toMatchObject({
      code: "LAQL_UNKNOWN_COLUMN",
    });
  });

  it("enforces every observable phase 1 budget", async () => {
    const rowsByPath = { table: [{ id: 1 }, { id: 2 }] };

    await expect(
      (await makeLake({ rowsByPath, budget: { maxFiles: 0 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({
      code: "LAQL_BUDGET_EXCEEDED",
    });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxBytes: 1 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({
      code: "LAQL_BUDGET_EXCEEDED",
    });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxRowsDecoded: 1 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({ code: "LAQL_BUDGET_EXCEEDED" });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxOutputRows: 1 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({ code: "LAQL_BUDGET_EXCEEDED" });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxRangeRequests: 0 } })).lake
        .path("table")
        .toArray(),
    ).rejects.toMatchObject({ code: "LAQL_BUDGET_EXCEEDED" });

    let now = 0;
    await expect(
      (
        await makeLake({
          rowsByPath,
          budget: { maxElapsedMs: 0 },
          now: () => {
            now += 1;
            return now;
          },
        })
      ).lake
        .path("table")
        .toArray(),
    ).rejects.toMatchObject({ code: "LAQL_BUDGET_EXCEEDED" });
  });

  it("handles empty JSON streams and no-match predicates", async () => {
    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1, maybe: null }] } });

    expect(
      await lake
        .path("table")
        .where(not(isNull("maybe")))
        .count(),
    ).toBe(0);
    await expect(
      new Response(lake.path("table").where(eq("id", 2)).streamJson()).text(),
    ).resolves.toBe("[]");
  });
});
