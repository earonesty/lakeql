import { describe, expect, it } from "vitest";
import { LaQLError } from "./errors.js";
import { and, between, col, eq, fn, gt, isIn, isNull, like, lit, not, or } from "./expr.js";
import { createBookmark } from "./manifest.js";
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
  policy?: ConstructorParameters<typeof Lake>[0]["policy"];
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
    policy: config.policy,
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

  it("applies caller query policy for columns, limits, row filters, and context", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, tenant: "a", visible: true, secret: "no" },
          { id: 2, tenant: "b", visible: true, secret: "no" },
          { id: 3, tenant: "a", visible: false, secret: "no" },
        ],
      },
      policy: {
        allowedColumns: ["id", "tenant", "visible"],
        maxLimit: 1,
        context: { tenant: "a" },
        rowFilter: (context) => and(eq("tenant", String(context.tenant)), eq("visible", true)),
      },
    });

    await expect(lake.path("table").toArray()).resolves.toEqual([
      { id: 1, tenant: "a", visible: true },
    ]);
    expect(scanner.requestedColumns[0]).toEqual(["id", "tenant", "visible"]);

    expect(() => lake.path("table").select(["secret"]).toArray()).toThrowError(LaQLError);
    expect(() => lake.path("table").select(["secret"]).toArray()).toThrow(/disallowed/u);
    expect(() => lake.path("table").where(eq("secret", "x")).toArray()).toThrow(/disallowed/u);
    expect(() =>
      lake
        .path("table")
        .orderBy([{ column: "secret" }])
        .toArray(),
    ).toThrow(/disallowed/u);
  });

  it("validates malformed query policy", async () => {
    const rowsByPath = { table: [{ id: 1 }] };
    const emptyColumns = await makeLake({ rowsByPath, policy: { allowedColumns: [] } });
    expect(() => emptyColumns.lake.path("table").toArray()).toThrow(/allowedColumns/u);
    const badLimit = await makeLake({ rowsByPath, policy: { maxLimit: -1 } });
    expect(() => badLimit.lake.path("table").toArray()).toThrow(/maxLimit/u);
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

  it("slices queries with bookmarks and resumes to the same rows", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, keep: true },
          { id: 2, keep: false },
          { id: 3, keep: true },
          { id: 4, keep: true },
        ],
      },
    });
    const query = lake.path("table").select(["id"]).where(eq("keep", true));

    const first = await query.run({ slice: { maxRows: 2 } });
    expect(first.rows).toEqual([{ id: 1 }, { id: 3 }]);
    expect(first.bookmark).toMatchObject({
      position: { rowOffset: 2, fileIndex: 0, rowGroup: 0 },
    });

    const second = await query.run({ slice: { maxRows: 2, bookmark: first.bookmark } });
    expect(second).toEqual({ rows: [{ id: 4 }] });

    const replayed: Row[] = [];
    for await (const batch of query.resumableBatches({ bookmarkEvery: 1 })) {
      replayed.push(...batch.rows);
    }
    expect(replayed).toEqual(await query.toArray());
  });

  it("orders rows before offset, limit, and projection", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "data/b.parquet": [
          { id: 4, score: 10, name: "low" },
          { id: 2, score: 30, name: "same-b" },
        ],
        "data/a.parquet": [
          { id: 3, score: 30, name: "same-a" },
          { id: 1, score: null, name: "missing" },
        ],
      },
    });

    await expect(
      lake
        .path("data/*.parquet")
        .select(["id", "name"])
        .orderBy([
          { column: "score", direction: "desc", nulls: "last" },
          { column: "id", direction: "asc" },
        ])
        .offset(1)
        .limit(2)
        .batchSize(1)
        .toArray(),
    ).resolves.toEqual([
      { id: 3, name: "same-a" },
      { id: 4, name: "low" },
    ]);
    expect(scanner.requestedColumns[0]).toEqual(["id", "name", "score"]);
    await expect(
      lake
        .path("data/*.parquet")
        .orderBy([{ column: "score", direction: "asc" }])
        .limit(2)
        .toArray(),
    ).resolves.toEqual([{ score: 10 }, { score: 30 }]);
    await expect(
      lake
        .path("data/*.parquet")
        .orderBy([{ column: "score", direction: "desc" }])
        .limit(1)
        .toArray(),
    ).resolves.toEqual([{ score: null }]);
    await expect(
      lake
        .path("data/*.parquet")
        .orderBy([
          { column: "score", direction: "desc", nulls: "last" },
          { column: "id", direction: "desc" },
        ])
        .limit(1)
        .toArray(),
    ).resolves.toEqual([{ id: 3, score: 30 }]);
    expect(
      (
        await lake
          .path("data/*.parquet")
          .orderBy([{ column: "score" }])
          .planTasks()
      )[0],
    ).toMatchObject({ projectedColumns: ["score"] });
  });

  it("parses JSON orderBy and rejects invalid order terms", async () => {
    expect(
      parseJsonQuery({
        version: 1,
        from: "t",
        orderBy: [{ column: "score", direction: "desc", nulls: "first" }],
      }).orderBy,
    ).toEqual([{ column: "score", direction: "desc", nulls: "first" }]);
    expect(() => parseJsonQuery({ version: 1, from: "t", orderBy: {} })).toThrow(/orderBy/u);
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", orderBy: [{ column: "a", direction: "sideways" }] }),
    ).toThrow(/direction/u);
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", orderBy: [{ column: "a", nulls: "middle" }] }),
    ).toThrow(/nulls/u);

    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1, value: {} }] } });
    expect(() => lake.path("table").orderBy([]).run()).toThrow(/orderBy/u);
    expect(() =>
      lake
        .path("table")
        .orderBy([{ column: "" }])
        .run(),
    ).toThrow(/columns/u);
    expect(() =>
      lake
        .path("table")
        .orderBy([{ column: "id", direction: "sideways" }])
        .run(),
    ).toThrow(/direction/u);
    await expect(
      lake
        .path("table")
        .orderBy([{ column: "missing" }])
        .toArray(),
    ).rejects.toMatchObject({ code: "LAQL_UNKNOWN_COLUMN" });
    await expect(
      lake
        .path("table")
        .orderBy([{ column: "value" }])
        .toArray(),
    ).rejects.toMatchObject({ code: "LAQL_TYPE_ERROR" });

    const mixed = await makeLake({
      rowsByPath: { table: [{ value: 1 }, { value: "two" }] },
    });
    await expect(
      mixed.lake
        .path("table")
        .orderBy([{ column: "value" }])
        .toArray(),
    ).rejects.toMatchObject({ code: "LAQL_TYPE_ERROR" });
  });

  it("rejects stale or invalid slice bookmarks", async () => {
    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1 }] } });
    const query = lake.path("table");

    await expect(query.run({ slice: { maxRows: 0 } })).rejects.toMatchObject({
      code: "LAQL_TYPE_ERROR",
    });
    await expect(
      query.run({
        slice: {
          maxRows: 1,
          bookmark: createBookmark({
            planFingerprint: "fp_stale",
            snapshot: "snapshot",
            position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
          }),
        },
      }),
    ).rejects.toMatchObject({ code: "LAQL_BOOKMARK_STALE" });
  });

  it("aggregates grouped rows with bounded group counts", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { region: "west", amount: 10, id: 1, label: "a" },
          { region: "west", amount: 20, id: 2, label: "b" },
          { region: "east", amount: 7, id: 2, label: "c" },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate(
          {
            rows: { op: "count" },
            total: { op: "sum", column: "amount" },
            average: { op: "avg", column: "amount" },
            minId: { op: "min", column: "id" },
            maxId: { op: "max", column: "id" },
            distinctIds: { op: "count_distinct", column: "id" },
            firstLabel: { op: "first", column: "label" },
            lastLabel: { op: "last", column: "label" },
            anyLabel: { op: "any", column: "label" },
          },
          { maxGroups: 2 },
        ),
    ).resolves.toEqual([
      {
        region: "west",
        rows: 2,
        total: 30,
        average: 15,
        minId: 1,
        maxId: 2,
        distinctIds: 2,
        firstLabel: "a",
        lastLabel: "b",
        anyLabel: "a",
      },
      {
        region: "east",
        rows: 1,
        total: 7,
        average: 7,
        minId: 2,
        maxId: 2,
        distinctIds: 1,
        firstLabel: "c",
        lastLabel: "c",
        anyLabel: "c",
      },
    ]);

    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ rows: { op: "count" } }, { maxGroups: 1 }),
    ).rejects.toMatchObject({ code: "LAQL_GROUP_LIMIT_EXCEEDED" });
  });

  it("validates aggregate requests and value types", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [{ region: "west", amount: "not numeric" }],
      },
    });

    await expect(
      lake
        .path("table")
        .groupBy([""])
        .aggregate({ rows: { op: "count" } }),
    ).rejects.toMatchObject({ code: "LAQL_TYPE_ERROR" });
    await expect(lake.path("table").groupBy(["region"]).aggregate({})).rejects.toMatchObject({
      code: "LAQL_TYPE_ERROR",
    });
    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ total: { op: "sum", column: "amount" } }),
    ).rejects.toMatchObject({ code: "LAQL_TYPE_ERROR" });
    await expect(
      lake
        .path("table")
        .groupBy(["missing"])
        .aggregate({ rows: { op: "count" } }),
    ).rejects.toMatchObject({ code: "LAQL_UNKNOWN_COLUMN" });
  });
});
