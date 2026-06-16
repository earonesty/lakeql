import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import {
  and,
  between,
  eq,
  gt,
  isIn,
  isNull,
  like,
  memoryStore,
  not,
  or,
  type Row,
  stableStringify,
} from "@laql/core";
import { fixturePath, SALES, STATS, TYPES } from "@laql/fixtures";
import type { SchemaElement } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it } from "vitest";
import { createParquetLake } from "./index.js";

const describeReference = process.env.LAQL_REFERENCE === "1" ? describe : describe.skip;

describeReference("DuckDB reference comparisons", () => {
  it("matches DuckDB for a filtered sales Parquet scan", async () => {
    const laqlRows = await laqlRowsFor(SALES.file, async (lake) =>
      lake
        .path(SALES.file)
        .select(["store_id", "region", "amount"])
        .where(eq("region", "west"))
        .toArray(),
    );
    const referenceRows = await duckDbRows(
      `select store_id, region, amount from read_parquet('${sqlString(fixturePath(SALES.file))}') where region = 'west'`,
    );

    expect(canonicalRows(laqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for primitive and nullable Parquet values", async () => {
    const laqlRows = await laqlRowsFor(TYPES.file, async (lake) =>
      lake.path(TYPES.file).select(["id", "big", "flag", "name", "score"]).toArray(),
    );
    const referenceRows = await duckDbRows(
      `select id, big, flag, name, score from read_parquet('${sqlString(fixturePath(TYPES.file))}') order by id`,
    );

    expect(canonicalRows(laqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for date and timestamp logical values", async () => {
    const schema: SchemaElement[] = [
      { name: "root", num_children: 3 },
      { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
      { name: "event_date", type: "INT32", converted_type: "DATE", repetition_type: "OPTIONAL" },
      {
        name: "event_ts",
        type: "INT64",
        converted_type: "TIMESTAMP_MILLIS",
        repetition_type: "OPTIONAL",
      },
    ];
    const bytes = new Uint8Array(
      parquetWriteBuffer({
        columnData: [
          { name: "id", data: [1, 2, 3] },
          { name: "event_date", data: [19723, 19724, null] },
          {
            name: "event_ts",
            data: [
              new Date("2024-01-01T00:00:00.000Z"),
              new Date("2024-01-02T03:04:05.006Z"),
              null,
            ],
          },
        ],
        schema,
      }),
    );
    const dir = mkdtempSync(join(tmpdir(), "laql-parquet-reference-"));
    const path = join(dir, "logical.parquet");
    writeFileSync(path, bytes);

    try {
      const store = memoryStore();
      await store.put("logical.parquet", bytes);
      const lake = createParquetLake({ store });
      const laqlRows = (await lake.path("logical.parquet").toArray()).map((row) => ({
        id: row.id,
        event_date: isoDateString(row.event_date),
        event_ts: isoDateString(row.event_ts),
      }));
      const referenceRows = await duckDbRows(`
        select
          id,
          strftime(event_date, '%Y-%m-%dT00:00:00.000Z') as event_date,
          strftime(event_ts, '%Y-%m-%dT%H:%M:%S.%gZ') as event_ts
        from read_parquet('${sqlString(path)}')
        order by id
      `);

      expect(canonicalRows(laqlRows)).toEqual(canonicalRows(referenceRows));
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("matches DuckDB-authored decimal, time, date, and timestamp logical values", async () => {
    const dir = mkdtempSync(join(tmpdir(), "laql-duckdb-parquet-reference-"));
    const path = join(dir, "duckdb-logicals.parquet");
    const connection = await DuckDBConnection.create();
    try {
      await connection.run(`
        copy (
          select * from (values
            (
              1,
              cast(123.45 as decimal(9,2)),
              cast('12:34:56.789' as time),
              cast('2024-01-02 03:04:05.006' as timestamp_ms),
              cast('2024-01-02' as date)
            ),
            (
              2,
              cast(-6.78 as decimal(9,2)),
              cast('00:00:01.002' as time),
              cast('2024-02-03 04:05:06.007' as timestamp_ms),
              cast('2024-02-03' as date)
            ),
            (3, null, null, null, null)
          ) as t(id, amount, event_time, event_ts, event_date)
        ) to '${sqlString(path)}' (format parquet)
      `);

      const store = memoryStore();
      await store.put("duckdb-logicals.parquet", readFileSync(path));
      const lake = createParquetLake({ store });
      const laqlRows = (await lake.path("duckdb-logicals.parquet").toArray()).map((row) => ({
        id: row.id,
        amount: row.amount,
        event_time: row.event_time === null ? null : String(row.event_time),
        event_ts: isoDateString(row.event_ts),
        event_date: isoDateString(row.event_date),
      }));
      const referenceRows = await connection.runAndReadAll(`
        select
          id,
          amount::double as amount,
          epoch_us(event_time) as event_time,
          strftime(event_ts, '%Y-%m-%dT%H:%M:%S.%gZ') as event_ts,
          strftime(event_date, '%Y-%m-%dT00:00:00.000Z') as event_date
        from read_parquet('${sqlString(path)}')
        order by id
      `);

      expect(canonicalRows(laqlRows)).toEqual(
        canonicalRows(referenceRows.getRowObjectsJson() as Row[]),
      );
    } finally {
      connection.disconnectSync();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("matches DuckDB-authored unsigned integer and binary values", async () => {
    const dir = mkdtempSync(join(tmpdir(), "laql-duckdb-parquet-reference-"));
    const path = join(dir, "duckdb-unsigned-binary.parquet");
    const connection = await DuckDBConnection.create();
    try {
      await connection.run(`
        copy (
          select * from (values
            (
              1,
              cast(255 as utinyint),
              cast(65535 as usmallint),
              cast(4294967295 as uinteger),
              cast(18446744073709551615 as ubigint),
              cast('abc' as blob)
            ),
            (
              2,
              cast(0 as utinyint),
              cast(1 as usmallint),
              cast(2 as uinteger),
              cast(3 as ubigint),
              cast('\\x00\\x01' as blob)
            ),
            (3, null, null, null, null, null)
          ) as t(id, u8, u16, u32, u64, payload)
        ) to '${sqlString(path)}' (format parquet)
      `);

      const store = memoryStore();
      await store.put("duckdb-unsigned-binary.parquet", readFileSync(path));
      const lake = createParquetLake({ store });
      const laqlRows = (await lake.path("duckdb-unsigned-binary.parquet").toArray()).map((row) => ({
        id: row.id,
        u8: row.u8,
        u16: row.u16,
        u32: row.u32,
        u64: row.u64 === null ? null : String(row.u64),
        payload: row.payload === null ? null : stringToHex(row.payload),
      }));
      const referenceRows = await connection.runAndReadAll(`
        select
          id,
          u8,
          u16,
          u32,
          u64::varchar as u64,
          hex(payload) as payload
        from read_parquet('${sqlString(path)}')
        order by id
      `);

      expect(canonicalRows(laqlRows)).toEqual(
        canonicalRows(referenceRows.getRowObjectsJson() as Row[]),
      );
    } finally {
      connection.disconnectSync();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("matches DuckDB for fixed-length byte arrays", async () => {
    const schema: SchemaElement[] = [
      { name: "root", num_children: 2 },
      { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
      {
        name: "fixed",
        type: "FIXED_LEN_BYTE_ARRAY",
        type_length: 4,
        repetition_type: "OPTIONAL",
      },
    ];
    const bytes = new Uint8Array(
      parquetWriteBuffer({
        columnData: [
          { name: "id", data: [1, 2, 3] },
          {
            name: "fixed",
            data: [new Uint8Array([1, 2, 3, 4]), new Uint8Array([255, 0, 16, 32]), null],
          },
        ],
        schema,
      }),
    );
    const dir = mkdtempSync(join(tmpdir(), "laql-parquet-reference-"));
    const path = join(dir, "fixed-len.parquet");
    writeFileSync(path, bytes);

    try {
      const store = memoryStore();
      await store.put("fixed-len.parquet", bytes);
      const lake = createParquetLake({ store });
      const laqlRows = (await lake.path("fixed-len.parquet").toArray()).map((row) => ({
        id: row.id,
        fixed: row.fixed === null ? null : bytesToHex(row.fixed),
      }));
      const referenceRows = await duckDbRows(`
        select id, hex(fixed) as fixed
        from read_parquet('${sqlString(path)}')
        order by id
      `);

      expect(canonicalRows(laqlRows)).toEqual(canonicalRows(referenceRows));
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("matches DuckDB-authored list and map values", async () => {
    const dir = mkdtempSync(join(tmpdir(), "laql-duckdb-parquet-reference-"));
    const path = join(dir, "duckdb-nested.parquet");
    const connection = await DuckDBConnection.create();
    try {
      await connection.run(`
        copy (
          select * from (values
            (
              1,
              [1, 2, 3]::integer[],
              map(['a', 'b'], [10, 20])::map(varchar, integer)
            ),
            (
              2,
              []::integer[],
              map(['x'], [30])::map(varchar, integer)
            ),
            (3, null::integer[], null::map(varchar, integer))
          ) as t(id, nums, attrs)
        ) to '${sqlString(path)}' (format parquet)
      `);

      const store = memoryStore();
      await store.put("duckdb-nested.parquet", readFileSync(path));
      const lake = createParquetLake({ store });
      const laqlRows = (await lake.path("duckdb-nested.parquet").toArray()).map((row) => ({
        id: row.id,
        nums: row.nums === null ? null : stableStringify(row.nums),
        attrs: row.attrs === null ? null : stableStringify(row.attrs),
      }));
      const referenceRows = await connection.runAndReadAll(`
        select
          id,
          to_json(nums) as nums,
          to_json(attrs) as attrs
        from read_parquet('${sqlString(path)}')
        order by id
      `);

      expect(canonicalRows(laqlRows)).toEqual(
        canonicalRows(referenceRows.getRowObjectsJson() as Row[]),
      );
    } finally {
      connection.disconnectSync();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("matches DuckDB-authored null-heavy scalar and nested values", async () => {
    const dir = mkdtempSync(join(tmpdir(), "laql-duckdb-parquet-reference-"));
    const path = join(dir, "duckdb-null-heavy.parquet");
    const connection = await DuckDBConnection.create();
    try {
      await connection.run(`
        copy (
          select * from (values
            (
              1,
              null::integer,
              null::boolean,
              null::varchar,
              null::decimal(9,2),
              null::date,
              null::timestamp_ms,
              null::integer[],
              null::map(varchar, integer),
              null::blob
            ),
            (
              2,
              42,
              true,
              'present',
              cast(12.34 as decimal(9,2)),
              cast('2024-01-02' as date),
              cast('2024-01-02 03:04:05.006' as timestamp_ms),
              [7, 8]::integer[],
              map(['k'], [9])::map(varchar, integer),
              cast('ok' as blob)
            ),
            (
              3,
              null::integer,
              false,
              null::varchar,
              null::decimal(9,2),
              null::date,
              null::timestamp_ms,
              []::integer[],
              map([]::varchar[], []::integer[])::map(varchar, integer),
              null::blob
            )
          ) as t(id, maybe_int, maybe_bool, maybe_text, maybe_decimal, maybe_date, maybe_ts, maybe_list, maybe_map, maybe_blob)
        ) to '${sqlString(path)}' (format parquet)
      `);

      const store = memoryStore();
      await store.put("duckdb-null-heavy.parquet", readFileSync(path));
      const lake = createParquetLake({ store });
      const laqlRows = (await lake.path("duckdb-null-heavy.parquet").toArray()).map((row) => ({
        id: row.id,
        maybe_int: row.maybe_int,
        maybe_bool: row.maybe_bool,
        maybe_text: row.maybe_text,
        maybe_decimal: row.maybe_decimal,
        maybe_date: isoDateString(row.maybe_date),
        maybe_ts: isoDateString(row.maybe_ts),
        maybe_list: row.maybe_list === null ? null : stableStringify(row.maybe_list),
        maybe_map: row.maybe_map === null ? null : stableStringify(row.maybe_map),
        maybe_blob: row.maybe_blob === null ? null : stringToHex(row.maybe_blob),
      }));
      const referenceRows = await connection.runAndReadAll(`
        select
          id,
          maybe_int,
          maybe_bool,
          maybe_text,
          maybe_decimal::double as maybe_decimal,
          strftime(maybe_date, '%Y-%m-%dT00:00:00.000Z') as maybe_date,
          strftime(maybe_ts, '%Y-%m-%dT%H:%M:%S.%gZ') as maybe_ts,
          to_json(maybe_list) as maybe_list,
          to_json(maybe_map) as maybe_map,
          hex(maybe_blob) as maybe_blob
        from read_parquet('${sqlString(path)}')
        order by id
      `);

      expect(canonicalRows(laqlRows)).toEqual(
        canonicalRows(referenceRows.getRowObjectsJson() as Row[]),
      );
    } finally {
      connection.disconnectSync();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("matches DuckDB for UTC-adjusted millisecond timestamps", async () => {
    const schema: SchemaElement[] = [
      { name: "root", num_children: 2 },
      { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
      {
        name: "ts_utc",
        type: "INT64",
        converted_type: "TIMESTAMP_MILLIS",
        logical_type: { type: "TIMESTAMP", isAdjustedToUTC: true, unit: "MILLIS" },
        repetition_type: "OPTIONAL",
      },
    ];
    const bytes = new Uint8Array(
      parquetWriteBuffer({
        columnData: [
          { name: "id", data: [1, 2, 3] },
          {
            name: "ts_utc",
            data: [
              new Date("2024-01-02T03:04:05.006Z"),
              new Date("2024-01-02T11:04:05.006Z"),
              null,
            ],
          },
        ],
        schema,
      }),
    );
    const dir = mkdtempSync(join(tmpdir(), "laql-parquet-reference-"));
    const path = join(dir, "utc-adjusted-ms.parquet");
    writeFileSync(path, bytes);
    const connection = await DuckDBConnection.create();

    try {
      const store = memoryStore();
      await store.put("utc-adjusted-ms.parquet", bytes);
      const lake = createParquetLake({ store });
      const laqlRows = (await lake.path("utc-adjusted-ms.parquet").toArray()).map((row) => ({
        id: row.id,
        ts_utc: isoDateString(row.ts_utc),
      }));
      await connection.run("set timezone='UTC'");
      const referenceRows = await connection.runAndReadAll(`
        select id, strftime(ts_utc, '%Y-%m-%dT%H:%M:%S.%gZ') as ts_utc
        from read_parquet('${sqlString(path)}')
        order by id
      `);

      expect(canonicalRows(laqlRows)).toEqual(
        canonicalRows(referenceRows.getRowObjectsJson() as Row[]),
      );
    } finally {
      connection.disconnectSync();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it.each([
    {
      name: "string equality",
      fixture: SALES.file,
      select: ["store_id", "region"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake.path(SALES.file).select(["store_id", "region"]).where(eq("region", "east")).toArray(),
      sql: `select store_id, region from read_parquet('${sqlString(fixturePath(SALES.file))}') where region = 'east'`,
    },
    {
      name: "boolean equality",
      fixture: TYPES.file,
      select: ["id", "flag"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake.path(TYPES.file).select(["id", "flag"]).where(eq("flag", true)).toArray(),
      sql: `select id, flag from read_parquet('${sqlString(fixturePath(TYPES.file))}') where flag = true`,
    },
    {
      name: "null predicate",
      fixture: TYPES.file,
      select: ["id", "name"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake.path(TYPES.file).select(["id", "name"]).where(isNull("name")).toArray(),
      sql: `select id, name from read_parquet('${sqlString(fixturePath(TYPES.file))}') where name is null`,
    },
    {
      name: "numeric greater-than",
      fixture: STATS.file,
      select: ["id", "metric"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake.path(STATS.file).select(["id", "metric"]).where(gt("metric", 205)).toArray(),
      sql: `select id, metric from read_parquet('${sqlString(fixturePath(STATS.file))}') where metric > 205`,
    },
    {
      name: "numeric between",
      fixture: STATS.file,
      select: ["id", "metric"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake
          .path(STATS.file)
          .select(["id", "metric"])
          .where(between("metric", 95, 105))
          .toArray(),
      sql: `select id, metric from read_parquet('${sqlString(fixturePath(STATS.file))}') where metric between 95 and 105`,
    },
    {
      name: "numeric in",
      fixture: STATS.file,
      select: ["id", "metric"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake
          .path(STATS.file)
          .select(["id", "metric"])
          .where(isIn("metric", [1, 105, 205]))
          .toArray(),
      sql: `select id, metric from read_parquet('${sqlString(fixturePath(STATS.file))}') where metric in (1, 105, 205)`,
    },
    {
      name: "string like",
      fixture: STATS.file,
      select: ["id", "label"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake.path(STATS.file).select(["id", "label"]).where(like("label", "g2")).toArray(),
      sql: `select id, label from read_parquet('${sqlString(fixturePath(STATS.file))}') where label like 'g2'`,
    },
    {
      name: "and/or/not composition",
      fixture: STATS.file,
      select: ["id", "metric", "label"],
      laql: (lake: ReturnType<typeof createParquetLake>) =>
        lake
          .path(STATS.file)
          .select(["id", "metric", "label"])
          .where(and(or(eq("label", "g0"), eq("label", "g2")), not(gt("metric", 205))))
          .toArray(),
      sql: `select id, metric, label from read_parquet('${sqlString(fixturePath(STATS.file))}') where (label = 'g0' or label = 'g2') and not (metric > 205)`,
    },
  ])("matches DuckDB for $name predicates", async (testCase) => {
    const laqlRows = await laqlRowsFor(testCase.fixture, testCase.laql);
    const referenceRows = await duckDbRows(testCase.sql);

    expect(canonicalRows(laqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("keeps row-group pruning expectations aligned with reference-compared predicates", async () => {
    const store = memoryStore();
    await store.put(STATS.file, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store });
    const result = lake
      .path(STATS.file)
      .select(["id", "metric", "label"])
      .where(between("metric", 95, 105))
      .run();

    expect(canonicalRows(await result.toArray())).toEqual(
      canonicalRows(
        await duckDbRows(
          `select id, metric, label from read_parquet('${sqlString(fixturePath(STATS.file))}') where metric between 95 and 105`,
        ),
      ),
    );
    expect(result.stats.rowGroupsRead).toBe(1);
    expect(result.stats.rowGroupsSkipped).toBe(2);
  });
});

async function laqlRowsFor(
  fixture: string,
  query: (lake: ReturnType<typeof createParquetLake>) => Promise<Row[]>,
): Promise<Row[]> {
  const store = memoryStore();
  await store.put(fixture, readFileSync(fixturePath(fixture)));
  return await query(createParquetLake({ store }));
}

async function duckDbRows(sql: string): Promise<Row[]> {
  const connection = await DuckDBConnection.create();
  try {
    const reader = await connection.runAndReadAll(sql);
    return reader.getRowObjectsJson() as Row[];
  } finally {
    connection.disconnectSync();
  }
}

function canonicalRows(rows: Row[]): Row[] {
  return rows
    .map((row) => normalizeRow(row))
    .sort((left, right) => stableStringify(left).localeCompare(stableStringify(right)));
}

function normalizeRow(row: Row): Row {
  const out: Row = {};
  for (const [key, value] of Object.entries(row)) {
    out[key] = normalizeValue(value);
  }
  return out;
}

function normalizeValue(value: unknown): unknown {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return Array.from(value);
  if (Array.isArray(value)) return value.map(normalizeValue);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(value)) out[key] = normalizeValue(nested);
    return out;
  }
  return value;
}

function sqlString(value: string): string {
  return value.replaceAll("'", "''");
}

function isoDateString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.toISOString();
  throw new Error(`Expected Date-compatible Parquet value, got ${String(value)}`);
}

function stringToHex(value: unknown): string {
  if (typeof value !== "string") throw new Error(`Expected string-compatible bytes, got ${value}`);
  return [...new TextEncoder().encode(value)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("")
    .toUpperCase();
}

function bytesToHex(value: unknown): string {
  if (!(value instanceof Uint8Array)) throw new Error(`Expected byte array, got ${value}`);
  return [...value]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("")
    .toUpperCase();
}
