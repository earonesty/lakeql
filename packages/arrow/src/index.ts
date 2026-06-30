import {
  Bool,
  Float64,
  Int64,
  RecordBatchStreamWriter,
  type Table,
  tableFromArrays,
  tableToIPC,
  Utf8,
  vectorFromArray,
} from "apache-arrow";
import {
  type Batch,
  createInMemoryLake,
  type InMemoryLakeOptions,
  isTimestampValue,
  type Lake,
  LakeqlError,
  type QueryBuilder,
  type QueryResult,
  type Row,
  vectorValue,
} from "lakeql-core";

export type ArrowValue =
  | string
  | number
  | bigint
  | boolean
  | Uint8Array
  | null
  | unknown[]
  | Record<string, unknown>;

export interface ArrowTableOptions {
  columns?: string[];
}

export interface ArrowQueryLike {
  toArray(): Promise<Row[]>;
}

export interface ArrowBatchQueryLike {
  batches(): AsyncIterable<Row[]>;
}

export function arrowTableToRows(table: Table): Row[] {
  const columns = table.schema.fields.map((field) => field.name);
  const rows: Row[] = [];
  for (let rowIndex = 0; rowIndex < table.numRows; rowIndex += 1) {
    const json = table.get(rowIndex)?.toJSON() as Record<string, unknown> | null | undefined;
    const row: Row = {};
    for (const column of columns) {
      row[column] = normalizeArrowCell(json?.[column], rowIndex, column);
    }
    rows.push(row);
  }
  return rows;
}

export function createArrowLake(
  tables: Record<string, Table>,
  options: InMemoryLakeOptions = {},
): Lake {
  return createInMemoryLake(
    Object.fromEntries(
      Object.entries(tables).map(([name, table]) => [name, arrowTableToRows(table)]),
    ),
    options,
  );
}

export function rowsToArrowTable(rows: readonly Row[], options: ArrowTableOptions = {}): Table {
  const columns = options.columns ?? inferColumns(rows);
  return tableFromArrays(columnsFromRows(rows, columns));
}

export function batchToArrowTable(batch: Batch, options: ArrowTableOptions = {}): Table {
  const columns = options.columns ?? Object.keys(batch.columns);
  const out: Record<string, ArrowValue[]> = {};
  for (const column of columns) {
    const vector = batch.columns[column];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown batch column ${column}`, { column });
    }
    out[column] = Array.from({ length: batch.rowCount }, (_value, index) => {
      const value = vectorValue(vector, index);
      return isTimestampValue(value) ? value.toJSON() : value;
    });
  }
  return tableFromArrays(out);
}

export async function queryToArrowTable(
  query: ArrowQueryLike | QueryBuilder | QueryResult,
  options: ArrowTableOptions = {},
): Promise<Table> {
  return rowsToArrowTable(await query.toArray(), options);
}

export function rowsToArrowIPC(rows: readonly Row[], options: ArrowTableOptions = {}): Uint8Array {
  return tableToIPC(rowsToArrowTable(rows, options));
}

export function batchToArrowIPC(batch: Batch, options: ArrowTableOptions = {}): Uint8Array {
  return tableToIPC(batchToArrowTable(batch, options));
}

export async function queryToArrowIPC(
  query: ArrowQueryLike | QueryBuilder | QueryResult,
  options: ArrowTableOptions = {},
): Promise<Uint8Array> {
  return tableToIPC(await queryToArrowTable(query, options));
}

export function queryToArrowIPCStream(
  query: ArrowBatchQueryLike | QueryBuilder | QueryResult,
  options: ArrowTableOptions = {},
): ReadableStream<Uint8Array> {
  const writer = new RecordBatchStreamWriter();
  void writer.writeAll(queryRecordBatches(query, options)).catch((error: unknown) => {
    writer.abort(error);
  });
  return writer.toDOMStream({ type: "bytes" }) as ReadableStream<Uint8Array>;
}

async function* queryRecordBatches(
  query: ArrowBatchQueryLike | QueryBuilder | QueryResult,
  options: ArrowTableOptions,
) {
  let columns = options.columns;
  for await (const rows of query.batches()) {
    if (rows.length === 0) continue;
    columns ??= inferColumns(rows);
    yield* rowsToArrowStreamTable(rows, columns).batches;
  }
}

function rowsToArrowStreamTable(rows: readonly Row[], columns: string[]): Table {
  const vectors = Object.fromEntries(
    Object.entries(columnsFromRows(rows, columns)).map(([column, values]) => [
      column,
      vectorFromArray(values, arrowVectorType(values)),
    ]),
  );
  return tableFromArrays(vectors as unknown as Record<string, readonly unknown[]>);
}

function columnsFromRows(rows: readonly Row[], columns: string[]): Record<string, ArrowValue[]> {
  const out: Record<string, ArrowValue[]> = Object.fromEntries(
    columns.map((column) => [column, []]),
  );
  for (const [rowIndex, row] of rows.entries()) {
    for (const column of columns) {
      out[column]?.push(normalizeArrowCell(row[column], rowIndex, column));
    }
  }
  return out;
}

function inferColumns(rows: readonly Row[]): string[] {
  const columns: string[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    for (const column of Object.keys(row)) {
      if (seen.has(column)) continue;
      columns.push(column);
      seen.add(column);
    }
  }
  return columns;
}

function arrowVectorType(values: readonly ArrowValue[]) {
  const value = values.find((entry) => entry !== null);
  switch (typeof value) {
    case "string":
      return new Utf8();
    case "number":
      return new Float64();
    case "bigint":
      return new Int64();
    case "boolean":
      return new Bool();
    case "undefined":
      return new Utf8();
    case "object":
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Arrow conversion for nested values is unsupported",
        {
          value,
        },
      );
  }
}

function normalizeArrowCell(value: unknown, rowIndex: number, column: string): ArrowValue {
  if (value === null || value === undefined) return null;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Arrow output requires scalar cell values", {
    rowIndex,
    column,
    valueType: Array.isArray(value) ? "array" : typeof value,
  });
}
