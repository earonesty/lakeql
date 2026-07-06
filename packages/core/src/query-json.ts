import { LakeqlError } from "./errors.js";
import type { Expr } from "./expr.js";
import { col, eq, gt, gte, isIn, isNotNull, isNull, lt, lte, ne, notIn } from "./expr.js";
import type { JsonExpr, OrderByTerm, PathQueryInit } from "./query.js";
import { normalizeOrderBy } from "./query-sort.js";

export function parseJsonQuery(input: unknown): PathQueryInit {
  if (!isRecord(input)) throwParse("JSON query must be an object");
  if (input.version !== 1) throwParse("JSON query version must be 1");
  if (typeof input.from !== "string") throwParse("JSON query from must be a string");
  const init: PathQueryInit = { source: input.from };
  if (input.select !== undefined) {
    if (!Array.isArray(input.select) || !input.select.every((value) => typeof value === "string")) {
      throwParse("JSON query select must be an array of strings");
    }
    init.select = input.select;
  }
  if (input.where !== undefined) init.where = parseJsonExpr(input.where);
  if (input.distinct !== undefined) {
    if (typeof input.distinct !== "boolean") throwParse("JSON query distinct must be a boolean");
    init.distinct = input.distinct;
  }
  if (input.orderBy !== undefined) {
    if (!Array.isArray(input.orderBy)) throwParse("JSON query orderBy must be an array");
    init.orderBy = normalizeOrderBy(input.orderBy.map(parseJsonOrderByTerm));
  }
  if (input.limit !== undefined) init.limit = parseNonNegativeInt(input.limit, "limit");
  if (input.offset !== undefined) init.offset = parseNonNegativeInt(input.offset, "offset");
  return init;
}

function parseJsonOrderByTerm(input: unknown): OrderByTerm {
  if (!isRecord(input)) throwParse("JSON query orderBy terms must be objects");
  if (typeof input.column !== "string") throwParse("JSON query orderBy column must be a string");
  const term: OrderByTerm = { column: input.column };
  if (input.direction !== undefined) {
    if (input.direction !== "asc" && input.direction !== "desc") {
      throwParse("JSON query orderBy direction must be asc or desc");
    }
    term.direction = input.direction;
  }
  if (input.nulls !== undefined) {
    if (input.nulls !== "first" && input.nulls !== "last") {
      throwParse("JSON query orderBy nulls must be first or last");
    }
    term.nulls = input.nulls;
  }
  return term;
}

function parseJsonExpr(input: unknown): Expr {
  if (!isRecord(input)) throwParse("JSON expression must be an object");
  const entries = Object.entries(input);
  if (entries.length !== 1) throwParse("JSON expression must have exactly one operator");
  const [op, value] = entries[0] as [keyof JsonExpr, unknown];
  switch (op) {
    case "eq":
      return tuple2(value, op, eq);
    case "ne":
      return tuple2(value, op, ne);
    case "gt":
      return tuple2(value, op, gt);
    case "gte":
      return tuple2(value, op, gte);
    case "lt":
      return tuple2(value, op, lt);
    case "lte":
      return tuple2(value, op, lte);
    case "in":
      return tupleArray(value, op, isIn);
    case "notIn":
      return tupleArray(value, op, notIn);
    case "between": {
      const tuple = requireTuple(value, op, 3);
      return {
        kind: "between",
        target: col(requireColumn(tuple[0], op)),
        low: literal(tuple[1]),
        high: literal(tuple[2]),
      };
    }
    case "isNotNull":
      return isNotNull(requireColumn(value, op));
    case "isNull":
      return isNull(requireColumn(value, op));
    case "like":
      return tuplePattern(value, op, false);
    case "ilike":
      return tuplePattern(value, op, true);
    case "and":
    case "or": {
      if (!Array.isArray(value)) throwParse(`${op} expects an array`);
      if (value.length < 2) throwParse(`${op} expects at least two expressions`);
      return { kind: "logical", op, operands: value.map(parseJsonExpr) };
    }
    case "not":
      return { kind: "not", operand: parseJsonExpr(value) };
    default:
      throwParse(`Unsupported JSON expression operator ${op}`);
  }
}

function tuple2(
  value: unknown,
  op: string,
  cb: (column: string, value: string | number | boolean | bigint | null) => Expr,
): Expr {
  const tuple = requireTuple(value, op, 2);
  return cb(requireColumn(tuple[0], op), requireScalar(tuple[1], op));
}

function tupleArray(
  value: unknown,
  op: string,
  cb: (column: string, values: (string | number | boolean | bigint | null)[]) => Expr,
): Expr {
  const tuple = requireTuple(value, op, 2);
  if (!Array.isArray(tuple[1])) throwParse(`${op} values must be an array`);
  return cb(
    requireColumn(tuple[0], op),
    tuple[1].map((inner) => requireScalar(inner, op)),
  );
}

function tuplePattern(value: unknown, op: string, caseInsensitive: boolean): Expr {
  const tuple = requireTuple(value, op, 2);
  const pattern = tuple[1];
  if (typeof pattern !== "string") throwParse(`${op} pattern must be a string`);
  return { kind: "like", caseInsensitive, target: col(requireColumn(tuple[0], op)), pattern };
}

function literal(value: unknown): Expr {
  return { kind: "literal", value: requireScalar(value, "literal") };
}

function requireTuple(value: unknown, op: string, length: number): unknown[] {
  if (!Array.isArray(value) || value.length !== length) {
    throwParse(`${op} expects a ${length}-item array`);
  }
  return value;
}

function requireColumn(value: unknown, op: string): string {
  if (typeof value !== "string") throwParse(`${op} column must be a string`);
  return value;
}

function requireScalar(value: unknown, op: string): string | number | boolean | bigint | null {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  throwParse(`${op} value must be a scalar`);
}

function parseNonNegativeInt(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throwParse(`${field} must be a non-negative integer`);
  }
  return value;
}

function throwParse(message: string): never {
  throw new LakeqlError("LAKEQL_PARSE_ERROR", message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
