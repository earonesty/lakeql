import { LakeqlError } from "./errors.js";
import type { Expr } from "./expr.js";
import { col, eq, gt, gte, isIn, isNotNull, isNull, lt, lte, ne, notIn } from "./expr.js";
import type { AggregateOp, JsonExpr, OrderByTerm, PathQueryInit } from "./query.js";
import { normalizeOrderBy } from "./query-sort.js";
import type {
  WindowExpr,
  WindowFn,
  WindowFrame,
  WindowFrameBound,
  WindowOrderTerm,
} from "./window.js";

const COMPARE_OPS = ["eq", "ne", "lt", "lte", "gt", "gte"] as const;
const ARITHMETIC_OPS = ["add", "sub", "mul", "div", "mod"] as const;
const AGGREGATE_OPS = [
  "count",
  "sum",
  "avg",
  "var_samp",
  "var_pop",
  "stddev_samp",
  "stddev_pop",
  "median",
  "quantile",
  "min",
  "max",
  "count_distinct",
  "approx_count_distinct",
  "mode",
  "first",
  "last",
] as const;
const WINDOW_FN_NAMES = [
  "row_number",
  "rank",
  "dense_rank",
  "percent_rank",
  "cume_dist",
  "ntile",
  "lag",
  "lead",
  "first_value",
  "last_value",
  "nth_value",
] as const;
const WINDOW_FRAME_MODES = ["rows", "range", "groups"] as const;
const WINDOW_FRAME_EXCLUDES = ["no-others", "current-row", "group", "ties"] as const;
const WINDOW_FRAME_BOUND_KINDS = [
  "unbounded-preceding",
  "preceding",
  "current-row",
  "following",
  "unbounded-following",
] as const;

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
  if (input.windows !== undefined) init.windows = parseJsonWindows(input.windows);
  if (input.qualify !== undefined) init.qualify = parseJsonExpr(input.qualify);
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
  if (typeof input.kind === "string") return parseJsonExprAst(input);
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

function parseJsonExprAst(input: Record<string, unknown>): Expr {
  switch (input.kind) {
    case "literal":
      return { kind: "literal", value: requireScalar(input.value, "literal") };
    case "column":
      if (typeof input.name !== "string") throwParse("column expression name must be a string");
      return { kind: "column", name: input.name };
    case "compare": {
      if (!isCompareOp(input.op))
        throwParse("compare expression op must be eq, ne, lt, lte, gt, or gte");
      return {
        kind: "compare",
        op: input.op,
        left: parseJsonExpr(input.left),
        right: parseJsonExpr(input.right),
      };
    }
    case "in": {
      if (typeof input.negated !== "boolean") throwParse("in expression negated must be a boolean");
      if (!Array.isArray(input.values)) throwParse("in expression values must be an array");
      return {
        kind: "in",
        negated: input.negated,
        target: parseJsonExpr(input.target),
        values: input.values.map(parseJsonExpr),
      };
    }
    case "between":
      return {
        kind: "between",
        target: parseJsonExpr(input.target),
        low: parseJsonExpr(input.low),
        high: parseJsonExpr(input.high),
      };
    case "null-check":
      if (typeof input.negated !== "boolean") {
        throwParse("null-check expression negated must be a boolean");
      }
      return { kind: "null-check", negated: input.negated, target: parseJsonExpr(input.target) };
    case "logical": {
      if (input.op !== "and" && input.op !== "or") {
        throwParse("logical expression op must be and or or");
      }
      if (!Array.isArray(input.operands))
        throwParse("logical expression operands must be an array");
      if (input.operands.length < 2) {
        throwParse("logical expression operands must include at least two expressions");
      }
      return { kind: "logical", op: input.op, operands: input.operands.map(parseJsonExpr) };
    }
    case "not":
      return { kind: "not", operand: parseJsonExpr(input.operand) };
    case "like":
      if (typeof input.caseInsensitive !== "boolean") {
        throwParse("like expression caseInsensitive must be a boolean");
      }
      if (typeof input.pattern !== "string") throwParse("like expression pattern must be a string");
      return {
        kind: "like",
        caseInsensitive: input.caseInsensitive,
        target: parseJsonExpr(input.target),
        pattern: input.pattern,
      };
    case "call":
      if (typeof input.fn !== "string") throwParse("call expression fn must be a string");
      if (!Array.isArray(input.args)) throwParse("call expression args must be an array");
      return { kind: "call", fn: input.fn, args: input.args.map(parseJsonExpr) };
    case "arithmetic":
      if (!isArithmeticOp(input.op)) throwParse("arithmetic expression op is invalid");
      return {
        kind: "arithmetic",
        op: input.op,
        left: parseJsonExpr(input.left),
        right: parseJsonExpr(input.right),
      };
    case "case": {
      if (!Array.isArray(input.whens)) throwParse("case expression whens must be an array");
      return {
        kind: "case",
        whens: input.whens.map((when) => {
          if (!isRecord(when)) throwParse("case expression whens must be objects");
          return { when: parseJsonExpr(when.when), value: parseJsonExpr(when.value) };
        }),
        ...(input.else === undefined ? {} : { else: parseJsonExpr(input.else) }),
      };
    }
    default:
      throwParse(`Unsupported JSON expression kind ${String(input.kind)}`);
  }
}

function parseJsonWindows(input: unknown): Record<string, WindowExpr> {
  if (!isRecord(input)) throwParse("JSON query windows must be an object");
  const windows: Record<string, WindowExpr> = {};
  for (const [alias, value] of Object.entries(input)) {
    if (alias.length === 0 || isPrototypeMutationKey(alias)) {
      throwParse("JSON query window aliases must be valid object keys");
    }
    windows[alias] = parseJsonWindowExpr(value);
  }
  return windows;
}

function parseJsonWindowExpr(input: unknown): WindowExpr {
  if (!isRecord(input)) throwParse("JSON window expression must be an object");
  const over = input.over;
  if (!isRecord(over)) throwParse("JSON window expression over must be an object");
  if (!Array.isArray(input.args)) throwParse("JSON window expression args must be an array");
  const expr: WindowExpr = {
    fn: parseJsonWindowFn(input.fn),
    args: input.args.map(parseJsonExpr),
    over: {
      partitionBy: parseExprArray(over.partitionBy, "JSON window partitionBy"),
      orderBy: parseJsonWindowOrderBy(over.orderBy),
      ...(over.frame === undefined ? {} : { frame: parseJsonWindowFrame(over.frame) }),
    },
  };
  if (input.filter !== undefined) expr.filter = parseJsonExpr(input.filter);
  if (input.ignoreNulls !== undefined) {
    if (typeof input.ignoreNulls !== "boolean") {
      throwParse("JSON window expression ignoreNulls must be a boolean");
    }
    expr.ignoreNulls = input.ignoreNulls;
  }
  if (input.distinct !== undefined) {
    if (typeof input.distinct !== "boolean") {
      throwParse("JSON window expression distinct must be a boolean");
    }
    expr.distinct = input.distinct;
  }
  return expr;
}

function parseJsonWindowFn(input: unknown): WindowFn {
  if (isWindowFnName(input)) return input;
  if (isRecord(input) && isAggregateOp(input.aggregate)) return { aggregate: input.aggregate };
  throwParse("JSON window function is invalid");
}

function parseJsonWindowOrderBy(input: unknown): WindowOrderTerm[] {
  if (input === undefined) return [];
  if (!Array.isArray(input)) throwParse("JSON window orderBy must be an array");
  return input.map((term) => {
    if (!isRecord(term)) throwParse("JSON window orderBy terms must be objects");
    const out: WindowOrderTerm = { expr: parseJsonExpr(term.expr) };
    if (term.direction !== undefined) {
      if (term.direction !== "asc" && term.direction !== "desc") {
        throwParse("JSON window orderBy direction must be asc or desc");
      }
      out.direction = term.direction;
    }
    if (term.nulls !== undefined) {
      if (term.nulls !== "first" && term.nulls !== "last") {
        throwParse("JSON window orderBy nulls must be first or last");
      }
      out.nulls = term.nulls;
    }
    return out;
  });
}

function parseJsonWindowFrame(input: unknown): WindowFrame {
  if (!isRecord(input)) throwParse("JSON window frame must be an object");
  if (!isOneOf(WINDOW_FRAME_MODES, input.mode)) {
    throwParse("JSON window frame mode must be rows, range, or groups");
  }
  if (!isOneOf(WINDOW_FRAME_EXCLUDES, input.exclude))
    throwParse("JSON window frame exclude is invalid");
  return {
    mode: input.mode,
    start: parseJsonWindowFrameBound(input.start),
    end: parseJsonWindowFrameBound(input.end),
    exclude: input.exclude,
  };
}

function parseJsonWindowFrameBound(input: unknown): WindowFrameBound {
  if (!isRecord(input)) throwParse("JSON window frame bound must be an object");
  if (!isOneOf(WINDOW_FRAME_BOUND_KINDS, input.kind))
    throwParse("JSON window frame bound kind is invalid");
  return {
    kind: input.kind,
    ...(input.offset === undefined ? {} : { offset: parseJsonExpr(input.offset) }),
  };
}

function parseExprArray(input: unknown, field: string): Expr[] {
  if (input === undefined) return [];
  if (!Array.isArray(input)) throwParse(`${field} must be an array`);
  return input.map(parseJsonExpr);
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

function isCompareOp(
  value: unknown,
): value is Expr extends { kind: "compare"; op: infer Op } ? Op : never {
  return isOneOf(COMPARE_OPS, value);
}

function isArithmeticOp(
  value: unknown,
): value is Expr extends { kind: "arithmetic"; op: infer Op } ? Op : never {
  return isOneOf(ARITHMETIC_OPS, value);
}

function isAggregateOp(value: unknown): value is AggregateOp {
  return isOneOf(AGGREGATE_OPS, value);
}

function isWindowFnName(value: unknown): value is Exclude<WindowFn, { aggregate: AggregateOp }> {
  return isOneOf(WINDOW_FN_NAMES, value);
}

function isPrototypeMutationKey(value: string): boolean {
  return value === "__proto__" || value === "prototype" || value === "constructor";
}

function isOneOf<const Values extends readonly string[]>(
  values: Values,
  value: unknown,
): value is Values[number] {
  return typeof value === "string" && values.includes(value);
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
