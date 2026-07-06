import { LakeqlError } from "./errors.js";
import type { IntervalValue } from "./interval.js";
import type { TimestampValue } from "./timestamp.js";

export type Scalar =
  | string
  | number
  | boolean
  | bigint
  | Uint8Array
  | TimestampValue
  | IntervalValue
  | null;

export type CompareOp = "eq" | "ne" | "lt" | "lte" | "gt" | "gte";

export interface LiteralExpr {
  kind: "literal";
  value: Scalar;
}

export interface ColumnExpr {
  kind: "column";
  name: string;
}

export interface CompareExpr {
  kind: "compare";
  op: CompareOp;
  left: Expr;
  right: Expr;
}

export interface InExpr {
  kind: "in";
  negated: boolean;
  target: Expr;
  values: Expr[];
}

export interface BetweenExpr {
  kind: "between";
  target: Expr;
  low: Expr;
  high: Expr;
}

export interface NullCheckExpr {
  kind: "null-check";
  negated: boolean;
  target: Expr;
}

export interface LogicalExpr {
  kind: "logical";
  op: "and" | "or";
  operands: Expr[];
}

export interface NotExpr {
  kind: "not";
  operand: Expr;
}

export interface LikeExpr {
  kind: "like";
  caseInsensitive: boolean;
  target: Expr;
  pattern: string;
}

export interface CallExpr {
  kind: "call";
  fn: string;
  args: Expr[];
}

export interface ArithmeticExpr {
  kind: "arithmetic";
  op: "add" | "sub" | "mul" | "div" | "mod";
  left: Expr;
  right: Expr;
}

export interface CaseWhenExpr {
  when: Expr;
  value: Expr;
}

export interface CaseExpr {
  kind: "case";
  whens: CaseWhenExpr[];
  else?: Expr;
}

export type Expr =
  | LiteralExpr
  | ColumnExpr
  | CompareExpr
  | InExpr
  | BetweenExpr
  | NullCheckExpr
  | LogicalExpr
  | NotExpr
  | LikeExpr
  | CallExpr
  | ArithmeticExpr
  | CaseExpr;

/** A value accepted where a column is expected: a column name or any expression. */
export type ColumnInput = string | Expr;

/** A value accepted where a literal is expected: a scalar or any expression. */
export type ValueInput = Scalar | Expr;

export function col(name: string): ColumnExpr {
  return { kind: "column", name };
}

export function lit(value: Scalar): LiteralExpr {
  return { kind: "literal", value };
}

function isExpr(value: unknown): value is Expr {
  return typeof value === "object" && value !== null && "kind" in value;
}

/** Bare strings name columns in column position. */
function toColumn(input: ColumnInput): Expr {
  return typeof input === "string" ? col(input) : input;
}

/** Bare scalars are literals in value position. */
function toValue(input: ValueInput): Expr {
  return isExpr(input) ? input : lit(input);
}

function compare(op: CompareOp, left: ColumnInput, right: ValueInput): CompareExpr {
  return { kind: "compare", op, left: toColumn(left), right: toValue(right) };
}

export function eq(column: ColumnInput, value: ValueInput): CompareExpr {
  return compare("eq", column, value);
}

export function ne(column: ColumnInput, value: ValueInput): CompareExpr {
  return compare("ne", column, value);
}

export function lt(column: ColumnInput, value: ValueInput): CompareExpr {
  return compare("lt", column, value);
}

export function lte(column: ColumnInput, value: ValueInput): CompareExpr {
  return compare("lte", column, value);
}

export function gt(column: ColumnInput, value: ValueInput): CompareExpr {
  return compare("gt", column, value);
}

export function gte(column: ColumnInput, value: ValueInput): CompareExpr {
  return compare("gte", column, value);
}

export function isIn(column: ColumnInput, values: ValueInput[]): InExpr {
  return { kind: "in", negated: false, target: toColumn(column), values: values.map(toValue) };
}

export function notIn(column: ColumnInput, values: ValueInput[]): InExpr {
  return { kind: "in", negated: true, target: toColumn(column), values: values.map(toValue) };
}

export function between(column: ColumnInput, low: ValueInput, high: ValueInput): BetweenExpr {
  return { kind: "between", target: toColumn(column), low: toValue(low), high: toValue(high) };
}

export function isNull(column: ColumnInput): NullCheckExpr {
  return { kind: "null-check", negated: false, target: toColumn(column) };
}

export function isNotNull(column: ColumnInput): NullCheckExpr {
  return { kind: "null-check", negated: true, target: toColumn(column) };
}

function logical(op: "and" | "or", operands: Expr[]): LogicalExpr {
  if (operands.length < 2) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${op}() requires at least 2 operands`, {
      received: operands.length,
    });
  }
  return { kind: "logical", op, operands };
}

export function and(...operands: Expr[]): LogicalExpr {
  return logical("and", operands);
}

export function or(...operands: Expr[]): LogicalExpr {
  return logical("or", operands);
}

export function not(operand: Expr): NotExpr {
  return { kind: "not", operand };
}

export function like(column: ColumnInput, pattern: string): LikeExpr {
  return { kind: "like", caseInsensitive: false, target: toColumn(column), pattern };
}

export function ilike(column: ColumnInput, pattern: string): LikeExpr {
  return { kind: "like", caseInsensitive: true, target: toColumn(column), pattern };
}

/** Generic function-call expression; named functions (h3_*, st_*) build on this. */
export function fn(name: string, ...args: ValueInput[]): CallExpr {
  return { kind: "call", fn: name, args: args.map(toValue) };
}

export function add(left: ValueInput, right: ValueInput): ArithmeticExpr {
  return { kind: "arithmetic", op: "add", left: toValue(left), right: toValue(right) };
}

export function sub(left: ValueInput, right: ValueInput): ArithmeticExpr {
  return { kind: "arithmetic", op: "sub", left: toValue(left), right: toValue(right) };
}

export function mul(left: ValueInput, right: ValueInput): ArithmeticExpr {
  return { kind: "arithmetic", op: "mul", left: toValue(left), right: toValue(right) };
}

export function div(left: ValueInput, right: ValueInput): ArithmeticExpr {
  return { kind: "arithmetic", op: "div", left: toValue(left), right: toValue(right) };
}

export function mod(left: ValueInput, right: ValueInput): ArithmeticExpr {
  return { kind: "arithmetic", op: "mod", left: toValue(left), right: toValue(right) };
}
