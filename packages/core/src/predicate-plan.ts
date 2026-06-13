import type { Expr } from "./expr.js";

export interface PredicatePlan {
  partition: Expr[];
  fileStats: Expr[];
  rowGroupStats: Expr[];
  residual: Expr[];
}

export interface PredicatePlanOptions {
  partitionColumns?: Iterable<string>;
  fileStatsColumns?: Iterable<string>;
  rowGroupStatsColumns?: Iterable<string>;
}

export function classifyPredicate(
  expr: Expr | undefined,
  options: PredicatePlanOptions = {},
): PredicatePlan {
  const plan: PredicatePlan = {
    partition: [],
    fileStats: [],
    rowGroupStats: [],
    residual: [],
  };
  if (!expr) return plan;
  const partitionColumns = new Set(options.partitionColumns ?? []);
  const fileStatsColumns = new Set(options.fileStatsColumns ?? []);
  const rowGroupStatsColumns = new Set(options.rowGroupStatsColumns ?? []);
  for (const part of conjuncts(expr)) {
    if (usesOnlyColumns(part, partitionColumns)) plan.partition.push(part);
    else if (statsPredicateColumn(part, rowGroupStatsColumns) !== undefined) {
      plan.rowGroupStats.push(part);
    } else if (statsPredicateColumn(part, fileStatsColumns) !== undefined) {
      plan.fileStats.push(part);
    } else {
      plan.residual.push(part);
    }
  }
  return plan;
}

function conjuncts(expr: Expr): Expr[] {
  if (expr.kind === "logical" && expr.op === "and") return expr.operands.flatMap(conjuncts);
  return [expr];
}

function usesOnlyColumns(expr: Expr, columns: Set<string>): boolean {
  const used = new Set<string>();
  collectColumns(expr, used);
  return used.size > 0 && [...used].every((column) => columns.has(column));
}

function statsPredicateColumn(expr: Expr, columns: Set<string>): string | undefined {
  if (columns.size === 0) return undefined;
  switch (expr.kind) {
    case "compare": {
      const column = columnLiteralPair(expr.left, expr.right);
      return column !== undefined && columns.has(column) ? column : undefined;
    }
    case "in":
      if (expr.negated || expr.target.kind !== "column") return undefined;
      if (expr.values.some((value) => value.kind !== "literal" || value.value === null)) {
        return undefined;
      }
      return columns.has(expr.target.name) ? expr.target.name : undefined;
    case "between":
      if (
        expr.target.kind !== "column" ||
        expr.low.kind !== "literal" ||
        expr.high.kind !== "literal"
      ) {
        return undefined;
      }
      return columns.has(expr.target.name) ? expr.target.name : undefined;
    default:
      return undefined;
  }
}

function columnLiteralPair(left: Expr, right: Expr): string | undefined {
  if (left.kind === "column" && right.kind === "literal" && right.value !== null) return left.name;
  if (right.kind === "column" && left.kind === "literal" && left.value !== null) return right.name;
  return undefined;
}

function collectColumns(expr: Expr, columns: Set<string>): void {
  switch (expr.kind) {
    case "column":
      columns.add(expr.name);
      return;
    case "literal":
      return;
    case "compare":
      collectColumns(expr.left, columns);
      collectColumns(expr.right, columns);
      return;
    case "in":
      collectColumns(expr.target, columns);
      for (const value of expr.values) collectColumns(value, columns);
      return;
    case "between":
      collectColumns(expr.target, columns);
      collectColumns(expr.low, columns);
      collectColumns(expr.high, columns);
      return;
    case "null-check":
      collectColumns(expr.target, columns);
      return;
    case "logical":
      for (const operand of expr.operands) collectColumns(operand, columns);
      return;
    case "not":
      collectColumns(expr.operand, columns);
      return;
    case "like":
      collectColumns(expr.target, columns);
      return;
    case "call":
      for (const arg of expr.args) collectColumns(arg, columns);
      return;
  }
}
