import type { Expr } from "./expr.js";
import { collectExprColumns } from "./expr-utils.js";
import { stableStringify } from "./manifest.js";
import type { OrderByTerm } from "./query.js";
import type { WindowExpr } from "./window.js";

export interface WindowReadColumnInput {
  select?: string[];
  orderBy?: OrderByTerm[];
  projections?: Record<string, Expr>;
  windows?: Record<string, WindowExpr>;
  where?: Expr;
  qualify?: Expr;
}

export interface CompatibleWindowSortItem {
  expr: WindowExpr;
}

export interface CompatibleWindowSortGroup<T extends CompatibleWindowSortItem> {
  sortExpr: WindowExpr;
  items: T[];
}

export type WindowTaskPlan =
  | {
      topology: "window-partition-fanout";
      available: true;
      bucketCount: number;
      partitionBy: Expr[];
    }
  | {
      topology: "window-partition-fanout";
      available: false;
      bucketCount: 1;
      reason: string;
    };

export interface WindowExplainPlan {
  expressions: number;
  sortGroups: number;
  qualify: boolean;
  execution: WindowExecutionMode;
  workUnits: WindowTaskPlan;
}

export type WindowExecutionMode = "row-materialized" | "vector-window" | "parquet-work-unit-fanout";

export function windowReadColumns(input: WindowReadColumnInput): string[] | undefined {
  const columns = new Set<string>();
  const windowAliases = new Set(Object.keys(input.windows ?? {}));
  for (const column of input.select ?? []) {
    if (!windowAliases.has(column)) columns.add(column);
  }
  for (const term of input.orderBy ?? []) {
    if (!windowAliases.has(term.column)) columns.add(term.column);
  }
  for (const expr of Object.values(input.projections ?? {})) collectExprColumns(expr, columns);
  for (const expr of Object.values(input.windows ?? {})) collectWindowColumns(expr, columns);
  collectExprColumns(input.where, columns);
  collectExprColumns(input.qualify, columns);
  for (const alias of windowAliases) columns.delete(alias);
  return columns.size === 0 ? undefined : [...columns].sort();
}

export function windowExpressions(windows: Record<string, WindowExpr> | undefined): Expr[] {
  const exprs: Expr[] = [];
  for (const expr of Object.values(windows ?? {})) {
    exprs.push(...expr.over.partitionBy);
    exprs.push(...expr.over.orderBy.map((term) => term.expr));
    exprs.push(...expr.args);
    if (expr.filter !== undefined) exprs.push(expr.filter);
    if (expr.over.frame?.start.offset !== undefined) exprs.push(expr.over.frame.start.offset);
    if (expr.over.frame?.end.offset !== undefined) exprs.push(expr.over.frame.end.offset);
  }
  return exprs;
}

export function collectWindowColumns(expr: WindowExpr, columns: Set<string>): void {
  for (const part of expr.over.partitionBy) collectExprColumns(part, columns);
  for (const term of expr.over.orderBy) collectExprColumns(term.expr, columns);
  for (const arg of expr.args) collectExprColumns(arg, columns);
  collectExprColumns(expr.filter, columns);
  if (expr.over.frame !== undefined) {
    collectExprColumns(expr.over.frame.start.offset, columns);
    collectExprColumns(expr.over.frame.end.offset, columns);
  }
}

export function compatibleWindowSortGroups<T extends CompatibleWindowSortItem>(
  items: readonly T[],
): CompatibleWindowSortGroup<T>[] {
  const groups: CompatibleWindowSortGroup<T>[] = [];
  const ordered = [...items].sort(
    (left, right) => right.expr.over.orderBy.length - left.expr.over.orderBy.length,
  );
  for (const item of ordered) {
    const group = groups.find((candidate) =>
      windowSortSpecsCompatible(candidate.sortExpr, item.expr),
    );
    if (group === undefined) {
      groups.push({ sortExpr: item.expr, items: [item] });
      continue;
    }
    if (group.sortExpr.over.orderBy.length < item.expr.over.orderBy.length) {
      group.sortExpr = item.expr;
    }
    group.items.push(item);
  }
  return groups;
}

export function windowSortGroupCount(windows: Record<string, WindowExpr>): number {
  const sortGroups = new Set<string>();
  for (const group of compatibleWindowSortGroups(
    Object.values(windows).map((expr) => ({ expr })),
  )) {
    sortGroups.add(
      stableStringify({
        partitionBy: group.sortExpr.over.partitionBy,
        orderBy: group.sortExpr.over.orderBy,
      }),
    );
  }
  return sortGroups.size;
}

export function windowTaskPlanForWindows(
  windows: Record<string, WindowExpr>,
  plannedBucketCount: number,
): WindowTaskPlan {
  const partitionSpecs = Object.values(windows).map((expr) => expr.over.partitionBy);
  if (partitionSpecs.length === 0 || partitionSpecs.every((spec) => spec.length === 0)) {
    return {
      topology: "window-partition-fanout",
      available: false,
      bucketCount: 1,
      reason: "window query has one global semantic partition",
    };
  }
  if (partitionSpecs.some((spec) => spec.length === 0)) {
    return {
      topology: "window-partition-fanout",
      available: false,
      bucketCount: 1,
      reason: "window query mixes global and partitioned windows",
    };
  }
  const [first, ...rest] = partitionSpecs;
  const firstKey = stableStringify(first ?? []);
  if (rest.some((spec) => stableStringify(spec) !== firstKey)) {
    return {
      topology: "window-partition-fanout",
      available: false,
      bucketCount: 1,
      reason: "window partition specs are not identical",
    };
  }
  return {
    topology: "window-partition-fanout",
    available: true,
    bucketCount: Math.max(1, Math.min(64, plannedBucketCount)),
    partitionBy: first ?? [],
  };
}

export function windowSortSpecsCompatible(left: WindowExpr, right: WindowExpr): boolean {
  if (stableExprListKey(left.over.partitionBy) !== stableExprListKey(right.over.partitionBy)) {
    return false;
  }
  return (
    orderTermsPrefixOf(left.over.orderBy, right.over.orderBy) ||
    orderTermsPrefixOf(right.over.orderBy, left.over.orderBy)
  );
}

function orderTermsPrefixOf(
  prefix: readonly WindowExpr["over"]["orderBy"][number][],
  full: readonly WindowExpr["over"]["orderBy"][number][],
): boolean {
  if (prefix.length > full.length) return false;
  for (let index = 0; index < prefix.length; index += 1) {
    if (stableOrderTermKey(prefix[index]) !== stableOrderTermKey(full[index])) return false;
  }
  return true;
}

function stableExprListKey(exprs: readonly Expr[]): string {
  return JSON.stringify(exprs);
}

function stableOrderTermKey(term: WindowExpr["over"]["orderBy"][number] | undefined): string {
  return JSON.stringify(term);
}
