import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Expr } from "./expr.js";
import { collectExprColumns } from "./expr-utils.js";
import { stableStringify } from "./manifest.js";
import type { MemoryScope } from "./memory-scope.js";
import { internStableDataKey } from "./owned-checkpoint-encoding.js";
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
  if (input.select === undefined || input.select.length === 0 || input.select.includes("*"))
    return undefined;
  const columns = new Set<string>();
  const windowAliases = new Set(Object.keys(input.windows ?? {}));
  for (const column of input.select ?? []) {
    if (column !== "*" && !windowAliases.has(column)) columns.add(column);
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
  scope?: MemoryScope,
): CompatibleWindowSortGroup<T>[] {
  const scratch = scope?.childScope();
  const allocate = <V>(
    owner: MemoryScope | undefined,
    bytes: number,
    purpose: string,
    create: () => V,
  ): V => (owner === undefined ? create() : owner.allocate(bytes, purpose, create).value);
  try {
    const groups = allocate(
      scope,
      c.array,
      "window sort groups",
      () => [] as CompatibleWindowSortGroup<T>[],
    );
    if (items.length === 0) return groups;
    const partitions = allocate(
      scratch,
      c.object,
      "window sort partition directory",
      () => new Map<string, { group: CompatibleWindowSortGroup<T>; keys: string[] }[]>(),
    );
    const ordered = allocate(
      scratch,
      c.array + items.length * c.arraySlot,
      "window ordered sort items",
      () => [...items],
    );
    const sorting = scratch?.reserve(
      c.array + items.length * (c.arraySlot + c.object),
      "window sort planning workspace",
    );
    try {
      ordered.sort((left, right) => right.expr.over.orderBy.length - left.expr.over.orderBy.length);
    } finally {
      sorting?.release();
    }
    const key = (value: unknown) =>
      scratch === undefined ? stableStringify(value) : internStableDataKey(value, scratch);
    for (const item of ordered) {
      const partition = key(item.expr.over.partitionBy);
      const keys = allocate(
        scratch,
        c.array + item.expr.over.orderBy.length * c.arraySlot,
        "window order term keys",
        () => Array.from(item.expr.over.orderBy, key),
      );
      let candidates = partitions.get(partition);
      const candidate = candidates?.find((entry) =>
        keys.every((value, index) => value === entry.keys[index]),
      );
      if (candidate !== undefined) {
        allocate(scope, c.arraySlot, "compatible window item", () =>
          candidate.group.items.push(item),
        );
        continue;
      }
      const group = allocate(
        scope,
        c.object + 2 * c.property + c.array + c.arraySlot,
        "window sort group",
        () => ({ sortExpr: item.expr, items: [item] }),
      );
      allocate(scope, c.arraySlot, "window sort group reference", () => groups.push(group));
      if (candidates === undefined) {
        candidates = allocate(
          scratch,
          c.property + c.array,
          "window partition candidates",
          () => [],
        );
        partitions.set(partition, candidates);
      }
      const entry = allocate(
        scratch,
        c.arraySlot + c.object + 2 * c.property,
        "window sort candidate",
        () => ({ group, keys }),
      );
      candidates.push(entry);
    }
    return groups;
  } finally {
    scratch?.dispose();
  }
}

export function windowSortGroupCount(windows: Record<string, WindowExpr>): number {
  return compatibleWindowSortGroups(Object.values(windows).map((expr) => ({ expr }))).length;
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
  return stableStringify(exprs);
}

function stableOrderTermKey(term: WindowExpr["over"]["orderBy"][number] | undefined): string {
  return stableStringify(term);
}
