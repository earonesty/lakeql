import {
  type Expr,
  isTimestampValue,
  jsonSafeValue,
  LakeqlError,
  type Row,
  RunningCounter,
  type SharedMemoryCache,
  stableStringify,
} from "lakeql-core";
import type { SqlCorrelatedScalarSubqueryAst, SqlQueryAst } from "./index.js";

/** Caller owns cache lifetime; SQL roots clear it on completion or failure. */
export function sqlMemoBytes(value: unknown): number {
  return new TextEncoder().encode(stableStringify(jsonSafeValue(value))).byteLength;
}

/** Canonical execution identity, including nested placeholder dependencies.
 * Parser registries can reference their parent registry, so serializing the AST
 * wholesale is unsafe. Only expressions that actually use a dependency include it. */
export function sqlMemoKey(query: SqlQueryAst): string {
  const active = new Set<object>();
  function normalize(query: SqlQueryAst): unknown {
    if (active.has(query))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic SQL subquery dependency");
    active.add(query);
    function visit(value: unknown): unknown {
      if (Array.isArray(value)) return value.map(visit);
      if (value === null || typeof value !== "object") return value;
      const item = value as Record<string, unknown>;
      if (item.kind === "literal") {
        const literal = item.value;
        const value = isTimestampValue(literal)
          ? {
              epochNanoseconds: String(literal.epochNanoseconds),
              unit: literal.unit,
              isAdjustedToUTC: literal.isAdjustedToUTC,
            }
          : typeof literal === "bigint" || typeof literal === "number"
            ? Object.is(literal, -0)
              ? "-0"
              : String(literal)
            : jsonSafeValue(literal);
        return { kind: "literal", valueType: typeof literal, value };
      }
      if (item.kind === "call" && item.fn === "__lakeql_scalar_subquery") {
        const id = (item.args as { value?: string }[])[0]?.value;
        const dependency = id === undefined ? undefined : query.scalarSubqueries?.[id];
        if (dependency === undefined)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing SQL subquery dependency");
        return {
          kind: "scalar-subquery",
          query: normalize(dependency.query),
          column: dependency.column,
          mode: dependency.mode,
        };
      }
      if (item.kind === "call" && item.fn === "__lakeql_correlated_scalar_subquery") {
        const id = (item.args as { value?: string }[])[0]?.value;
        const dependency = id === undefined ? undefined : query.correlatedScalarSubqueries?.[id];
        if (dependency === undefined)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing correlated SQL dependency");
        if (active.has(dependency))
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic correlated SQL dependency");
        active.add(dependency);
        try {
          return { kind: "correlated-subquery", dependency: visit(dependency) };
        } finally {
          active.delete(dependency);
        }
      }
      return Object.fromEntries(Object.entries(item).map(([name, inner]) => [name, visit(inner)]));
    }
    try {
      const { scalarSubqueries: _scalar, correlatedScalarSubqueries: _correlated, ...plan } = query;
      return visit(plan);
    } finally {
      active.delete(query);
    }
  }
  return stableStringify(jsonSafeValue(normalize(query)));
}

export async function resolveSqlScalarSubqueries(
  ast: SqlQueryAst,
  execute: (query: SqlQueryAst) => Promise<Row[]>,
  memo: SharedMemoryCache,
): Promise<SqlQueryAst> {
  if (ast.scalarSubqueries === undefined) return ast;
  const values = new Map<string, unknown>();
  // Registries are shared with parent queries. Resolve only placeholders used
  // by this plan, then let each dependency resolve its own plan through the memo.
  const dependencies = new Set<string>();
  mapScalarExpressions(ast, (expr) =>
    replaceScalarSubqueryExpr(expr, (id) => {
      dependencies.add(id);
      return { kind: "literal", value: null };
    }),
  );
  for (const id of dependencies) {
    const subquery = ast.scalarSubqueries[id];
    if (subquery === undefined)
      throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Missing scalar subquery metadata");
    const rows = await memo.getOrCompute(
      `sql:rows:${sqlMemoKey(subquery.query)}`,
      async () => execute(await resolveSqlScalarSubqueries(subquery.query, execute, memo)),
      sqlMemoBytes,
    );
    if (subquery.mode === "exists") {
      values.set(id, rows.length > 0);
      continue;
    }
    if (rows.length > 1) {
      throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Scalar subquery returned more than one row");
    }
    values.set(id, rows.length === 0 ? null : (rows[0]?.[subquery.column] ?? null));
  }
  const out = mapScalarExpressions(ast, (expr) =>
    replaceScalarSubqueryExpr(expr, (id) => ({
      kind: "literal",
      value: values.get(id) as string | number | boolean | null,
    })),
  );
  // Correlated lookup preparation is a later stage and may still need scalar
  // dependencies from this shared registry. It resolves its own plan on demand.
  if (Object.keys(out.correlatedScalarSubqueries ?? {}).length === 0) delete out.scalarSubqueries;
  return out;
}

function mapScalarExpressions<T extends SqlQueryAst>(ast: T, replace: (expr: Expr) => Expr): T {
  const out = { ...ast };
  if (ast.where !== undefined) out.where = replace(ast.where);
  if (ast.having !== undefined) out.having = replace(ast.having);
  if (ast.qualify !== undefined) out.qualify = replace(ast.qualify);
  if (ast.windows !== undefined) {
    out.windows = Object.fromEntries(
      Object.entries(ast.windows).map(([alias, window]) => [
        alias,
        {
          ...window,
          args: window.args.map(replace),
          ...(window.filter === undefined ? {} : { filter: replace(window.filter) }),
          over: {
            ...window.over,
            partitionBy: window.over.partitionBy.map(replace),
            orderBy: window.over.orderBy.map((term) => ({ ...term, expr: replace(term.expr) })),
            ...(window.over.frame === undefined
              ? {}
              : {
                  frame: {
                    ...window.over.frame,
                    start: {
                      ...window.over.frame.start,
                      ...(window.over.frame.start.offset === undefined
                        ? {}
                        : { offset: replace(window.over.frame.start.offset) }),
                    },
                    end: {
                      ...window.over.frame.end,
                      ...(window.over.frame.end.offset === undefined
                        ? {}
                        : { offset: replace(window.over.frame.end.offset) }),
                    },
                  },
                }),
          },
        },
      ]),
    );
  }
  if (ast.projections !== undefined) {
    out.projections = Object.fromEntries(
      Object.entries(ast.projections).map(([alias, expr]) => [alias, replace(expr)]),
    );
  }
  if (ast.aggregates !== undefined) {
    out.aggregates = Object.fromEntries(
      Object.entries(ast.aggregates).map(([alias, aggregate]) => [
        alias,
        aggregate.expr === undefined ? aggregate : { ...aggregate, expr: replace(aggregate.expr) },
      ]),
    );
  }
  if (ast.joins !== undefined) {
    out.joins = ast.joins.map((join) =>
      join.predicate === undefined ? join : { ...join, predicate: replace(join.predicate) },
    );
    const firstJoin = out.joins[0];
    if (firstJoin !== undefined) out.join = firstJoin;
  } else if (ast.join?.predicate !== undefined) {
    out.join = { ...ast.join, predicate: replace(ast.join.predicate) };
  }
  const subqueryJoins =
    ast.subqueryJoins ?? (ast.subqueryJoin === undefined ? undefined : [ast.subqueryJoin]);
  if (subqueryJoins !== undefined) {
    const mapped = subqueryJoins.map((join) => ({
      ...mapScalarExpressions(join, replace),
      ...(join.predicate === undefined ? {} : { predicate: replace(join.predicate) }),
    }));
    if (ast.subqueryJoins !== undefined) out.subqueryJoins = mapped;
    if (mapped[0] !== undefined) out.subqueryJoin = mapped[0];
  }
  return out;
}

function replaceScalarSubqueryExpr(expr: Expr, replace: (id: string) => Expr): Expr {
  switch (expr.kind) {
    case "call":
      if (expr.fn === "__lakeql_scalar_subquery") {
        const id = expr.args[0];
        if (id?.kind !== "literal" || typeof id.value !== "string") {
          throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Invalid scalar subquery placeholder");
        }
        return replace(id.value);
      }
      return { ...expr, args: expr.args.map((arg) => replaceScalarSubqueryExpr(arg, replace)) };
    case "compare":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, replace),
        right: replaceScalarSubqueryExpr(expr.right, replace),
      };
    case "between":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, replace),
        low: replaceScalarSubqueryExpr(expr.low, replace),
        high: replaceScalarSubqueryExpr(expr.high, replace),
      };
    case "in":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, replace),
        values: expr.values.map((value) => replaceScalarSubqueryExpr(value, replace)),
      };
    case "logical":
      return {
        ...expr,
        operands: expr.operands.map((operand) => replaceScalarSubqueryExpr(operand, replace)),
      };
    case "not":
      return { ...expr, operand: replaceScalarSubqueryExpr(expr.operand, replace) };
    case "null-check":
      return { ...expr, target: replaceScalarSubqueryExpr(expr.target, replace) };
    case "like":
      return { ...expr, target: replaceScalarSubqueryExpr(expr.target, replace) };
    case "arithmetic":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, replace),
        right: replaceScalarSubqueryExpr(expr.right, replace),
      };
    case "case":
      return {
        ...expr,
        whens: expr.whens.map((branch) => ({
          when: replaceScalarSubqueryExpr(branch.when, replace),
          value: replaceScalarSubqueryExpr(branch.value, replace),
        })),
        ...(expr.else === undefined ? {} : { else: replaceScalarSubqueryExpr(expr.else, replace) }),
      };
    case "column":
    case "literal":
      return expr;
  }
}

export interface SqlCorrelatedLookup {
  subquery: SqlCorrelatedScalarSubqueryAst;
  rows: Map<string, Row>;
}

export async function resolveSqlCorrelatedLookups(
  ast: SqlQueryAst,
  execute: (query: SqlQueryAst) => Promise<Row[]>,
  memo: SharedMemoryCache,
): Promise<Map<string, SqlCorrelatedLookup>> {
  const lookups = new Map<string, SqlCorrelatedLookup>();
  for (const [id, subquery] of Object.entries(ast.correlatedScalarSubqueries ?? {})) {
    const query: SqlQueryAst = {
      source: subquery.source,
      select: subquery.rightKey,
      groupBy: subquery.groupBy,
      aggregates: subquery.aggregates,
      ...(subquery.hiddenAggregates === undefined
        ? {}
        : { hiddenAggregates: subquery.hiddenAggregates }),
      ...(subquery.where === undefined ? {} : { where: subquery.where }),
      ...(subquery.having === undefined ? {} : { having: subquery.having }),
      ...(ast.scalarSubqueries === undefined ? {} : { scalarSubqueries: ast.scalarSubqueries }),
    };
    const result = await memo.getOrCompute(
      `sql:lookup:${sqlMemoKey(query)}`,
      async () => {
        const rows = new Map<string, Row>();
        const usage = new RunningCounter();
        const resolved = await resolveSqlScalarSubqueries(query, execute, memo);
        for (const row of await execute(resolved)) {
          const key = sqlCorrelationKey(row, subquery.rightKey);
          if (key === undefined) continue;
          const previous = rows.get(key);
          if (previous !== undefined) usage.remove(sqlMemoBytes([key, previous]));
          usage.add(sqlMemoBytes([key, row]));
          rows.set(key, row);
        }
        return { rows, bytes: usage.bytes };
      },
      (result) => result.bytes,
    );
    lookups.set(id, { subquery, rows: result.rows });
  }
  return lookups;
}

/** Shared lookup identity for both correlation sides. SQL nulls never match. */
export function sqlCorrelationKey(row: Row, columns: readonly string[]): string | undefined {
  const values: unknown[] = [];
  for (const column of columns) {
    const value = row[column];
    if (value === null || value === undefined) return undefined;
    values.push([typeof value, typeof value === "bigint" ? String(value) : jsonSafeValue(value)]);
  }
  return stableStringify(values);
}
