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
  for (const [id, subquery] of Object.entries(ast.scalarSubqueries)) {
    const rows = await memo.getOrCompute(
      `sql:rows:${sqlMemoKey(subquery.query)}`,
      () => execute(subquery.query),
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
  const out = { ...ast };
  if (ast.where !== undefined) out.where = replaceScalarSubqueryExpr(ast.where, values);
  if (ast.having !== undefined) out.having = replaceScalarSubqueryExpr(ast.having, values);
  if (ast.projections !== undefined) {
    out.projections = Object.fromEntries(
      Object.entries(ast.projections).map(([alias, expr]) => [
        alias,
        replaceScalarSubqueryExpr(expr, values),
      ]),
    );
  }
  if (ast.joins !== undefined) {
    out.joins = ast.joins.map((join) =>
      join.predicate === undefined
        ? join
        : { ...join, predicate: replaceScalarSubqueryExpr(join.predicate, values) },
    );
    const firstJoin = out.joins[0];
    if (firstJoin !== undefined) out.join = firstJoin;
  } else if (ast.join?.predicate !== undefined) {
    out.join = { ...ast.join, predicate: replaceScalarSubqueryExpr(ast.join.predicate, values) };
  }
  if (ast.subqueryJoins !== undefined) {
    out.subqueryJoins = ast.subqueryJoins.map((join) => ({
      ...join,
      ...(join.where === undefined ? {} : { where: replaceScalarSubqueryExpr(join.where, values) }),
      ...(join.predicate === undefined
        ? {}
        : { predicate: replaceScalarSubqueryExpr(join.predicate, values) }),
    }));
    const firstSubqueryJoin = out.subqueryJoins[0];
    if (firstSubqueryJoin !== undefined) out.subqueryJoin = firstSubqueryJoin;
  } else if (ast.subqueryJoin?.where !== undefined || ast.subqueryJoin?.predicate !== undefined) {
    out.subqueryJoin = {
      ...ast.subqueryJoin,
      ...(ast.subqueryJoin.where === undefined
        ? {}
        : { where: replaceScalarSubqueryExpr(ast.subqueryJoin.where, values) }),
      ...(ast.subqueryJoin.predicate === undefined
        ? {}
        : { predicate: replaceScalarSubqueryExpr(ast.subqueryJoin.predicate, values) }),
    };
  }
  delete out.scalarSubqueries;
  return out;
}

function replaceScalarSubqueryExpr(expr: Expr, values: Map<string, unknown>): Expr {
  switch (expr.kind) {
    case "call":
      if (expr.fn === "__lakeql_scalar_subquery") {
        const id = expr.args[0];
        if (id?.kind !== "literal" || typeof id.value !== "string" || !values.has(id.value)) {
          throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Invalid scalar subquery placeholder");
        }
        return { kind: "literal", value: values.get(id.value) as string | number | boolean | null };
      }
      return { ...expr, args: expr.args.map((arg) => replaceScalarSubqueryExpr(arg, values)) };
    case "compare":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, values),
        right: replaceScalarSubqueryExpr(expr.right, values),
      };
    case "between":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, values),
        low: replaceScalarSubqueryExpr(expr.low, values),
        high: replaceScalarSubqueryExpr(expr.high, values),
      };
    case "in":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, values),
        values: expr.values.map((value) => replaceScalarSubqueryExpr(value, values)),
      };
    case "logical":
      return {
        ...expr,
        operands: expr.operands.map((operand) => replaceScalarSubqueryExpr(operand, values)),
      };
    case "not":
      return { ...expr, operand: replaceScalarSubqueryExpr(expr.operand, values) };
    case "null-check":
      return { ...expr, target: replaceScalarSubqueryExpr(expr.target, values) };
    case "like":
      return { ...expr, target: replaceScalarSubqueryExpr(expr.target, values) };
    case "arithmetic":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, values),
        right: replaceScalarSubqueryExpr(expr.right, values),
      };
    case "case":
      return {
        ...expr,
        whens: expr.whens.map((branch) => ({
          when: replaceScalarSubqueryExpr(branch.when, values),
          value: replaceScalarSubqueryExpr(branch.value, values),
        })),
        ...(expr.else === undefined ? {} : { else: replaceScalarSubqueryExpr(expr.else, values) }),
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
    };
    const result = await memo.getOrCompute(
      `sql:lookup:${sqlMemoKey(query)}`,
      async () => {
        const rows = new Map<string, Row>();
        const usage = new RunningCounter();
        for (const row of await execute(query)) {
          const key = JSON.stringify(subquery.rightKey.map((name) => row[name]));
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
