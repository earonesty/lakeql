import type { createParquetLake } from "lakeql-parquet";
import type { parseSql } from "lakeql-sql";

type Lake = ReturnType<typeof createParquetLake>;

export function applyAst(builder: ReturnType<Lake["path"]>, ast: ReturnType<typeof parseSql>) {
  let next = builder;
  if (ast.select) next = next.select(ast.select);
  if (ast.projections) next = next.project(ast.projections);
  if (ast.where) next = next.where(ast.where);
  for (const [alias, expr] of Object.entries(ast.windows ?? {})) next = next.window(alias, expr);
  if (ast.qualify) next = next.qualify(ast.qualify);
  if (ast.distinct === true) next = next.distinct();
  if (ast.orderBy) next = next.orderBy(ast.orderBy);
  if (ast.offset !== undefined) next = next.offset(ast.offset);
  if (ast.limit !== undefined) next = next.limit(ast.limit);
  return next;
}
