import { gatherBatch, vectorOrderByBatch, vectorTopKBatch } from "../packages/core/dist/index.js";

export function orderVectorScanBatch(batch, ast) {
  if (batch === undefined) return undefined;
  if (ast.orderBy === undefined) {
    if (ast.offset === undefined && ast.limit === undefined) return batch;
    return gatherBatch(batch, sliceIndices(batch.rowCount, ast.offset ?? 0, ast.limit));
  }
  if (ast.limit !== undefined) {
    return vectorTopKBatch(batch, ast.orderBy, {
      ...(ast.offset === undefined ? {} : { offset: ast.offset }),
      limit: ast.limit,
    });
  }
  const ordered = vectorOrderByBatch(batch, ast.orderBy);
  if (ast.offset === undefined || ast.offset === 0) return ordered;
  const indices = [];
  for (let index = ast.offset; index < ordered.rowCount; index += 1) indices.push(index);
  return gatherBatch(ordered, indices);
}

export function selectionIndices(selection) {
  const indices = [];
  for (let index = 0; index < selection.length; index += 1) {
    if (selection[index] === 1) indices.push(index);
  }
  return indices;
}

export function referencedColumns(ast) {
  const columns = new Set(ast.select ?? []);
  if (ast.where !== undefined) collectExprColumns(ast.where, columns);
  if (ast.qualify !== undefined) collectExprColumns(ast.qualify, columns);
  for (const expr of Object.values(ast.projections ?? {})) collectExprColumns(expr, columns);
  for (const expr of Object.values(ast.windows ?? {})) collectWindowColumns(expr, columns);
  for (const term of ast.orderBy ?? []) columns.add(term.column);
  for (const alias of Object.keys(ast.windows ?? {})) columns.delete(alias);
  return [...columns].filter((column) => column !== "*");
}

export function referencedJoinSideColumns(ast, alias) {
  const columns = new Set();
  if (ast.join !== undefined) {
    const keys = alias === ast.join.leftAlias ? ast.join.leftKey : ast.join.rightKey;
    for (const key of keys) columns.add(stripAlias(key, alias));
  }
  for (const column of referencedJoinColumns(ast)) {
    if (isQualifiedBy(column, alias)) columns.add(stripAlias(column, alias));
  }
  return [...columns];
}

export function referencedSubqueryJoinColumns(ast, side) {
  const join = ast.subqueryJoin;
  const columns = new Set(side === "left" ? join.leftKey : join.rightKey);
  if (side === "left") {
    for (const column of ast.select ?? []) columns.add(column);
    for (const term of ast.orderBy ?? []) columns.add(term.column);
    for (const expr of Object.values(ast.projections ?? {})) collectExprColumns(expr, columns);
  } else if (join.where !== undefined) {
    collectExprColumns(join.where, columns);
  }
  return [...columns].filter((column) => column !== "*");
}

function referencedJoinColumns(ast) {
  const columns = new Set(ast.select ?? []);
  if (ast.where !== undefined) collectExprColumns(ast.where, columns);
  for (const expr of Object.values(ast.projections ?? {})) collectExprColumns(expr, columns);
  for (const expr of Object.values(ast.windows ?? {})) collectWindowColumns(expr, columns);
  for (const term of ast.orderBy ?? []) columns.add(term.column);
  for (const alias of Object.keys(ast.windows ?? {})) columns.delete(alias);
  return [...columns].filter((column) => column !== "*");
}

function collectWindowColumns(expr, columns) {
  for (const part of expr.over.partitionBy ?? []) collectExprColumns(part, columns);
  for (const term of expr.over.orderBy ?? []) collectExprColumns(term.expr, columns);
  for (const arg of expr.args ?? []) collectExprColumns(arg, columns);
  if (expr.filter !== undefined) collectExprColumns(expr.filter, columns);
}

function sliceIndices(rowCount, offset, limit) {
  const end = limit === undefined ? rowCount : Math.min(rowCount, offset + limit);
  const indices = [];
  for (let index = offset; index < end; index += 1) indices.push(index);
  return indices;
}

function isQualifiedBy(column, alias) {
  return column.startsWith(`${alias}.`);
}

function stripAlias(column, alias) {
  return isQualifiedBy(column, alias) ? column.slice(alias.length + 1) : column;
}

function collectExprColumns(expr, columns) {
  switch (expr.kind) {
    case "column":
      columns.add(expr.name);
      return;
    case "compare":
      collectExprColumns(expr.left, columns);
      collectExprColumns(expr.right, columns);
      return;
    case "between":
      collectExprColumns(expr.target, columns);
      collectExprColumns(expr.low, columns);
      collectExprColumns(expr.high, columns);
      return;
    case "in":
      collectExprColumns(expr.target, columns);
      for (const value of expr.values) collectExprColumns(value, columns);
      return;
    case "logical":
      for (const operand of expr.operands) collectExprColumns(operand, columns);
      return;
    case "not":
      collectExprColumns(expr.operand, columns);
      return;
    case "null-check":
    case "like":
      collectExprColumns(expr.target, columns);
      return;
    case "call":
      for (const arg of expr.args) collectExprColumns(arg, columns);
      return;
    case "arithmetic":
      collectExprColumns(expr.left, columns);
      collectExprColumns(expr.right, columns);
      return;
    case "case":
      for (const branch of expr.whens) {
        collectExprColumns(branch.when, columns);
        collectExprColumns(branch.value, columns);
      }
      if (expr.else !== undefined) collectExprColumns(expr.else, columns);
      return;
    case "literal":
      return;
  }
}
