import type { Expr } from "./expr.js";
import type { AggregateOp } from "./query.js";

export interface WindowOrderTerm {
  expr: Expr;
  direction?: "asc" | "desc";
  nulls?: "first" | "last";
}

export interface WindowFrameBound {
  kind: "unbounded-preceding" | "preceding" | "current-row" | "following" | "unbounded-following";
  offset?: Expr;
}

export interface WindowFrame {
  mode: "rows" | "range" | "groups";
  start: WindowFrameBound;
  end: WindowFrameBound;
  exclude: "no-others" | "current-row" | "group" | "ties";
}

export type WindowRankingFn =
  | "row_number"
  | "rank"
  | "dense_rank"
  | "percent_rank"
  | "cume_dist"
  | "ntile";

export type WindowValueFn = "lag" | "lead" | "first_value" | "last_value" | "nth_value";

export type WindowFn = WindowRankingFn | WindowValueFn | { aggregate: AggregateOp };

export interface WindowSpec {
  partitionBy: Expr[];
  orderBy: WindowOrderTerm[];
  frame?: WindowFrame;
}

export interface WindowExpr {
  fn: WindowFn;
  args: Expr[];
  over: WindowSpec;
  filter?: Expr;
  ignoreNulls?: boolean;
  distinct?: boolean;
}
