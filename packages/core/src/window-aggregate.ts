import { LakeqlError } from "./errors.js";
import { evaluate, jsonSafeValue } from "./evaluator.js";
import type { Scalar } from "./expr.js";
import type { AggregateOp } from "./query.js";
import { compareAggregateScalars, numericAggregateValue } from "./scalar-order.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { filterRow, requireExactWindowAggregate } from "./window-utils.js";

export function aggregateWindowValue(rows: readonly Row[], expr: WindowExpr): Scalar {
  if (typeof expr.fn !== "object") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window expression is not an aggregate");
  }
  const op = expr.fn.aggregate;
  requireExactWindowAggregate(op);
  const state = new AggregateState(op, quantile(expr));
  for (const row of rows) {
    if (!filterRow(expr, row)) continue;
    const value = aggregateInputValue(expr, row);
    state.add(value, expr.distinct === true);
  }
  return state.value();
}

function aggregateInputValue(expr: WindowExpr, row: Row): Scalar {
  if (expr.args.length === 0) return 1;
  const arg = expr.args[0];
  if (arg === undefined) return 1;
  return evaluate(arg, row);
}

class AggregateState {
  private count = 0;
  private sum = 0;
  private mean = 0;
  private m2 = 0;
  private min: Scalar | undefined;
  private max: Scalar | undefined;
  private first: Scalar | undefined;
  private last: Scalar | undefined;
  private values: Scalar[] = [];
  private modeValues = new Map<string, { value: Scalar; count: number }>();
  private seenDistinct = new Set<string>();

  constructor(
    private readonly op: AggregateOp,
    private readonly quantile: number | undefined,
  ) {}

  add(value: Scalar, distinct: boolean): void {
    if (this.op === "count") {
      if (value !== null && this.addDistinct(value, distinct)) this.count += 1;
      return;
    }
    if (value === null || !this.addDistinct(value, distinct)) return;
    switch (this.op) {
      case "sum":
      case "avg":
        this.addNumber(value);
        break;
      case "var_samp":
      case "var_pop":
      case "stddev_samp":
      case "stddev_pop":
        this.addVariance(value);
        break;
      case "min":
        if (this.min === undefined || compareScalar(value, this.min) < 0) this.min = value;
        break;
      case "max":
        if (this.max === undefined || compareScalar(value, this.max) > 0) this.max = value;
        break;
      case "first":
      case "any":
        if (this.first === undefined) this.first = value;
        break;
      case "last":
        this.last = value;
        break;
      case "median":
      case "quantile":
        this.values.push(numericValue(this.op, value));
        break;
      case "mode":
        this.addMode(value);
        break;
      case "count_distinct":
        this.count += 1;
        break;
      case "approx_count_distinct":
        requireExactWindowAggregate(this.op);
        break;
    }
  }

  value(): Scalar {
    switch (this.op) {
      case "count":
      case "count_distinct":
        return this.count;
      case "sum":
        return this.count === 0 ? null : this.sum;
      case "avg":
        return this.count === 0 ? null : this.sum / this.count;
      case "var_samp":
        return this.count <= 1 ? null : this.m2 / (this.count - 1);
      case "var_pop":
        return this.count === 0 ? null : this.m2 / this.count;
      case "stddev_samp":
        return this.count <= 1 ? null : Math.sqrt(this.m2 / (this.count - 1));
      case "stddev_pop":
        return this.count === 0 ? null : Math.sqrt(this.m2 / this.count);
      case "min":
        return this.min ?? null;
      case "max":
        return this.max ?? null;
      case "first":
      case "any":
        return this.first ?? null;
      case "last":
        return this.last ?? null;
      case "median":
        return continuousQuantile(this.values, 0.5);
      case "quantile":
        return continuousQuantile(this.values, this.quantile ?? 0.5);
      case "mode":
        return this.mode();
      case "approx_count_distinct":
        requireExactWindowAggregate(this.op);
        return null;
    }
  }

  private addDistinct(value: Scalar, distinct: boolean): boolean {
    if (!distinct && this.op !== "count_distinct") return true;
    const key = JSON.stringify(jsonSafeValue(value));
    if (this.seenDistinct.has(key)) return false;
    this.seenDistinct.add(key);
    return true;
  }

  private addNumber(value: Scalar): void {
    const numeric = numericValue(this.op, value);
    this.sum += numeric;
    this.count += 1;
  }

  private addVariance(value: Scalar): void {
    const numeric = numericValue(this.op, value);
    this.count += 1;
    const delta = numeric - this.mean;
    this.mean += delta / this.count;
    this.m2 += delta * (numeric - this.mean);
  }

  private addMode(value: Scalar): void {
    const key = JSON.stringify(jsonSafeValue(value));
    const current = this.modeValues.get(key);
    if (current === undefined) {
      this.modeValues.set(key, { value, count: 1 });
      return;
    }
    current.count += 1;
  }

  private mode(): Scalar {
    let best: { value: Scalar; count: number } | undefined;
    for (const entry of this.modeValues.values()) {
      if (
        best === undefined ||
        entry.count > best.count ||
        (entry.count === best.count && compareScalar(entry.value, best.value) < 0)
      ) {
        best = entry;
      }
    }
    return best?.value ?? null;
  }
}

function quantile(expr: WindowExpr): number | undefined {
  if (typeof expr.fn !== "object" || expr.fn.aggregate !== "quantile") return undefined;
  const arg = expr.args[1];
  if (arg === undefined || arg.kind !== "literal" || typeof arg.value !== "number") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "quantile_cont window percentile must be numeric");
  }
  if (!Number.isFinite(arg.value) || arg.value < 0 || arg.value > 1) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "quantile_cont percentile must be between 0 and 1");
  }
  return arg.value;
}

function continuousQuantile(values: readonly Scalar[], q: number): number | null {
  const sorted = values.map((value) => numericValue("quantile", value)).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const position = (sorted.length - 1) * q;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  const lowerValue = sorted[lower] ?? 0;
  const upperValue = sorted[upper] ?? lowerValue;
  return lowerValue + (upperValue - lowerValue) * (position - lower);
}

function numericValue(op: AggregateOp, value: Scalar): number {
  return numericAggregateValue(op, value);
}

function compareScalar(left: Scalar, right: Scalar): number {
  return compareAggregateScalars(left, right);
}
