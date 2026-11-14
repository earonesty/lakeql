import { MATERIALIZATION_CHARGES as charges } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { evaluate, jsonSafeValue } from "./evaluator.js";
import type { Scalar } from "./expr.js";
import { heapSort } from "./heap-sort.js";
import type { MemoryReservation, MemoryReservationSource } from "./memory-budget.js";
import type { AggregateOp } from "./query.js";
import { compareAggregateScalars, numericAggregateValue } from "./scalar-order.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { filterRow, requireExactWindowAggregate } from "./window-utils.js";

export function aggregateWindowValue(
  rows: Iterable<Row>,
  expr: WindowExpr,
  budget?: MemoryReservationSource,
): Scalar {
  return createWindowAggregateEvaluator(expr, budget)(rows);
}

/** Operation-local evaluator for independent frames. */
export function createWindowAggregateEvaluator(
  expr: WindowExpr,
  budget?: MemoryReservationSource,
): (rows: Iterable<Row>) => Scalar {
  const accumulator = createWindowAggregateAccumulator(expr, budget);
  let active = false;
  return (rows) => {
    if (active) throw aggregateReentry();
    active = true;
    try {
      return accumulator.append(rows);
    } finally {
      active = false;
      accumulator.reset();
    }
  };
}

/** Append-only aggregate state over immutable ordered rows. Call reset when a
 * frame removes rows; no inverse arithmetic or retained per-row directory. */
export function createWindowAggregateAccumulator(
  expr: WindowExpr,
  budget?: MemoryReservationSource,
): {
  append(rows: Iterable<Row>): Scalar;
  reset(): void;
} {
  if (typeof expr.fn !== "object") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window expression is not an aggregate");
  }
  const op = expr.fn.aggregate;
  requireExactWindowAggregate(op);
  const state = new AggregateState(op, quantile(expr), budget);
  let active = false;
  return {
    append(rows) {
      if (active) throw aggregateReentry();
      active = true;
      try {
        for (const row of rows) {
          if (!filterRow(expr, row)) continue;
          state.add(aggregateInputValue(expr, row), expr.distinct === true);
        }
        return state.value();
      } catch (error) {
        state.reset();
        throw error;
      } finally {
        active = false;
      }
    },
    reset() {
      if (active) throw aggregateReentry();
      state.reset();
    },
  };
}

function aggregateReentry(): LakeqlError {
  return new LakeqlError("LAKEQL_VALIDATION_ERROR", "Window aggregate evaluation cannot reenter");
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
  private values: number[] | undefined;
  private valuesReservation: MemoryReservation | undefined;
  private modeValues: Map<string, { value: Scalar; count: number }> | undefined;
  private seenDistinct: Set<string> | undefined;

  constructor(
    private readonly op: AggregateOp,
    private readonly quantile: number | undefined,
    private readonly budget?: MemoryReservationSource,
  ) {}

  reset(): void {
    this.count = this.sum = this.mean = this.m2 = 0;
    this.min = this.max = this.first = this.last = undefined;
    this.values = undefined;
    this.valuesReservation?.release();
    this.valuesReservation = undefined;
    this.modeValues?.clear();
    this.seenDistinct?.clear();
  }

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
        this.addQuantileValue(numericValue(this.op, value));
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
        return continuousQuantile(this.values ?? [], 0.5);
      case "quantile":
        return continuousQuantile(this.values ?? [], this.quantile ?? 0.5);
      case "mode":
        return this.mode();
      case "approx_count_distinct":
        requireExactWindowAggregate(this.op);
        return null;
    }
  }

  private addQuantileValue(value: number): void {
    const reservation = this.budget?.reserve(
      (this.values === undefined ? charges.array : 0) + charges.arraySlot,
      "window quantile values",
    );
    if (reservation !== undefined) {
      if (this.valuesReservation === undefined) this.valuesReservation = reservation;
      else this.valuesReservation = this.valuesReservation.merge(reservation);
    }
    this.values ??= [];
    this.values.push(value);
  }

  private addDistinct(value: Scalar, distinct: boolean): boolean {
    if (!distinct && this.op !== "count_distinct") return true;
    const key = JSON.stringify(jsonSafeValue(value));
    this.seenDistinct ??= new Set();
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
    this.modeValues ??= new Map();
    const current = this.modeValues.get(key);
    if (current === undefined) {
      this.modeValues.set(key, { value, count: 1 });
      return;
    }
    current.count += 1;
  }

  private mode(): Scalar {
    if (this.modeValues === undefined) return null;
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

function continuousQuantile(sorted: number[], q: number): number | null {
  // Numeric state has no insertion-order semantics. Reuse its admitted backing;
  // heapsort requires constant scratch, unlike a copied/native sorted array.
  heapSort(sorted, (a, b) => a - b);
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
