import { continuousQuantileInPlace } from "./aggregate-quantile.js";
import { MATERIALIZATION_CHARGES as charges } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { evaluate, jsonSafeValue } from "./evaluator.js";
import type { Scalar } from "./expr.js";
import { jsonScalarKeyAllocationBytes } from "./json-scalar-key-memory.js";
import {
  MemoryBudget,
  type MemoryReservation,
  type MemoryReservationSource,
} from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { MutableMemoryLedger } from "./mutable-memory-ledger.js";
import type { AggregateOp } from "./query.js";
import { compareAggregateScalars, numericAggregateValue } from "./scalar-order.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { filterRow, requireExactWindowAggregate } from "./window-utils.js";
import { retainWindowValue } from "./window-value-ownership.js";

export function aggregateWindowValue(
  rows: Iterable<Row>,
  expr: WindowExpr,
  budget?: MemoryReservationSource,
  resultScope?: MemoryScope,
): Scalar {
  return createWindowAggregateEvaluator(expr, budget, resultScope)(rows);
}

/** Operation-local evaluator for independent frames. */
export function createWindowAggregateEvaluator(
  expr: WindowExpr,
  budget?: MemoryReservationSource,
  resultScope?: MemoryScope,
): (rows: Iterable<Row>) => Scalar {
  const accumulator = createWindowAggregateAccumulator(expr, budget, resultScope);
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
  resultScope?: MemoryScope,
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
      if (budget instanceof MemoryScope && budget.disposed) {
        state.reset();
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Window aggregate memory scope is disposed",
        );
      }
      active = true;
      try {
        for (const row of rows) {
          const temporary =
            budget instanceof MemoryScope
              ? budget.childScope()
              : budget instanceof MemoryBudget
                ? new MemoryScope(budget)
                : undefined;
          try {
            if (!filterRow(expr, row, temporary)) continue;
            state.add(aggregateInputValue(expr, row, temporary), expr.distinct === true, temporary);
          } finally {
            temporary?.dispose();
          }
        }
        return state.result(resultScope);
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

function aggregateInputValue(expr: WindowExpr, row: Row, scope?: MemoryScope): Scalar {
  if (expr.args.length === 0) return 1;
  const arg = expr.args[0];
  if (arg === undefined) return 1;
  return evaluate(arg, row, scope);
}

class AggregateState {
  private payloadScope: MemoryScope | undefined;
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
  private bufferScope: MemoryScope | undefined;
  private bufferLedger: MutableMemoryLedger | undefined;
  private modeValues: Map<string, { value: Scalar; count: number }> | undefined;
  private seenDistinct: Set<string> | undefined;
  private keyReservation: MemoryReservation | undefined;

  constructor(
    private readonly op: AggregateOp,
    private readonly quantile: number | undefined,
    private readonly budget?: MemoryReservationSource,
  ) {}

  reset(): void {
    this.count = this.sum = this.mean = this.m2 = 0;
    this.min = this.max = this.first = this.last = undefined;
    this.values = undefined;
    this.payloadScope?.dispose();
    this.payloadScope = undefined;
    this.bufferScope?.dispose();
    this.bufferScope = undefined;
    this.bufferLedger = undefined;
    this.valuesReservation?.release();
    this.valuesReservation = undefined;
    this.modeValues = undefined;
    this.seenDistinct = undefined;
    this.keyReservation?.release();
    this.keyReservation = undefined;
  }

  add(value: Scalar, distinct: boolean, temporary?: MemoryScope): void {
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
        if (this.min === undefined || compareScalar(value, this.min) < 0)
          this.min = this.capture(value, temporary);
        break;
      case "max":
        if (this.max === undefined || compareScalar(value, this.max) > 0)
          this.max = this.capture(value, temporary);
        break;
      case "first":
      case "any":
        if (this.first === undefined) this.first = this.capture(value, temporary);
        break;
      case "last":
        this.last = this.capture(value, temporary);
        break;
      case "median":
      case "quantile":
        this.addQuantileValue(numericValue(this.op, value));
        break;
      case "mode":
        this.addMode(value, temporary);
        break;
      case "count_distinct":
        this.count += 1;
        break;
      case "approx_count_distinct":
        requireExactWindowAggregate(this.op);
        break;
    }
  }

  result(scope?: MemoryScope): Scalar {
    return retainWindowValue(this.value(), scope, this.payloadScope) as Scalar;
  }

  private capture(value: Scalar, temporary?: MemoryScope, append = false): Scalar {
    if (!(this.budget instanceof MemoryBudget || this.budget instanceof MemoryScope)) return value;
    const previous = this.payloadScope;
    if (value === null || typeof value === "number" || typeof value === "boolean") {
      if (!append) {
        previous?.dispose();
        this.payloadScope = undefined;
      }
      return value;
    }
    const scope =
      append && previous !== undefined
        ? previous
        : this.budget instanceof MemoryScope
          ? this.budget.childScope()
          : new MemoryScope(this.budget);
    try {
      const retained = retainWindowValue(value, scope, temporary) as Scalar;
      this.payloadScope = scope;
      if (previous !== scope) previous?.dispose();
      return retained;
    } catch (error) {
      if (previous !== scope) scope.dispose();
      throw error;
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
        return continuousQuantileInPlace(this.values ?? [], 0.5);
      case "quantile":
        return continuousQuantileInPlace(this.values ?? [], this.quantile ?? 0.5);
      case "mode":
        return this.mode();
      case "approx_count_distinct":
        requireExactWindowAggregate(this.op);
        return null;
    }
  }

  private ledger(): MutableMemoryLedger | undefined {
    if (!(this.budget instanceof MemoryBudget || this.budget instanceof MemoryScope))
      return undefined;
    if (this.bufferLedger === undefined) {
      this.bufferScope =
        this.budget instanceof MemoryScope
          ? this.budget.childScope()
          : new MemoryScope(this.budget);
      // Drop all backing before the ledger releases admission on scope closure.
      this.bufferScope.signal.addEventListener(
        "abort",
        () => {
          this.values = undefined;
          this.modeValues = undefined;
          this.seenDistinct = undefined;
        },
        { once: true },
      );
      this.bufferLedger = new MutableMemoryLedger(this.bufferScope);
    }
    return this.bufferLedger;
  }

  private addQuantileValue(value: number): void {
    const ledger = this.ledger();
    if (ledger !== undefined) {
      ledger.append(
        (this.values === undefined ? charges.array : 0) + charges.arraySlot,
        "window quantile values",
        () => {
          this.values ??= [];
          this.values.push(value);
        },
      );
      return;
    }
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
    return this.withKey(value, this.seenDistinct === undefined, false, (key) => {
      this.seenDistinct ??= new Set();
      if (this.seenDistinct.has(key)) return false;
      this.seenDistinct.add(key);
      return true;
    });
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

  private addMode(value: Scalar, temporary?: MemoryScope): void {
    this.withKey(value, this.modeValues === undefined, true, (key) => {
      this.modeValues ??= new Map();
      const current = this.modeValues.get(key);
      if (current !== undefined) {
        current.count += 1;
        return false;
      }
      this.modeValues.set(key, { value: this.capture(value, temporary, true), count: 1 });
      return true;
    });
  }

  private withKey(
    value: Scalar,
    first: boolean,
    mode: boolean,
    insert: (key: string) => boolean,
  ): boolean {
    const entryBytes =
      charges.property +
      (first ? charges.object : 0) +
      (mode ? charges.object + 2 * charges.property : 0);
    const admissionBytes = jsonScalarKeyAllocationBytes(value) + entryBytes;
    const ledger = this.ledger();
    if (ledger !== undefined) {
      let added = false;
      let retainedBytes = 0;
      ledger.append(admissionBytes, "window aggregate JSON key", () => {
        const key = JSON.stringify(jsonSafeValue(value));
        added = insert(key);
        if (added)
          retainedBytes = charges.string + charges.stringCodeUnit * key.length + entryBytes;
      });
      ledger.release(admissionBytes - retainedBytes);
      return added;
    }
    let reservation = this.budget?.reserve(admissionBytes, "window aggregate JSON key");
    try {
      const key = JSON.stringify(jsonSafeValue(value));
      const added = insert(key);
      if (added && reservation !== undefined) {
        const retainedBytes = charges.string + charges.stringCodeUnit * key.length + entryBytes;
        reservation.shrink(retainedBytes);
        this.keyReservation =
          this.keyReservation === undefined ? reservation : this.keyReservation.merge(reservation);
        reservation = undefined;
      }
      return added;
    } finally {
      reservation?.release();
    }
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

function numericValue(op: AggregateOp, value: Scalar): number {
  return numericAggregateValue(op, value);
}

function compareScalar(left: Scalar, right: Scalar): number {
  return compareAggregateScalars(left, right);
}
