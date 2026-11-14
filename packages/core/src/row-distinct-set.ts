import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import { isIntervalValue } from "./interval.js";
import type { MemoryScope } from "./memory-scope.js";
import { MutableMemoryLedger } from "./mutable-memory-ledger.js";
import { stringifyOwnedStableData } from "./owned-checkpoint-encoding.js";
import { snapshotScalarWithAdmission } from "./owned-snapshot-rows.js";
import { serializedValueBytes } from "./serialized-size.js";
import { isTimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";

/** DISTINCT keys have a query lifetime; formatting has a single-row lifetime. */
export class RowDistinctSet {
  private readonly seen: Set<string>;
  private readonly ledger: MutableMemoryLedger;
  private serializedBytes = 2;

  constructor(
    private readonly scope: MemoryScope,
    private readonly check: (rows: number, bytes: number) => void,
  ) {
    this.seen = scope.allocate(c.object, "row distinct directory", () => new Set<string>()).value;
    this.ledger = new MutableMemoryLedger(scope);
  }

  add(row: Row): boolean {
    const scratch = this.scope.childScope();
    try {
      const active = scratch.allocate(
        c.object,
        "distinct normalization traversal",
        () => new Set<object>(),
      ).value;
      const normalized = normalize(row, scratch, active);
      const key = scratch.adopt(stringifyOwnedStableData(normalized, scratch)).value;
      if (this.seen.has(key)) return false;
      const bytes = this.serializedBytes + serializedValueBytes(key) + (this.seen.size ? 1 : 0);
      this.check(this.seen.size + 1, bytes);
      this.ledger.append(
        c.property + c.string + key.length * c.stringCodeUnit,
        "row distinct retained key",
        () => {
          this.seen.add(key);
          this.serializedBytes = bytes;
        },
      );
      return true;
    } finally {
      scratch.dispose();
    }
  }
}

/** Preserve jsonSafeValue's data semantics, admitting recursive conversion first. */
function normalize(value: unknown, scope: MemoryScope, active: Set<object>): unknown {
  // Check enumerable data before structural scalar guards read marker fields.
  if (value !== null && typeof value === "object") {
    for (const key in value) {
      if (Object.hasOwn(value, key)) dataProperty(value, key, scope);
    }
  }
  if (
    typeof value === "bigint" ||
    value instanceof Uint8Array ||
    isDecimalValue(value) ||
    isTimestampValue(value) ||
    isIntervalValue(value)
  )
    return snapshotScalarWithAdmission(value, scope);
  if (value === null || typeof value !== "object") return value;
  if (active.has(value))
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "DISTINCT requires acyclic data");
  const frame = scope.reserve(c.property, "distinct normalization frame");
  active.add(value);
  try {
    if (Array.isArray(value)) {
      const out = scope.allocate(
        c.array + value.length * c.arraySlot,
        "distinct normalized array",
        () => new Array<unknown>(value.length),
      ).value;
      for (let index = 0; index < value.length; index++) {
        if (!Object.hasOwn(value, index)) continue;
        out[index] = normalize(dataProperty(value, String(index), scope), scope, active);
      }
      return out;
    }
    const out = scope.allocate(
      c.object,
      "distinct normalized record",
      () => ({}) as Record<string, unknown>,
    ).value;
    for (const key in value) {
      if (!Object.hasOwn(value, key)) continue;
      const inner = normalize(dataProperty(value, key, scope), scope, active);
      scope.allocate(
        c.property + c.object + c.string + key.length * c.stringCodeUnit,
        "distinct normalized field",
        () => {
          Object.defineProperty(out, key, {
            value: inner,
            enumerable: true,
            configurable: true,
            writable: true,
          });
        },
      );
    }
    return out;
  } finally {
    active.delete(value);
    frame.release();
  }
}

function dataProperty(value: object, key: string, scope: MemoryScope): unknown {
  const scratch = scope.reserve(c.object, "distinct property descriptor");
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !("value" in descriptor))
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "DISTINCT requires data properties");
    return descriptor.value;
  } finally {
    scratch.release();
  }
}
