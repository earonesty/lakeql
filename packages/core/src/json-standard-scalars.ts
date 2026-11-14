import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope } from "./memory-scope.js";

/** Standard JSON data semantics without invoking arbitrary toJSON callbacks.
 * Inputs must remain immutable. User callbacks cannot be allocation-forecast;
 * callers needing them must use the legacy, externally accounted serializer.
 */
export class JsonStandardScalars {
  private dateWorkspace: MemoryReservation | undefined;

  constructor(private readonly scope: MemoryScope | undefined) {}

  normalize(value: unknown): unknown {
    if (value === null) return value;
    if (typeof value === "bigint")
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "BigInt is not standard JSON serializable");
    if (typeof value !== "object" && typeof value !== "function") return value;
    const toJSON = (value as { toJSON?: unknown }).toJSON;
    if (value instanceof Date && toJSON === Date.prototype.toJSON) {
      if (
        value.toISOString !== Date.prototype.toISOString ||
        value.valueOf !== Date.prototype.valueOf ||
        value[Symbol.toPrimitive] !== Date.prototype[Symbol.toPrimitive]
      )
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "Custom JSON date conversion cannot be forecast",
        );
      if (!Number.isFinite(Date.prototype.getTime.call(value))) return null;
      this.dateWorkspace ??= this.scope?.reserve(
        c.string + 27 * c.stringCodeUnit,
        "JSON date formatting",
      );
      return Date.prototype.toISOString.call(value);
    }
    if (typeof toJSON === "function")
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Custom JSON conversion cannot be forecast");
    if (value instanceof BigInt)
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "BigInt is not standard JSON serializable");
    if (value instanceof Boolean) return Boolean.prototype.valueOf.call(value);
    if (value instanceof Number || value instanceof String) {
      const primitive = (value as unknown as { [Symbol.toPrimitive]?: unknown })[
        Symbol.toPrimitive
      ];
      if (
        primitive !== undefined ||
        (value instanceof Number && value.valueOf !== Number.prototype.valueOf) ||
        (value instanceof String && value.toString !== String.prototype.toString)
      )
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "Custom JSON scalar conversion cannot be forecast",
        );
      return value instanceof Number
        ? Number.prototype.valueOf.call(value)
        : String.prototype.valueOf.call(value);
    }
    return value;
  }

  dispose(): void {
    this.dateWorkspace?.release();
    this.dateWorkspace = undefined;
  }
}
