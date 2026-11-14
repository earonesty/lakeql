import {
  type DecimalValue,
  decimalFormatAllocationBytes,
  isDecimalValue,
} from "./decimal-value.js";
import {
  type IntervalValue,
  intervalStringAllocationBytes,
  intervalToString,
  isIntervalValue,
} from "./interval.js";
import type { MemoryReservation } from "./memory-budget.js";
import { reserveMemoryCapacity } from "./memory-capacity.js";
import type { MemoryScope } from "./memory-scope.js";
import { scalarStringAllocationBytes } from "./scalar-cast-allocation.js";
import { isTimestampValue, type TimestampValue } from "./timestamp.js";

/** One synchronous traversal reuses the largest admitted scalar workspace.
 * Consumers must not retain formatted text. Capacity is replaced only between
 * calls, after the previous string's last use, and released by the traversal.
 */
export class JsonScalarFormatter {
  private reservation: MemoryReservation | undefined;

  constructor(private readonly scope: MemoryScope | undefined) {}

  // biome-ignore lint/suspicious/noConfusingVoidType: consumers either measure or write, never retain text.
  consume<T extends number | void>(
    value: number | bigint | TimestampValue | IntervalValue | DecimalValue,
    consume: (text: string) => T,
  ): T {
    if (this.scope !== undefined) {
      const bytes = isDecimalValue(value)
        ? decimalFormatAllocationBytes(value)
        : isIntervalValue(value)
          ? intervalStringAllocationBytes(value)
          : scalarStringAllocationBytes(value);
      if (bytes > (this.reservation?.bytes ?? 0)) {
        try {
          this.reservation = reserveMemoryCapacity(
            this.scope,
            this.reservation,
            bytes,
            "JSON scalar formatting",
          );
        } catch (error) {
          this.dispose();
          throw error;
        }
      }
    }
    const text = isTimestampValue(value)
      ? value.toJSON()
      : isIntervalValue(value)
        ? intervalToString(value)
        : String(value);
    return consume(text);
  }

  dispose(): void {
    this.reservation?.release();
    this.reservation = undefined;
  }
}
