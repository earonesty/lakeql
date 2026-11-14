import {
  type IntervalValue,
  intervalStringAllocationBytes,
  intervalToString,
  isIntervalValue,
} from "./interval.js";
import type { MemoryReservation } from "./memory-budget.js";
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
    value: number | bigint | TimestampValue | IntervalValue,
    consume: (text: string) => T,
  ): T {
    if (this.scope !== undefined) {
      const bytes = isIntervalValue(value)
        ? intervalStringAllocationBytes(value)
        : scalarStringAllocationBytes(value);
      if (bytes > (this.reservation?.bytes ?? 0)) {
        this.dispose();
        this.reservation = this.scope.reserve(bytes, "JSON scalar formatting");
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
