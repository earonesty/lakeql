import { LakeqlError } from "./errors.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";

/** Collect an exact window while validating/draining the bounded decoded stream.
 * Only output storage is engine-owned; incoming chunks remain runtime-owned.
 */
export async function readOwnedStreamWindow(
  stream: ReadableStream<Uint8Array> | null,
  scope: MemoryScope,
  options: {
    offset: number;
    length: number;
    maxBytes: number;
    metric?: string;
    initialReservation?: MemoryReservation;
  },
): Promise<OwnedMemory<Uint8Array>> {
  const { offset, length, maxBytes } = options;
  if (
    ![offset, length, maxBytes, offset + length].every((n) => Number.isSafeInteger(n) && n >= 0)
  ) {
    await stream?.cancel().catch(() => undefined);
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid stream window");
  }
  let output: OwnedMemory<Uint8Array> | undefined;
  const reader = stream?.getReader();
  const signal = scope.signal;
  const cancel = () => {
    void reader?.cancel(signal.reason).catch(() => undefined);
  };
  signal.addEventListener("abort", cancel, { once: true });
  if (signal.aborted) cancel();
  try {
    if (offset + length > maxBytes)
      throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Stream window exceeds decoded byte limit");
    if (options.initialReservation !== undefined && options.initialReservation.bytes !== length)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Stream window reservation must match output length",
      );
    const scoped =
      options.initialReservation === undefined
        ? scope.allocate(length, "stream window", () => new Uint8Array(length))
        : scope.allocateReserved(options.initialReservation, () => new Uint8Array(length));
    output = scoped.retain();
    scoped.release();
    let consumed = 0;
    while (reader !== undefined) {
      const next = await reader.read();
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Stream scope disposed");
      if (next.done) break;
      const end = consumed + next.value.byteLength;
      if (!Number.isSafeInteger(end) || end > maxBytes)
        throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Stream exceeds decoded byte limit", {
          metric: options.metric ?? "stream bytes",
          limit: maxBytes,
          actual: end,
        });
      const startCopy = Math.max(offset, consumed);
      const endCopy = Math.min(offset + length, end);
      if (startCopy < endCopy)
        output.value.set(
          next.value.subarray(startCopy - consumed, endCopy - consumed),
          startCopy - offset,
        );
      consumed = end;
    }
    if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Stream scope disposed");
    if (consumed < offset + length)
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", "Stream ended before requested range");
    const result = scope.adopt(output);
    output = undefined;
    return result;
  } catch (error) {
    await reader?.cancel(error).catch(() => undefined);
    throw error;
  } finally {
    output?.release();
    signal.removeEventListener("abort", cancel);
    reader?.releaseLock();
  }
}
