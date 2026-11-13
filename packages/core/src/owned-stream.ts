import { isLakeqlError, LakeqlError } from "./errors.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";

export interface OwnedStreamOptions {
  maxBytes: number;
  /** Decoded length, when known; compressed wire lengths must not be supplied. */
  knownLength?: number;
  purpose?: string;
  metric?: string;
  /** Preflight capacity transferred into the first buffer. Caller releases on exit. */
  initialReservation?: MemoryReservation;
}

/** Reserves engine buffers before allocation; source chunks remain runtime-owned. */
export async function readOwnedStream(
  stream: ReadableStream<Uint8Array> | null,
  scope: MemoryScope,
  options: OwnedStreamOptions,
): Promise<OwnedMemory<Uint8Array>> {
  const { maxBytes, knownLength = 0, purpose = "stream buffer", metric = "stream bytes" } = options;
  if (
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 0 ||
    !Number.isSafeInteger(knownLength) ||
    knownLength < 0
  ) {
    await stream?.cancel().catch(() => undefined);
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Stream byte limits must be nonnegative safe integers",
    );
  }
  if (knownLength > maxBytes) {
    await stream?.cancel().catch(() => undefined);
    throw streamBudget(maxBytes, knownLength, metric);
  }
  if (stream === null) return scope.allocate(0, purpose, () => new Uint8Array());
  const reader = stream.getReader();
  const signal = scope.signal;
  const cancel = () => {
    void reader.cancel(signal.reason).catch(() => undefined);
  };
  signal.addEventListener("abort", cancel, { once: true });
  if (signal.aborted) cancel();
  let buffer: OwnedMemory<Uint8Array> | undefined;
  let bytes = 0;
  try {
    if (options.initialReservation !== undefined) {
      let reservation = options.initialReservation;
      if (reservation.bytes > maxBytes) throw streamBudget(maxBytes, reservation.bytes, metric);
      // A transport may learn a shorter decoded extent only after receiving headers.
      if (knownLength > 0 && knownLength < reservation.bytes) {
        const parts = reservation.partition([knownLength, reservation.bytes - knownLength]);
        const first = parts[0];
        parts[1]?.release();
        if (first === undefined)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing stream reservation");
        reservation = first;
      }
      const size = reservation.bytes;
      let scoped: OwnedMemory<Uint8Array>;
      try {
        scoped = scope.allocateReserved(reservation, () => new Uint8Array(size));
      } finally {
        reservation.release();
      }
      buffer = scoped.retain();
      scoped.release();
    } else if (knownLength > 0) buffer = allocateWorking(knownLength, scope, purpose);
    while (true) {
      const next = await reader.read();
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Stream scope disposed");
      if (next.done) break;
      const needed = bytes + next.value.byteLength;
      if (needed > maxBytes) throw streamBudget(maxBytes, needed, metric);
      if (needed > (buffer?.bytes ?? 0)) {
        const desired = Math.min(maxBytes, Math.max(needed, (buffer?.bytes ?? 0) * 2));
        let grown: OwnedMemory<Uint8Array>;
        try {
          grown = allocateWorking(desired, scope, purpose);
        } catch (error) {
          if (
            desired === needed ||
            !isLakeqlError(error) ||
            error.code !== "LAKEQL_BUDGET_EXCEEDED"
          )
            throw error;
          grown = allocateWorking(needed, scope, purpose);
        }
        if (buffer !== undefined) grown.value.set(buffer.value.subarray(0, bytes));
        buffer?.release();
        buffer = grown;
      }
      buffer?.value.set(next.value, bytes);
      bytes = needed;
    }
    if (buffer === undefined) return scope.allocate(0, purpose, () => new Uint8Array());
    // Return a view rather than allocating a second output buffer to trim spare capacity.
    // The ownership charge deliberately remains the full backing allocation size.
    const output = byteView(buffer, buffer.value.subarray(0, bytes));
    const adopted = scope.adopt(output);
    buffer = undefined;
    return adopted;
  } catch (error) {
    await reader.cancel(error).catch(() => undefined);
    if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Stream scope disposed");
    throw error;
  } finally {
    buffer?.release();
    signal.removeEventListener("abort", cancel);
    reader.releaseLock();
  }
}

function allocateWorking(
  bytes: number,
  scope: MemoryScope,
  purpose: string,
): OwnedMemory<Uint8Array> {
  const scoped = scope.allocate(bytes, purpose, () => new Uint8Array(bytes));
  const independent = scoped.retain();
  scoped.release();
  return independent;
}

export function byteView(
  owner: OwnedMemory<Uint8Array>,
  value: Uint8Array,
): OwnedMemory<Uint8Array> {
  return {
    value,
    bytes: owner.bytes,
    budget: owner.budget,
    retain: () => byteView(owner.retain(), value),
    retainIn: (budget) => byteView(owner.retainIn(budget), value),
    release: () => owner.release(),
  };
}

function streamBudget(limit: number, actual: number, metric: string): LakeqlError {
  return new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Stream exceeds decoded byte limit", {
    metric,
    limit,
    actual,
  });
}
