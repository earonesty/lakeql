import { LakeqlError } from "./errors.js";

interface Waiter {
  resume(): void;
  previous: Waiter | undefined;
  next: Waiter | undefined;
  queued: boolean;
}

/** FIFO read slots, transferred without a microtask-sized gap in the active count. */
export class ReadSemaphore {
  private active = 0;
  private head: Waiter | undefined;
  private tail: Waiter | undefined;

  constructor(private readonly limit: number) {
    if (!Number.isInteger(limit) || limit < 1) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "maxConcurrentReads must be a positive integer",
        { maxConcurrentReads: limit },
      );
    }
  }

  async acquire(signal?: AbortSignal): Promise<() => void> {
    if (signal?.aborted) throw aborted(signal);
    if (this.active < this.limit) {
      this.active += 1;
      return this.releaseHandle();
    }
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => signal?.removeEventListener("abort", abort);
      const waiter: Waiter = {
        previous: this.tail,
        next: undefined,
        queued: true,
        resume() {
          cleanup();
          resolve();
        },
      };
      const abort = () => {
        this.remove(waiter);
        cleanup();
        reject(aborted(signal));
      };
      if (this.tail === undefined) this.head = waiter;
      else this.tail.next = waiter;
      this.tail = waiter;
      signal?.addEventListener("abort", abort, { once: true });
    });
    // The slot was transferred to us before resolving. Cancellation at that
    // boundary must pass it onward, not strand the remaining queue.
    if (signal?.aborted) {
      this.release();
      throw aborted(signal);
    }
    return this.releaseHandle();
  }

  private releaseHandle(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.release();
    };
  }

  private release(): void {
    const next = this.head;
    if (next === undefined) {
      this.active -= 1;
      return;
    }
    this.remove(next);
    next.resume();
  }

  private remove(waiter: Waiter): void {
    if (!waiter.queued) return;
    if (waiter.previous === undefined) this.head = waiter.next;
    else waiter.previous.next = waiter.next;
    if (waiter.next === undefined) this.tail = waiter.previous;
    else waiter.next.previous = waiter.previous;
    waiter.previous = undefined;
    waiter.next = undefined;
    waiter.queued = false;
  }
}

function aborted(signal: AbortSignal | undefined): LakeqlError {
  return new LakeqlError("LAKEQL_ABORTED", "Query aborted", {
    reason: signal?.reason instanceof Error ? signal.reason.message : signal?.reason,
  });
}
