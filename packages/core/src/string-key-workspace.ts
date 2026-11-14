import { LakeqlError } from "./errors.js";
import { heapSort } from "./heap-sort.js";
import type { MemoryScope } from "./memory-scope.js";

export interface LastStringKeys {
  has(key: string): boolean;
  forEach(visit: (key: string, index: number) => void): void;
}

/** Reusable typed key-index workspace. Key payload stays in the source. Hash
 * chains are bounded; collision-heavy inputs switch to in-place heap sorting.
 * Nested callers hold separate buffers until their visitor completes.
 */
export class StringKeyWorkspace {
  private readonly buffers: Uint32Array[] = [];
  private depth = 0;
  private readonly scope: MemoryScope | undefined;

  constructor(scope?: MemoryScope) {
    this.scope = scope?.childScope();
  }

  dispose(): void {
    this.scope?.dispose();
    this.buffers.length = 0;
  }

  withKeys<T>(
    start: number,
    end: number,
    keyAt: (index: number) => string | null,
    visit: (keys: LastStringKeys) => T,
  ): T {
    const count = Math.max(0, end - start);
    if (
      !Number.isSafeInteger(start) ||
      !Number.isSafeInteger(end) ||
      start < 0 ||
      end < start ||
      end > 0xffffffff
    )
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid string-key index extent");
    let capacity = 1;
    while (capacity < count * 2) capacity *= 2;
    // Sorting uses one index per input and offers a smaller mandatory extent.
    const previous = this.buffers[this.depth];
    let hashing = count > 0 && capacity <= 0x80000000;
    if (
      this.scope !== undefined &&
      (previous?.length ?? 0) < capacity &&
      capacity * 4 > this.scope.availableAllocationBytes
    )
      hashing = false;
    const required = hashing ? capacity : count;
    let buffer = previous;
    if (buffer === undefined || buffer.length < required) {
      buffer =
        this.scope === undefined
          ? new Uint32Array(required)
          : this.scope.allocate(
              required * 4,
              "string-key index workspace",
              () => new Uint32Array(required),
            ).value;
      if (previous !== undefined) this.scope?.releaseAllocation(previous);
      this.buffers[this.depth] = buffer;
    }
    this.depth++;
    try {
      const indices = buffer;
      const compare = (a: number, b: number) => {
        const left = keyAt(a);
        const right = keyAt(b);
        return left === right
          ? a - b
          : left === null
            ? -1
            : right === null
              ? 1
              : left < right
                ? -1
                : 1;
      };
      if (hashing) {
        indices.fill(0, 0, capacity);
        outer: for (let index = start; index < end; index++) {
          const key = keyAt(index);
          if (key === null) continue;
          let slot = hashString(key) & (capacity - 1);
          for (let probes = 0; ; probes++, slot = (slot + 1) & (capacity - 1)) {
            if (probes === 8) {
              hashing = false;
              break outer;
            }
            const encoded = indices[slot] ?? 0;
            if (encoded === 0 || keyAt(encoded - 1) === key) {
              indices[slot] = index + 1;
              break;
            }
          }
        }
      }
      if (!hashing) {
        for (let i = 0; i < count; i++) indices[i] = start + i;
        heapSort(indices.subarray(0, count), compare);
      }
      const forEach = (callback: (key: string, index: number) => void) => {
        if (hashing) {
          for (let slot = 0; slot < capacity; slot++) {
            const encoded = indices[slot] ?? 0;
            if (encoded !== 0) {
              const index = encoded - 1;
              const key = keyAt(index);
              if (key !== null) callback(key, index);
            }
          }
        } else {
          for (let i = 0; i < count; i++) {
            const index = indices[i] ?? 0;
            const key = keyAt(index);
            if (key !== null && (i + 1 === count || keyAt(indices[i + 1] ?? 0) !== key))
              callback(key, index);
          }
        }
      };
      return visit({
        forEach,
        has(key) {
          if (hashing) {
            let slot = hashString(key) & (capacity - 1);
            for (let probe = 0; probe < 8; probe++, slot = (slot + 1) & (capacity - 1)) {
              const encoded = indices[slot] ?? 0;
              if (encoded === 0) return false;
              if (keyAt(encoded - 1) === key) return true;
            }
            return false;
          }
          let low = 0;
          let high = count;
          while (low < high) {
            const middle = Math.floor((low + high) / 2);
            const candidate = keyAt(indices[middle] ?? 0);
            if (candidate === key) return true;
            if (candidate === null || candidate < key) low = middle + 1;
            else high = middle;
          }
          return false;
        },
      });
    } finally {
      this.depth--;
    }
  }
}

function hashString(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i++) hash = Math.imul(hash ^ value.charCodeAt(i), 16777619);
  return hash >>> 0;
}
