import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { BoundedTopK } from "./bounded-top-k.js";
import type { BufferMemory } from "./buffer-memory.js";
import { LakeqlError } from "./errors.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";

interface Entry<T> {
  owner: OwnedMemory<T>;
  scope: MemoryScope;
}

/** Stable bounded selection with independently owned candidate payloads.
 * Admission covers candidate replacement overlap and heap descriptors. */
export class OwnedBoundedTopK<T> {
  private readonly scope: MemoryScope;
  private readonly heap: BoundedTopK<Entry<T>>;
  private closed = false;

  constructor(
    parent: MemoryScope,
    limit: number,
    private readonly compare: (left: T, right: T) => number,
    private readonly memory?: BufferMemory<T>,
  ) {
    this.scope = parent.childScope();
    try {
      this.heap = this.scope.allocate(
        c.object + c.array + 4 * c.property,
        "owned top-k directory",
        () =>
          new BoundedTopK<Entry<T>>(limit, (left, right) =>
            compare(left.owner.value, right.owner.value),
          ),
      ).value;
    } catch (error) {
      this.scope.dispose();
      throw error;
    }
  }

  get size(): number {
    return this.heap.size;
  }

  /** Read-only check before detaching borrowed payload. Input and ordering keys
   * must stay stable until offer; offer still performs the authoritative check. */
  wouldAccept(value: T): boolean {
    this.requireOpen();
    if (this.heap.limit === 0) return false;
    if (this.heap.size < this.heap.limit) return true;
    try {
      const worst = this.heap.worst;
      return worst !== undefined && this.compare(value, worst.owner.value) < 0;
    } catch (error) {
      this.dispose();
      throw error;
    }
  }

  offer(input: OwnedMemory<T>): boolean {
    this.requireOpen();
    if (this.heap.limit === 0) return false;
    const candidate = this.scope.childScope();
    let accepted = false;
    try {
      const entry = candidate.allocate(
        2 * c.object + 4 * c.property + c.arraySlot,
        "owned top-k candidate",
        () => ({ owner: candidate.adopt(input.retainIn(candidate.budget)), scope: candidate }),
      ).value;
      const scratch = candidate.reserve(c.array + c.arraySlot, "top-k eviction result");
      try {
        const result = this.heap.add(entry);
        accepted = result.accepted;
        if (result.accepted) {
          for (const removed of result.removed) {
            this.memory?.remove(removed.owner.value);
            removed.scope.dispose();
          }
          this.memory?.add(input.value);
        }
        return accepted;
      } finally {
        scratch.release();
      }
    } catch (error) {
      // Comparator failure can interrupt a heap mutation; never expose partial state.
      this.dispose();
      throw error;
    } finally {
      if (!accepted) candidate.dispose();
    }
  }

  /** Winners remain valid after this selector and its parent are disposed. */
  finish(offset = 0, limit = this.heap.size): OwnedMemory<T[]> {
    this.requireOpen();
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(limit) || limit < 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid top-k output extent");
    const output = new MemoryScope(this.scope.budget);
    try {
      const count = this.heap.size;
      const selectedCount = Math.min(limit, Math.max(0, count - offset));
      const workspace = output.reserve(
        c.array + count * (c.arraySlot + c.object),
        "top-k final ordering workspace",
      );
      try {
        const values = output.allocate(
          c.array + selectedCount * c.arraySlot,
          "top-k ordered values",
          () => {
            const entries = this.heap.drainSorted();
            const values: T[] = [];
            for (let index = offset; index < offset + selectedCount; index++) {
              const entry = entries[index] as Entry<T>;
              values.push(output.adopt(entry.owner.retain()).value);
            }
            return values;
          },
        );
        return output.retainAllocations(values.value);
      } finally {
        workspace.release();
      }
    } finally {
      output.dispose();
      this.dispose();
    }
  }

  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    this.memory?.clear();
    this.heap.clear();
    this.scope.dispose();
  }

  private requireOpen(): void {
    if (this.closed || this.scope.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Owned top-k is disposed");
  }
}
