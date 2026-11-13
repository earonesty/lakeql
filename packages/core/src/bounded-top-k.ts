import { LakeqlError } from "./errors.js";

const REJECTED = Object.freeze({ accepted: false as const });

interface Candidate<T> {
  value: T;
  ordinal: number;
}

/** Stable bounded selection. The heap root is the worst retained candidate. */
export class BoundedTopK<T> {
  private readonly heap: Candidate<T>[] = [];
  private ordinal = 0;

  constructor(
    readonly limit: number,
    private readonly compare: (left: T, right: T) => number,
  ) {
    if (!Number.isSafeInteger(limit) || limit < 0) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "topK must be a non-negative safe integer", {
        limit,
      });
    }
  }

  get size(): number {
    return this.heap.length;
  }

  add(value: T): { accepted: false } | { accepted: true; removed: T[] } {
    if (this.limit === 0) return REJECTED;
    if (this.ordinal === Number.MAX_SAFE_INTEGER) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Top-k encounter ordinal exceeded safe integer range",
      );
    }
    const ordinal = this.ordinal++;
    const worst = this.heap[0];
    // A later tie cannot displace an earlier candidate. Avoid per-row wrappers
    // and result objects for the common full-heap rejection path.
    if (this.heap.length === this.limit && worst && !(this.compare(value, worst.value) < 0))
      return REJECTED;
    const candidate = { value, ordinal };
    if (this.heap.length < this.limit) {
      this.heap.push(candidate);
      for (let index = this.heap.length - 1; index > 0; ) {
        const parent = Math.floor((index - 1) / 2);
        if (this.order(this.heap[index] as Candidate<T>, this.heap[parent] as Candidate<T>) <= 0)
          break;
        const swapped = this.heap[index] as Candidate<T>;
        this.heap[index] = this.heap[parent] as Candidate<T>;
        this.heap[parent] = swapped;
        index = parent;
      }
      return { accepted: true, removed: [] };
    }
    this.heap[0] = candidate;
    for (let index = 0; ; ) {
      const left = index * 2 + 1;
      if (left >= this.heap.length) break;
      const right = left + 1;
      const child =
        right < this.heap.length &&
        this.order(this.heap[right] as Candidate<T>, this.heap[left] as Candidate<T>) > 0
          ? right
          : left;
      if (this.order(this.heap[index] as Candidate<T>, this.heap[child] as Candidate<T>) >= 0)
        break;
      const swapped = this.heap[index] as Candidate<T>;
      this.heap[index] = this.heap[child] as Candidate<T>;
      this.heap[child] = swapped;
      index = child;
    }
    return { accepted: true, removed: [(worst as Candidate<T>).value] };
  }

  /** Finalize selection; callers must not add more values after draining. */
  drainSorted(): T[] {
    this.heap.sort((left, right) => this.order(left, right));
    const values = this.heap.map((candidate) => candidate.value);
    this.heap.length = 0;
    return values;
  }

  /** Drop retained references without comparing or allocating a result. */
  clear(): void {
    this.heap.length = 0;
    this.ordinal = 0;
  }

  private order(left: Candidate<T>, right: Candidate<T>): number {
    return this.compare(left.value, right.value) || left.ordinal - right.ordinal;
  }
}
