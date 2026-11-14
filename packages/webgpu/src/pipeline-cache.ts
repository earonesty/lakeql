import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryBudget,
  type MemoryReservation,
} from "lakeql";

/** Bounds retained pipeline references and their modeled source/key metadata.
 * Driver-owned compiled code is opaque and is not a portable byte measurement.
 */
export class PipelineCache<T> {
  private readonly entries = new Map<
    string,
    {
      value: Promise<T>;
      bytes: number;
      reservation?: MemoryReservation;
      detach?: () => void;
    }
  >();
  private bytes = 0;

  constructor(
    private readonly maxEntries: number,
    private readonly maxBytes: number,
  ) {
    if (![maxEntries, maxBytes].every((value) => Number.isSafeInteger(value) && value >= 0))
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Pipeline cache limits must be non-negative safe integers",
      );
  }

  has(key: string): boolean {
    return this.entries.has(key);
  }

  get(key: string, pool?: MemoryBudget): Promise<T> | undefined {
    const entry = this.entries.get(key);
    if (
      entry === undefined ||
      (pool !== undefined && entry.reservation?.budget !== pool.allocationPool)
    )
      return undefined;
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.value;
  }

  pin(key: string, budget: MemoryBudget): MemoryReservation | undefined {
    const reservation = this.entries.get(key)?.reservation;
    if (reservation === undefined || !reservation.budget.sharesPoolWith(budget)) return undefined;
    return reservation.retainIn(budget);
  }

  set(
    key: string,
    source: string,
    value: Promise<T>,
    pool?: MemoryBudget,
    backing?: MemoryReservation,
  ): void {
    const bytes = pipelineMetadataBytes(key, source);
    if (
      backing !== undefined &&
      (backing.bytes !== bytes || pool === undefined || !backing.budget.sharesPoolWith(pool))
    )
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Pipeline cache backing does not match its metadata",
      );
    this.delete(key);
    if (this.maxEntries === 0 || bytes > this.maxBytes) return;
    while (this.entries.size >= this.maxEntries || this.bytes + bytes > this.maxBytes) {
      const oldest = this.entries.entries().next().value;
      if (oldest === undefined) break;
      this.delete(oldest[0]);
    }
    const root = pool?.allocationPool;
    const reservation =
      root === undefined
        ? undefined
        : (backing?.retainIn(root) ?? root.tryReserve(bytes, "WebGPU pipeline cache metadata"));
    if (root !== undefined && reservation === undefined) return;
    const entry = {
      value,
      bytes,
      ...(reservation === undefined ? {} : { reservation }),
      ...(root === undefined ? {} : { detach: root.registerReclaimer(() => this.reclaim(root)) }),
    };
    this.entries.set(key, entry);
    this.bytes += bytes;
    void value.catch(() => {
      const entry = this.entries.get(key);
      if (entry?.value !== value) return;
      this.delete(key);
    });
  }

  private reclaim(pool: MemoryBudget): void {
    for (const [key, entry] of this.entries) {
      if (entry.reservation?.budget !== pool) continue;
      this.delete(key);
      return;
    }
  }

  private delete(key: string): void {
    const entry = this.entries.get(key);
    if (entry === undefined) return;
    this.entries.delete(key);
    this.bytes -= entry.bytes;
    entry.detach?.();
    entry.reservation?.release();
  }

  clear(): void {
    for (const key of this.entries.keys()) this.delete(key);
  }
}

export function pipelineMetadataBytes(key: string, source: string): number {
  return 2 * c.string + (key.length + source.length) * c.stringCodeUnit + c.object + 5 * c.property;
}
