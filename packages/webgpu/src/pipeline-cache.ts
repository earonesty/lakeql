import { MATERIALIZATION_CHARGES as c, LakeqlError } from "lakeql";

/** Bounds retained pipeline references and their modeled source/key metadata.
 * Driver-owned compiled code is opaque and is not a portable byte measurement.
 */
export class PipelineCache<T> {
  private readonly entries = new Map<string, { value: Promise<T>; bytes: number }>();
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

  get(key: string): Promise<T> | undefined {
    const entry = this.entries.get(key);
    if (entry === undefined) return undefined;
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.value;
  }

  set(key: string, source: string, value: Promise<T>): void {
    const bytes =
      2 * c.string + (key.length + source.length) * c.stringCodeUnit + c.object + 3 * c.property;
    const previous = this.entries.get(key);
    if (previous !== undefined) {
      this.entries.delete(key);
      this.bytes -= previous.bytes;
    }
    if (this.maxEntries === 0 || bytes > this.maxBytes) return;
    while (this.entries.size >= this.maxEntries || this.bytes + bytes > this.maxBytes) {
      const oldest = this.entries.entries().next().value;
      if (oldest === undefined) break;
      this.entries.delete(oldest[0]);
      this.bytes -= oldest[1].bytes;
    }
    this.entries.set(key, { value, bytes });
    this.bytes += bytes;
    void value.catch(() => {
      const entry = this.entries.get(key);
      if (entry?.value !== value) return;
      this.entries.delete(key);
      this.bytes -= entry.bytes;
    });
  }

  clear(): void {
    this.entries.clear();
    this.bytes = 0;
  }
}
