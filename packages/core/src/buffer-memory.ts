/** Incremental serialized-list accounting, independent of the payload representation. */
export class BufferMemory<T> {
  private count = 0;
  private payloadBytes = 0;

  constructor(
    private readonly itemBytes: (value: T) => number,
    initial: Iterable<T> = [],
  ) {
    for (const value of initial) this.add(value);
  }

  get bytes(): number {
    return 2 + this.payloadBytes + Math.max(0, this.count - 1);
  }

  add(value: T): void {
    this.payloadBytes += this.itemBytes(value);
    this.count += 1;
  }

  remove(value: T): void {
    this.payloadBytes -= this.itemBytes(value);
    this.count -= 1;
  }

  clear(): void {
    this.count = 0;
    this.payloadBytes = 0;
  }
}
