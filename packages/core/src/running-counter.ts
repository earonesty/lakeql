import { LakeqlError } from "./errors.js";

/** Running retained usage. Owners charge additions and release removals; restoring
 * an owner initializes this once instead of rescanning it on every limit check.
 * Bytes describe the caller's metric, not an estimate of total runtime heap. */
export class RunningCounter {
  private retainedBytes = 0;
  private retainedItems = 0;

  get bytes(): number {
    return this.retainedBytes;
  }
  get items(): number {
    return this.retainedItems;
  }
  /** JSON list framing around individually measured serialized items. */
  get listBytes(): number {
    return RunningCounter.framedListBytes(this.retainedBytes, this.retainedItems);
  }

  static framedListBytes(payloadBytes: number, items: number): number {
    RunningCounter.checkedListBytes(payloadBytes);
    RunningCounter.checkedListBytes(items);
    return RunningCounter.checkedListBytes(2 + payloadBytes + Math.max(0, items - 1));
  }
  static appendListBytes(listBytes: number, itemsBefore: number, itemBytes: number): number {
    RunningCounter.checkedListBytes(listBytes);
    RunningCounter.checkedListBytes(itemsBefore);
    RunningCounter.checkedListBytes(itemBytes);
    return RunningCounter.checkedListBytes(
      itemsBefore === 0 ? 2 + itemBytes : listBytes + 1 + itemBytes,
    );
  }
  private static checkedListBytes(bytes: number): number {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Serialized list counter exceeds safe integer range",
      );
    return bytes;
  }

  add(bytes: number, items = 1): void {
    this.change(bytes, items, 1);
  }
  remove(bytes: number, items = 1): void {
    this.change(bytes, items, -1);
  }
  clear(): void {
    this.retainedBytes = this.retainedItems = 0;
  }

  private change(bytes: number, items: number, direction: number): void {
    const nextBytes = this.retainedBytes + direction * bytes;
    const nextItems = this.retainedItems + direction * items;
    if (
      !Number.isSafeInteger(bytes) ||
      bytes < 0 ||
      !Number.isSafeInteger(items) ||
      items < 0 ||
      !Number.isSafeInteger(nextBytes) ||
      nextBytes < 0 ||
      !Number.isSafeInteger(nextItems) ||
      nextItems < 0
    )
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid running counter adjustment");
    this.retainedBytes = nextBytes;
    this.retainedItems = nextItems;
  }
}
