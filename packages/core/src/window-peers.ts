import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryBudget, MemoryReservation } from "./memory-budget.js";
import { samePeer, type WindowRow } from "./window-utils.js";

interface PeerPosition {
  start: number;
  end: number;
  group: number;
}

/** One instance (eleven fields) and three cursor records (three fields each).
 * The partition and its keys are borrowed and charged by their existing owner.
 */
export const WINDOW_PEER_METADATA_BYTES = 4 * c.object + 20 * c.property;

/** Evaluation-local cursors over an immutable partition. Sequential traversal
 * uses constant auxiliary space. Repeated traversal can admit a compact index
 * when measured work exceeds four partition passes and a budget is available.
 */
export class WindowPeers {
  private current: PeerPosition;
  private first: PeerPosition;
  private last: PeerPosition;
  private count: number | undefined;
  private comparisons = 0;
  private index: Uint32Array | undefined;
  private reservation: MemoryReservation | undefined;
  private attemptedIndex = false;
  private metadata: MemoryReservation | undefined;
  constructor(
    private readonly partition: readonly WindowRow[],
    private readonly budget?: MemoryBudget,
  ) {
    // Instance fields and three reusable cursor records; partition storage is borrowed.
    this.metadata = budget?.reserve(WINDOW_PEER_METADATA_BYTES, "window peer metadata");
    this.current = { start: 0, end: 0, group: 0 };
    this.first = { start: 0, end: 0, group: 0 };
    this.last = { start: 0, end: 0, group: 0 };
  }

  dispose(): void {
    this.metadata?.release();
    this.metadata = undefined;
    this.index = undefined;
    this.reservation?.release();
    this.reservation = undefined;
    this.attemptedIndex = true;
  }

  private maybeIndex(): void {
    if (
      this.index !== undefined ||
      this.attemptedIndex ||
      this.budget === undefined ||
      this.partition.length === 0 ||
      this.comparisons <= this.partition.length * 4
    )
      return;
    this.attemptedIndex = true;
    const groups = this.groupCount;
    const reservation = this.budget.tryReserve(c.view + (groups + 1) * 4, "window peer index");
    if (reservation === undefined) return;
    try {
      const index = new Uint32Array(groups + 1);
      let group = 1;
      for (let row = 1; row < this.partition.length; row += 1) {
        if (!samePeer(this.partition[row - 1] as WindowRow, this.partition[row] as WindowRow))
          index[group++] = row;
      }
      index[groups] = this.partition.length;
      this.index = index;
      this.reservation = reservation;
    } catch (error) {
      reservation.release();
      throw error;
    }
  }

  private indexedGroup(group: number, cursor: PeerPosition): PeerPosition {
    const index = this.index;
    if (index === undefined) return cursor;
    cursor.group = Math.max(0, Math.min(group, index.length - 2));
    cursor.start = index[cursor.group] ?? 0;
    cursor.end = index[cursor.group + 1] ?? this.partition.length;
    return cursor;
  }

  peer(index: number): PeerPosition {
    this.maybeIndex();
    if (this.index !== undefined) {
      let low = 0;
      let high = this.index.length - 1;
      while (low + 1 < high) {
        const middle = Math.floor((low + high) / 2);
        if ((this.index[middle] ?? 0) <= index) low = middle;
        else high = middle;
      }
      return this.indexedGroup(low, this.current);
    }
    this.initialize(this.current);
    while (index >= this.current.end && this.current.end < this.partition.length)
      this.forward(this.current);
    while (index < this.current.start && this.current.start > 0) this.backward(this.current);
    return this.current;
  }

  get groupCount(): number {
    if (this.count === undefined) {
      let count = this.partition.length > 0 ? 1 : 0;
      for (let index = 1; index < this.partition.length; index += 1) {
        if (!samePeer(this.partition[index - 1] as WindowRow, this.partition[index] as WindowRow))
          count += 1;
      }
      this.count = count;
    }
    return this.count;
  }

  group(index: number, side: "start" | "end"): PeerPosition {
    this.maybeIndex();
    const cursor = side === "start" ? this.first : this.last;
    if (this.index !== undefined) return this.indexedGroup(index, cursor);
    this.initialize(cursor);
    while (cursor.group < index && cursor.end < this.partition.length) this.forward(cursor);
    while (cursor.group > index && cursor.start > 0) this.backward(cursor);
    return cursor;
  }

  private initialize(cursor: PeerPosition): void {
    if (cursor.end === 0 && this.partition.length > 0) {
      cursor.end = 1;
      while (cursor.end < this.partition.length && this.equal(cursor.start, cursor.end))
        cursor.end += 1;
    }
  }

  private forward(cursor: PeerPosition): void {
    cursor.start = cursor.end;
    cursor.end += 1;
    cursor.group += 1;
    while (cursor.end < this.partition.length && this.equal(cursor.start, cursor.end))
      cursor.end += 1;
  }

  private backward(cursor: PeerPosition): void {
    cursor.end = cursor.start;
    cursor.start -= 1;
    cursor.group -= 1;
    while (cursor.start > 0 && this.equal(cursor.start - 1, cursor.end - 1)) cursor.start -= 1;
  }

  private equal(left: number, right: number): boolean {
    this.comparisons += 1;
    return samePeer(this.partition[left] as WindowRow, this.partition[right] as WindowRow);
  }
}
