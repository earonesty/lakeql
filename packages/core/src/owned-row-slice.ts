import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { collectOwnedRows } from "./owned-row-collector.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import { validateRowLimits } from "./row-limits.js";
import type { Row } from "./types.js";

/** Collect a bounded interval from borrowed batches. Detach selected payload before
 * advancing or closing its producer; skipped and lookahead rows are never copied.
 * The caller owns the returned rows and must release them after use.
 */
export async function collectOwnedRowSlice(
  batches: AsyncIterable<Row[]>,
  budget: MemoryBudget,
  offset: number,
  limit: number,
  signal?: AbortSignal,
): Promise<{ rows: OwnedMemory<Row[]>; hasMore: boolean; seen: number }> {
  validateRowLimits({ offset, limit });
  let skipped = 0;
  let collected = 0;
  let hasMore = false;
  async function* selected(fail: (error: unknown) => never): AsyncIterable<OwnedMemory<Row[]>> {
    for await (const batch of batches) {
      const scope = new MemoryScope(budget);
      try {
        const start = Math.min(batch.length, offset - skipped);
        skipped += start;
        const count = Math.min(batch.length - start, limit - collected);
        hasMore = batch.length - start > count;
        if (count > 0) {
          const rows = scope.allocate(
            c.array + count * c.arraySlot,
            "slice row directory",
            () => new Array<Row>(count),
          ).value;
          for (let i = 0; i < count; i++)
            rows[i] = scope.adopt(detachOwnedRow(batch[start + i] as Row, budget)).value;
          collected += count;
          yield scope.retainAllocations(rows);
        }
        if (hasMore) return;
      } catch (error) {
        // Release both prior chunks and this partial copy before awaiting the
        // producer's potentially asynchronous return/remote cleanup.
        scope.dispose();
        fail(error);
      } finally {
        scope.dispose();
      }
    }
  }
  const rows = await collectOwnedRows(selected, budget, signal);
  return { rows, hasMore, seen: skipped + collected };
}
