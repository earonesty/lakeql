import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { encodeOwnedCsvOutput } from "./csv-output-encoding.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { ownedOutputStream } from "./owned-output-stream.js";
import type { Row } from "./types.js";

export function csvOutputStream(
  rows: AsyncIterable<Row>,
  budget: MemoryBudget,
  options: { columns?: string[]; header?: boolean },
): ReadableStream<Uint8Array> {
  const scope = new MemoryScope(budget);
  let columns = options.columns;
  let first = true;
  const header = options.header ?? true;
  return ownedOutputStream(
    rows[Symbol.asyncIterator](),
    scope,
    (row) => {
      if (columns === undefined) {
        let count = 0;
        for (const key in row) if (Object.hasOwn(row, key)) count++;
        columns = scope.allocate(c.array + count * c.arraySlot, "CSV inferred columns", () =>
          Object.keys(row),
        ).value;
      }
      const output = encodeOwnedCsvOutput(row, columns, first && header, scope);
      first = false;
      return output;
    },
    {
      finish: () =>
        first && header && columns !== undefined
          ? encodeOwnedCsvOutput(undefined, columns, true, scope)
          : undefined,
    },
  );
}
