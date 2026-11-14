import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { ownedOutputStream } from "./owned-output-stream.js";
import type { Row } from "./types.js";

export function jsonOutputStream(
  rows: AsyncIterable<Row>,
  budget: MemoryBudget,
  format: "json" | "ndjson",
): ReadableStream<Uint8Array> {
  const lifetime = new MemoryScope(budget);
  let first = true;
  const frame = (value: number) => {
    const scope = lifetime.childScope();
    try {
      return scope.allocate(c.view + 1, "JSON stream framing", () => Uint8Array.of(value)).retain();
    } finally {
      scope.dispose();
    }
  };
  return ownedOutputStream(
    rows[Symbol.asyncIterator](),
    lifetime,
    (row) => {
      const output = encodeOwnedJsonOutput(
        row,
        lifetime,
        format === "ndjson" ? { suffix: "\n" } : { prefix: first ? "" : "," },
      );
      first = false;
      return output;
    },
    format === "json" ? { start: () => frame(91), finish: () => frame(93) } : {},
  );
}
