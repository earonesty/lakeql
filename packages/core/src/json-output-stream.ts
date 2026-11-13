import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { iteratorReadableStream } from "./iterator-stream.js";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import type { Row } from "./types.js";

/** Queue ownership ends on the next pull (consumer handoff) or cancellation.
 * Consumers retaining delivered byte arrays must provide their own accounting.
 */
export function jsonOutputStream(
  rows: AsyncIterable<Row>,
  budget: MemoryBudget,
  format: "json" | "ndjson",
): ReadableStream<Uint8Array> {
  const iterator = rows[Symbol.asyncIterator]();
  const lifetime = new MemoryScope(budget);
  let queued: OwnedMemory<Uint8Array> | undefined;
  let first = true;
  let finished = false;
  let stopped = false;
  const release = () => {
    queued?.release();
    queued = undefined;
  };
  const frame = (value: number) => {
    const scope = lifetime.childScope();
    try {
      return scope.allocate(c.view + 1, "JSON stream framing", () => Uint8Array.of(value)).retain();
    } finally {
      scope.dispose();
    }
  };
  const producer: AsyncIterator<Row> = {
    next: () => iterator.next(),
    async return() {
      stopped = true;
      try {
        return (await iterator.return?.()) ?? { done: true, value: undefined };
      } finally {
        release();
        lifetime.dispose();
      }
    },
  };
  return iteratorReadableStream<Uint8Array>(producer, {
    start(controller) {
      if (format === "json") {
        queued = frame(91);
        controller.enqueue(queued.value);
      }
    },
    async pull(controller) {
      release();
      if (finished) {
        stopped = true;
        lifetime.dispose();
        controller.close();
        return;
      }
      const next = await iterator.next();
      if (stopped) return;
      if (next.done) {
        finished = true;
        if (format === "json") {
          queued = frame(93);
          controller.enqueue(queued.value);
        } else {
          lifetime.dispose();
          controller.close();
        }
        return;
      }
      queued = encodeOwnedJsonOutput(
        next.value,
        lifetime,
        format === "ndjson" ? { suffix: "\n" } : { prefix: first ? "" : "," },
      );
      first = false;
      controller.enqueue(queued.value);
    },
  });
}
