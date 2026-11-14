import { iteratorReadableStream } from "./iterator-stream.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";

/** One queued output owner. Delivered chunks leave library accounting on the
 * next pull; callers retaining them provide their own lifetime accounting.
 */
export function ownedOutputStream<T>(
  iterator: AsyncIterator<T>,
  lifetime: MemoryScope,
  encode: (value: T) => OwnedMemory<Uint8Array>,
  framing: {
    start?: () => OwnedMemory<Uint8Array> | undefined;
    finish?: () => OwnedMemory<Uint8Array> | undefined;
  } = {},
): ReadableStream<Uint8Array> {
  let queued: OwnedMemory<Uint8Array> | undefined;
  let finished = false;
  let stopped = false;
  const release = () => {
    queued?.release();
    queued = undefined;
  };
  const producer: AsyncIterator<T> = {
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
      queued = framing.start?.();
      if (queued) controller.enqueue(queued.value);
    },
    async pull(controller) {
      release();
      if (!finished) {
        const next = await iterator.next();
        if (stopped) return;
        if (!next.done) {
          queued = encode(next.value);
          controller.enqueue(queued.value);
          return;
        }
        finished = true;
        queued = framing.finish?.();
        if (queued) {
          controller.enqueue(queued.value);
          return;
        }
      }
      stopped = true;
      lifetime.dispose();
      controller.close();
    },
  });
}
