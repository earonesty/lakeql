import { throwIfAborted } from "./store.js";

/** Check cancellation after asynchronous advancement, including empty completion.
 * An owned source must provide discard to release values rejected before handoff.
 * This waits for an in-flight next() to settle; it does not detach live I/O.
 */
export function checkedAsyncIterable<T>(
  source: AsyncIterable<T>,
  signal?: AbortSignal,
  discard?: (value: T) => void,
): AsyncIterable<T> {
  return signal === undefined ? source : checkedIteration(source, signal, discard);
}

async function* checkedIteration<T>(
  source: AsyncIterable<T>,
  signal: AbortSignal,
  discard?: (value: T) => void,
): AsyncIterable<T> {
  throwIfAborted(signal);
  for await (const value of source) {
    try {
      throwIfAborted(signal);
    } catch (error) {
      discard?.(value);
      throw error;
    }
    yield value;
    throwIfAborted(signal);
  }
  throwIfAborted(signal);
}
