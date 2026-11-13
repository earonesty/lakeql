/** Close a suspended producer when serialization fails as well as on cancellation.
 * Preserve the original stream failure if producer cleanup also fails.
 */
export function iteratorReadableStream<T>(
  iterator: AsyncIterator<unknown>,
  source: Pick<UnderlyingDefaultSource<T>, "start" | "pull">,
): ReadableStream<T> {
  let closing: Promise<unknown> | undefined;
  const close = () => {
    closing ??= Promise.resolve().then(() => iterator.return?.());
    return closing;
  };
  const protect = async (work: () => unknown) => {
    try {
      await work();
    } catch (error) {
      try {
        await close();
      } catch {
        /* The original failure remains authoritative. */
      }
      throw error;
    }
  };
  return new ReadableStream<T>({
    start: (controller) => protect(() => source.start?.(controller)),
    pull: (controller) => protect(() => source.pull?.(controller)),
    cancel: () => close().then(() => undefined),
  });
}
