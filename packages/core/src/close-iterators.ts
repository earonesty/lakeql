/** Close every iterator and drain all cleanup before propagating the first
 * error in input order. Cleanup must never stop admitting remaining closers.
 */
export async function closeAsyncIterators(
  iterators: readonly Pick<AsyncIterator<unknown>, "return">[],
): Promise<void> {
  const results = await Promise.allSettled(
    iterators.map(async (iterator) => await iterator.return?.()),
  );
  for (const result of results) {
    if (result.status === "rejected") throw result.reason;
  }
}
