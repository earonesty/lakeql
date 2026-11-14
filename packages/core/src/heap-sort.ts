/** In-place O(n log n) sort for dense mutable arrays; constant auxiliary storage.
 * Equal elements may reorder. The comparator must not mutate the input.
 */
export function heapSort<T>(
  values: { readonly length: number; [index: number]: T },
  compare: (a: T, b: T) => number,
): void {
  const sift = (root: number, size: number) => {
    for (;;) {
      let child = root * 2 + 1;
      if (child >= size) return;
      if (child + 1 < size && compare(values[child] as T, values[child + 1] as T) < 0) child++;
      if (compare(values[root] as T, values[child] as T) >= 0) return;
      const value = values[root] as T;
      values[root] = values[child] as T;
      values[child] = value;
      root = child;
    }
  };
  for (let root = Math.floor(values.length / 2) - 1; root >= 0; root--) sift(root, values.length);
  for (let size = values.length - 1; size > 0; size--) {
    const value = values[0] as T;
    values[0] = values[size] as T;
    values[size] = value;
    sift(0, size);
  }
}
