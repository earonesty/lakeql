import { LakeqlError } from "./errors.js";

/** Ordered results with bounded pending work. On failure, stop starting work
 * and wait for active callbacks before rejecting, preserving their lifetimes.
 */
export async function mapConcurrent<T, R>(
  values: readonly T[],
  concurrency: number,
  map: (value: T, index: number) => Promise<R>,
): Promise<R[]> {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Concurrency must be a positive safe integer");
  }
  const output = new Array<R>(values.length);
  let next = 0;
  let failed = false;
  let failure: unknown;
  const worker = async () => {
    while (!failed && next < values.length) {
      const index = next++;
      try {
        output[index] = await map(values[index] as T, index);
      } catch (error) {
        if (!failed) {
          failed = true;
          failure = error;
        }
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, values.length) }, worker));
  if (failed) throw failure;
  return output;
}
