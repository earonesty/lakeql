import type { StoreAsyncBuffer } from "./types.js";

let nextReadIdentity = 0n;
const identities = new WeakMap<StoreAsyncBuffer, { etag: string | undefined; value: string }>();

/** ETags permit reuse across readers. Without one, cache products belong only
 * to this read observation, including wrappers around the same buffer. Weak
 * ownership avoids a retained per-path index. Unversioned identities are only
 * used in bounded in-memory caches. */
export function bufferCacheIdentity(file: StoreAsyncBuffer): string {
  const prior = identities.get(file);
  if (prior !== undefined && prior.etag === file.etag) return prior.value;
  const value =
    file.etag === undefined
      ? JSON.stringify(["read", (++nextReadIdentity).toString()])
      : JSON.stringify(["etag", file.etag]);
  identities.set(file, { etag: file.etag, value });
  return value;
}

export function inheritBufferCacheIdentity(
  source: StoreAsyncBuffer,
  target: StoreAsyncBuffer,
): StoreAsyncBuffer {
  identities.set(target, { etag: target.etag, value: bufferCacheIdentity(source) });
  return target;
}
