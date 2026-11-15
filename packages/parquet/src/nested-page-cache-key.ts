import { jsonStringUtf8Bytes } from "lakeql-core";
import {
  buildDecodedCacheKey,
  type DecodedCacheKey,
  type DecodedCacheKeyPlan,
  decodedCacheKeyJsonCharacters,
  decodedCacheKeyPlan,
  decodedCacheKeyWorkspaceBytes,
  decodedCacheNumberCharacters,
} from "./decoded-column-cache.js";

export interface NestedPageKeyParts {
  shape: "dictionary" | "compact" | "leaf-slots";
  interpretation: string;
  parentKey: DecodedCacheKey;
  byteLength: number;
  etag?: string | undefined;
  path: readonly string[];
  start: number;
  end: number;
  previousDefinition?: number | undefined;
}

/** Defer optional serialization until the cache admits short-lived workspace.
 * Borrowed parts stay immutable for the page's lookup and insertion operations. */
export function planNestedPageCacheKey(parts: NestedPageKeyParts): DecodedCacheKeyPlan {
  let path = parts.path.length === 0 ? 2 : parts.path.length + 1;
  for (const field of parts.path) path += jsonStringUtf8Bytes(field);
  const characters =
    27 +
    jsonStringUtf8Bytes(parts.shape) +
    jsonStringUtf8Bytes(parts.interpretation) +
    decodedCacheKeyJsonCharacters(parts.parentKey) +
    decodedCacheNumberCharacters(parts.byteLength) +
    (parts.etag === undefined ? 4 : jsonStringUtf8Bytes(parts.etag)) +
    path +
    decodedCacheNumberCharacters(parts.start) +
    decodedCacheNumberCharacters(parts.end) +
    decodedCacheNumberCharacters(parts.previousDefinition);
  return decodedCacheKeyPlan(
    characters,
    () =>
      JSON.stringify([
        "nested-page-v3",
        parts.shape,
        parts.interpretation,
        buildDecodedCacheKey(parts.parentKey),
        parts.byteLength,
        parts.etag,
        parts.path,
        parts.start,
        parts.end,
        parts.previousDefinition,
      ]),
    decodedCacheKeyWorkspaceBytes(parts.parentKey),
  );
}
