import { LakeqlError } from "./errors.js";

export interface JsonContainerEntry {
  key: string | undefined;
  value: unknown;
}

/** Borrowed entry reused until the next iterator step. Consumers must finish
 * using it before advancing. No key array or cloned value tree is materialized.
 */
export function* jsonContainerEntries(value: object): Generator<JsonContainerEntry> {
  const entry: JsonContainerEntry = { key: undefined, value: undefined };
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) {
      entry.value = value[index];
      yield entry;
    }
  } else {
    for (const key in value) {
      if (!Object.hasOwn(value, key)) continue;
      const inner = (value as Record<string, unknown>)[key];
      if (key === "toJSON" && typeof inner === "function")
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Custom JSON output cannot be forecast");
      entry.key = key;
      entry.value = inner;
      yield entry;
    }
  }
}
