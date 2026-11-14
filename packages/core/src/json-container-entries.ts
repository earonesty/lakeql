import { LakeqlError } from "./errors.js";

/** Borrowed cursor fields remain valid until the next advance. Arrays use direct
 * indexing, including holes; objects enumerate without materializing a key array.
 */
export class JsonContainerCursor {
  key: string | undefined;
  value: unknown;
  private index = 0;
  private readonly array: unknown[] | undefined;
  private readonly keys: Generator<string> | undefined;

  constructor(private readonly container: object) {
    this.array = Array.isArray(container) ? container : undefined;
    this.keys = this.array === undefined ? ownKeys(container) : undefined;
  }

  advance(): boolean {
    if (this.array !== undefined) {
      if (this.index >= this.array.length) return false;
      this.value = this.array[this.index++];
      return true;
    }
    const next = (this.keys as Generator<string>).next();
    if (next.done) return false;
    this.key = next.value;
    this.value = (this.container as Record<string, unknown>)[this.key];
    if (this.key === "toJSON" && typeof this.value === "function")
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Custom JSON output cannot be forecast");
    return true;
  }
}

function* ownKeys(value: object): Generator<string> {
  for (const key in value) {
    if (Object.hasOwn(value, key)) yield key;
  }
}
