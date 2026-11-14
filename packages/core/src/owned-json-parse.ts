import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";

/** Forecast JSON storage from UTF8 bytes or borrowed UTF16 text.
 * String payloads are bounded by input bytes/code units; punctuation bounds containers,
 * properties and slots. JSON.parse remains responsible for syntax validation.
 */
export function parseOwnedJson(
  bytes: Uint8Array | string,
  scope: MemoryScope,
): OwnedMemory<unknown> {
  let storage = c.object + bytes.length * c.stringCodeUnit;
  let quoted = false;
  let escaped = false;
  let scalar = false;
  for (let offset = 0; offset < bytes.length; offset++) {
    const byte = typeof bytes === "string" ? bytes.charCodeAt(offset) : (bytes[offset] ?? 0);
    if (quoted) {
      if (escaped) escaped = false;
      else if (byte === 92) escaped = true;
      else if (byte === 34) quoted = false;
      continue;
    }
    if (byte === 34) {
      quoted = true;
      scalar = false;
      storage += c.string + c.arraySlot;
    } else if (byte === 123 || byte === 91) {
      storage += c.object + c.array + c.arraySlot;
      scalar = false;
    } else if (byte === 58) {
      storage += c.property;
      scalar = false;
    } else if (byte === 44 || byte === 93 || byte === 125 || byte <= 32) {
      scalar = false;
    } else if (!scalar) {
      storage += c.number + c.arraySlot;
      scalar = true;
    }
  }
  const text =
    typeof bytes === "string"
      ? undefined
      : scope.reserve(c.string + bytes.length * c.stringCodeUnit, "JSON decode text");
  try {
    return scope
      .allocate(storage, "parsed JSON tree", () =>
        JSON.parse(typeof bytes === "string" ? bytes : new TextDecoder().decode(bytes)),
      )
      .retain();
  } finally {
    text?.release();
  }
}
