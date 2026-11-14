import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Scalar } from "./expr.js";
import { intervalStringAllocationBytes, isIntervalValue } from "./interval.js";
import { jsonStringUtf8Bytes } from "./json-string-size.js";
import { scalarStringAllocationBytes } from "./scalar-cast-allocation.js";

/** Conservative simultaneous normalization and JSON key storage, before formatting.
 * Inputs are immutable scalars. Runtime/GC scratch is outside these charge units.
 * UTF8 length bounds UTF16 code units, including escaped lone surrogates.
 */
export function jsonScalarKeyAllocationBytes(value: Scalar): number {
  if (typeof value === "string") return c.string + c.stringCodeUnit * jsonStringUtf8Bytes(value);
  const formatting = isIntervalValue(value)
    ? intervalStringAllocationBytes(value)
    : scalarStringAllocationBytes(value);
  // Formatting storage plus a separately escaped JSON result (six characters
  // per input code unit is the maximum JSON escape expansion), including quotes.
  return formatting + c.string + 6 * formatting + 4;
}
