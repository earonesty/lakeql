import { MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";

const defaultDecoder = new TextDecoder();

/** UTF-8 produces at most one UTF-16 code unit per input byte. Keep the
 * conservative string reservation with the metadata owner, including on reuse.
 */
export function decodeMetadataText(
  value: Uint8Array | undefined,
  scope?: MemoryScope,
  decoder: TextDecoder = defaultDecoder,
): string | undefined {
  if (value === undefined) return undefined;
  if (scope === undefined) return decoder.decode(value);
  const bytes =
    MATERIALIZATION_CHARGES.string + value.byteLength * MATERIALIZATION_CHARGES.stringCodeUnit;
  return scope.allocate(bytes, "decoded metadata text", () => decoder.decode(value)).value;
}
