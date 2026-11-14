/** Decimal extent for an already validated safe integer; JSON writes -0 as 0. */
export function jsonIntegerBytes(value: number): number {
  let bytes = value < 0 ? 1 : 0;
  let remaining = Math.abs(value);
  do {
    bytes++;
    remaining = Math.trunc(remaining / 10);
  } while (remaining !== 0);
  return bytes;
}

/** Write a validated safe integer without a temporary formatted string. */
export function writeJsonInteger(value: number, buffer: Uint8Array, offset: number): number {
  const end = offset + jsonIntegerBytes(value);
  let cursor = end;
  let remaining = Math.abs(value);
  if (value < 0) buffer[offset] = 45;
  do {
    const quotient = Math.trunc(remaining / 10);
    buffer[--cursor] = 48 + (remaining - quotient * 10);
    remaining = quotient;
  } while (remaining !== 0);
  return end;
}
