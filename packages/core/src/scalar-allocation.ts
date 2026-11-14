import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";

export function decimalStringAllocationBytes(value: bigint): number {
  // Count without allocating the decimal output before admission. BigInt arithmetic
  // and VM scratch are outside explicit reservation units, as in other forecasts.
  let digits = value < 0n ? 1 : 0;
  let remaining = value;
  const chunk = 1_000_000_000_000_000_000n;
  while (remaining >= chunk || remaining <= -chunk) {
    digits += 18;
    remaining /= chunk;
  }
  do {
    digits++;
    remaining /= 10n;
  } while (remaining !== 0n);
  return c.string + digits * c.stringCodeUnit;
}
