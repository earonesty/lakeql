/** Explicit reservation units, not portable VM heap measurements. Array slots
 * include growth slack; properties include boxed storage/dictionary overhead.
 * Runtime/GC headroom and caller aliases require separate lifecycle controls.
 */
export const MATERIALIZATION_CHARGES = Object.freeze({
  object: 64,
  property: 64,
  array: 64,
  arraySlot: 16,
  string: 64,
  stringCodeUnit: 2,
  number: 16,
  bigint: 64,
  timestamp: 128,
  view: 128,
});
