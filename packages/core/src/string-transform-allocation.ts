import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope } from "./memory-scope.js";

/** Unicode full case mappings expand to at most three UTF-16 units per input
 * unit. Trimming and slicing cannot expand. Native VM scratch is not measured.
 */
export function transformStringWithAdmission(
  value: string,
  operation: "lower" | "upper" | "trim" | "substr",
  transform: (value: string) => string,
  scope?: MemoryScope,
): string {
  if (scope === undefined) return transform(value);
  const units = value.length * (operation === "lower" || operation === "upper" ? 3 : 1);
  return scope.allocate(c.string + c.stringCodeUnit * units, "string transform result", () =>
    transform(value),
  ).value;
}
