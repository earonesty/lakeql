import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";

/** Native string-search replacement semantics, including $$, $&, $` and $'.
 * Count output and match-index workspace without materializing matches/results.
 */
export function replaceAllWithAdmission(
  value: string,
  search: string,
  replacement: string,
  scope?: MemoryScope,
): string {
  if (scope === undefined) return value.replaceAll(search, replacement);
  let literal = 0,
    matches = 0,
    prefixes = 0,
    suffixes = 0;
  for (let i = 0; i < replacement.length; i++) {
    if (replacement[i] === "$" && i + 1 < replacement.length) {
      const token = replacement[i + 1];
      if (token === "$" || token === "&" || token === "`" || token === "'") {
        i++;
        if (token === "$") literal++;
        else if (token === "&") matches++;
        else if (token === "`") prefixes++;
        else suffixes++;
        continue;
      }
    }
    literal++;
  }
  let units = value.length;
  let count = 0;
  const step = Math.max(1, search.length);
  for (let position = value.indexOf(search); position !== -1 && position <= value.length; ) {
    units +=
      literal +
      matches * search.length +
      prefixes * position +
      suffixes * (value.length - position - search.length) -
      search.length;
    count++;
    if (!Number.isSafeInteger(units) || !Number.isSafeInteger(count * c.arraySlot))
      throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "String replacement extent overflow");
    if (position + step > value.length) break;
    position = value.indexOf(search, position + step);
  }
  if (count === 0) return value;
  const bytes = c.string + c.stringCodeUnit * units + c.array + c.arraySlot * count;
  return scope.allocate(bytes, "string replacement result and workspace", () =>
    value.replaceAll(search, replacement),
  ).value;
}
