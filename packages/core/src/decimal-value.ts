import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope } from "./memory-scope.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";

/** Exact coefficient * 10^-scale. Scale never implies an expanded allocation.
 * Internal scalar contract; reader/operator integration is separate.
 */
export class DecimalValue {
  constructor(
    readonly coefficient: bigint,
    readonly scale: number,
  ) {
    validateParts(coefficient, scale);
    Object.freeze(this);
  }

  toString(): string {
    return decimalToString(this);
  }

  toJSON(): string {
    return this.toString();
  }

  [Symbol.toPrimitive](hint: string): string {
    if (hint === "string") return this.toString();
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Decimal requires explicit numeric operations");
  }
}

export function isDecimalValue(value: unknown): value is DecimalValue {
  return value instanceof DecimalValue;
}

export function requireDecimalValue(value: unknown): DecimalValue {
  if (!isDecimalValue(value))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing or invalid decimal vector value");
  return value;
}

export function decimalFromParts(
  coefficient: bigint,
  scale: number,
  scope?: MemoryScope,
): DecimalValue {
  validateParts(coefficient, scale);
  const allocate = () => new DecimalValue(coefficient, scale);
  return scope === undefined
    ? allocate()
    : scope.allocate(decimalValueAllocationBytes(coefficient), "decimal scalar backing", allocate)
        .value;
}

export function decimalValueAllocationBytes(coefficient: bigint): number {
  return c.object + 2 * c.property + c.bigint + decimalStringAllocationBytes(coefficient);
}

function validateParts(coefficient: bigint, scale: number): void {
  if (typeof coefficient !== "bigint" || !Number.isSafeInteger(scale) || scale < 0)
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Decimal requires a bigint coefficient and nonnegative safe integer scale",
    );
}

/** Bound raw digits, slices, mantissa, exponent and final output before formatting.
 * BigInt forecasting arithmetic and VM/GC internals remain outside these units.
 */
export function decimalFormatAllocationBytes(value: DecimalValue): number {
  return 8 * c.string + 6 * decimalStringAllocationBytes(value.coefficient) + 128;
}

/** Canonical spelling: fixed form for adjusted exponents -6 through 20, otherwise
 * scientific form. Trailing coefficient zeroes do not change the spelling.
 * Output length depends on coefficient digits, never on a huge scale gap.
 */
export function decimalToString(value: DecimalValue, scope?: MemoryScope): string {
  if (scope !== undefined)
    return scope.allocate(decimalFormatAllocationBytes(value), "decimal formatting", () =>
      decimalToString(value),
    ).value;
  if (value.coefficient === 0n) return "0";
  const raw = String(value.coefficient);
  const start = value.coefficient < 0n ? 1 : 0;
  const sign = start === 1 ? "-" : "";
  const exponent = raw.length - start - 1 - value.scale;
  let end = raw.length;
  while (end > start + 1 && raw.charCodeAt(end - 1) === 48) end--;
  const digits = raw.slice(start, end);
  if (exponent < -6 || exponent >= 21) {
    const mantissa = digits.length === 1 ? digits : `${digits[0]}.${digits.slice(1)}`;
    return `${sign}${mantissa}e${exponent >= 0 ? "+" : ""}${exponent}`;
  }
  if (exponent < 0) return `${sign}0.${"0".repeat(-exponent - 1)}${digits}`;
  const integerDigits = exponent + 1;
  return integerDigits >= digits.length
    ? `${sign}${digits}${"0".repeat(integerDigits - digits.length)}`
    : `${sign}${digits.slice(0, integerDigits)}.${digits.slice(integerDigits)}`;
}

/** Compare virtual aligned digits without creating powers of ten or zero padding.
 * Traversal and formatted workspace are bounded by the input coefficients.
 */
export function compareDecimalValues(
  left: DecimalValue,
  right: DecimalValue,
  scope?: MemoryScope,
): -1 | 0 | 1 {
  const direct = directDecimalOrder(left, right);
  if (direct !== undefined) return direct;
  const a = left.coefficient;
  const b = right.coefficient;
  const admission = scope?.reserve(
    decimalStringAllocationBytes(a) + decimalStringAllocationBytes(b),
    "decimal comparison digits",
  );
  try {
    const first = String(a);
    const second = String(b);
    return compareDecimalDigits(left, right, first, second);
  } finally {
    admission?.release();
  }
}

function directDecimalOrder(left: DecimalValue, right: DecimalValue): -1 | 0 | 1 | undefined {
  const a = left.coefficient;
  const b = right.coefficient;
  if (left.scale === right.scale || a === 0n || b === 0n || a < 0n !== b < 0n)
    return a < b ? -1 : a > b ? 1 : 0;
  return undefined;
}

function compareDecimalDigits(
  left: DecimalValue,
  right: DecimalValue,
  first: string,
  second: string,
): -1 | 0 | 1 {
  const start = left.coefficient < 0n ? 1 : 0;
  const firstExponent = first.length - start - left.scale;
  const secondExponent = second.length - start - right.scale;
  let order = firstExponent < secondExponent ? -1 : firstExponent > secondExponent ? 1 : 0;
  if (order === 0) {
    const length = Math.max(first.length, second.length);
    for (let index = start; index < length; index++) {
      const x = index < first.length ? first.charCodeAt(index) : 48;
      const y = index < second.length ? second.charCodeAt(index) : 48;
      if (x !== y) {
        order = x < y ? -1 : 1;
        break;
      }
    }
  }
  if (order === 0) return 0;
  return (left.coefficient < 0n ? -order : order) as -1 | 0 | 1;
}

/** One borrowed comparator per operation, with lazy fixed-operand text. Neither
 * the comparator nor its scalar can escape through the callback result. */
export function withPreparedDecimalComparison<T extends number | Uint8Array>(
  right: DecimalValue,
  scope: MemoryScope | undefined,
  consume: (compare: (left: DecimalValue) => number) => T,
): T {
  const state = scope?.reserve(2 * c.object + 4 * c.property, "prepared decimal comparison state");
  let text: string | undefined;
  let storage: MemoryReservation | undefined;
  try {
    return consume((left) => {
      const direct = directDecimalOrder(left, right);
      if (direct !== undefined) return direct;
      if (text === undefined) {
        storage = scope?.reserve(
          decimalStringAllocationBytes(right.coefficient),
          "prepared decimal digits",
        );
        text = String(right.coefficient);
      }
      const scratch = scope?.reserve(
        decimalStringAllocationBytes(left.coefficient),
        "decimal comparison digits",
      );
      try {
        return compareDecimalDigits(left, right, String(left.coefficient), text);
      } finally {
        scratch?.release();
      }
    });
  } finally {
    storage?.release();
    state?.release();
  }
}
