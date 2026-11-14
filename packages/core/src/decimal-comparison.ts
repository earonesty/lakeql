import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { compareDecimalValues, DecimalValue, isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";

/** Compare mathematical numeric values: a number contributes its exact binary64
 * value, never its rounded display string. No decimal operand becomes Number. */
export function compareDecimalOperands(left: unknown, right: unknown, scope?: MemoryScope): number {
  if (isDecimalValue(left) && isDecimalValue(right))
    return compareDecimalValues(left, right, scope);
  if (isDecimalValue(left))
    return withDecimalComparisonOperand(right, scope, (value) =>
      compareDecimalValues(left, value, scope),
    );
  if (isDecimalValue(right)) {
    const order = withDecimalComparisonOperand(left, scope, (value) =>
      compareDecimalValues(right, value, scope),
    );
    return order === 0 ? 0 : -order;
  }
  throw invalidOperand();
}

const scalarBytes = c.object + 2 * c.property;

/** Synchronous borrowed operand. Conversion workspace remains admitted until
 * consume returns; its result cannot expose the temporary decimal scalar. */
export function withDecimalComparisonOperand<T extends number | Uint8Array>(
  value: unknown,
  scope: MemoryScope | undefined,
  consume: (value: DecimalValue) => T,
): T {
  if (isDecimalValue(value)) return consume(value);
  if (typeof value === "bigint") {
    const scratch = scope?.reserve(scalarBytes, "borrowed integer decimal comparison");
    try {
      return consume(new DecimalValue(value, 0));
    } finally {
      scratch?.release();
    }
  }
  if (typeof value !== "number" || !Number.isFinite(value)) throw invalidOperand();
  if (Number.isInteger(value)) {
    // BigInt(number) exactly decodes the represented integer, including integers
    // beyond MAX_SAFE_INTEGER. Binary64 integers need at most 1024 coefficient bits.
    const bytes = Number.isSafeInteger(value) ? 8 : 128;
    const scratch = scope?.reserve(scalarBytes + c.bigint + bytes, "integer decimal comparison");
    try {
      return consume(new DecimalValue(BigInt(value), 0));
    } finally {
      scratch?.release();
    }
  }
  const reader = scope?.reserve(c.object + c.view + 8, "binary64 comparison reader");
  try {
    const view = new DataView(new ArrayBuffer(8));
    view.setFloat64(0, value, false);
    const high = view.getUint32(0, false);
    const low = view.getUint32(4, false);
    const biasedExponent = (high >>> 20) & 2047;
    const exponent = biasedExponent === 0 ? -1074 : biasedExponent - 1023 - 52;
    const scale = -exponent; // Noninteger finite binary64 necessarily has exponent < 0.
    // 5^scale uses fewer than 3*scale bits; significand uses at most 53.
    // Admit bounded intermediate BigInts and the scalar before decoding any bits.
    const integerBytes = c.bigint + Math.ceil((53 + 3 * scale) / 8);
    const scratch = scope?.reserve(scalarBytes + 8 * integerBytes, "binary64 decimal comparison");
    try {
      const fraction = (BigInt(high & 0xfffff) << 32n) | BigInt(low);
      const significand = biasedExponent === 0 ? fraction : fraction | (1n << 52n);
      const magnitude = significand * 5n ** BigInt(scale);
      const coefficient = high >>> 31 ? -magnitude : magnitude;
      return consume(new DecimalValue(coefficient, scale));
    } finally {
      scratch?.release();
    }
  } finally {
    reader?.release();
  }
}

function invalidOperand(): LakeqlError {
  return new LakeqlError(
    "LAKEQL_TYPE_ERROR",
    "Decimal comparisons require finite numeric operands",
  );
}
