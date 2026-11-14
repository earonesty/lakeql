import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { DecimalValue, decimalFromParts, decimalValueAllocationBytes } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";

export type ExactDecimalOperation = "add" | "subtract" | "multiply";

/** Exact arithmetic requires a caller-owned budget. Admit aligned coefficients,
 * powers, result backing and the scalar before evaluating any arithmetic.
 * Native BigInt implementation scratch/GC remains outside the explicit model.
 */
export function decimalArithmetic(
  operation: ExactDecimalOperation,
  left: DecimalValue,
  right: DecimalValue,
  scope: MemoryScope,
): DecimalValue {
  const a = left.coefficient;
  const b = right.coefficient;
  if (operation === "multiply" && (a === 0n || b === 0n)) return decimalFromParts(0n, 0, scope);
  if (operation !== "multiply" && b === 0n) return decimalFromParts(a, left.scale, scope);
  if (operation === "add" && a === 0n) return decimalFromParts(b, right.scale, scope);

  if (operation === "subtract" && a === 0n)
    return scope.allocate(
      decimalValueAllocationBytes(b),
      "decimal negation",
      () => new DecimalValue(-b, right.scale),
    ).value;

  const scale =
    operation === "multiply" ? left.scale + right.scale : Math.max(left.scale, right.scale);
  if (!Number.isSafeInteger(scale))
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Decimal result scale exceeds safe integer range");
  const firstDigits = coefficientDigits(a);
  const secondDigits = coefficientDigits(b);
  const digits =
    operation === "multiply"
      ? firstDigits + secondDigits
      : Math.max(firstDigits + (scale - left.scale), secondDigits + (scale - right.scale)) + 1;
  const outputBytes = c.object + 2 * c.property + c.bigint + c.string + 2 * (digits + 1);
  const workspaceBytes = 8 * (c.bigint + 2 * (digits + 1));
  const total = outputBytes + workspaceBytes;
  if (!Number.isSafeInteger(total))
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      "Decimal alignment exceeds addressable capacity",
      {
        limit: scope.budget.maxBytes,
        operation,
      },
    );
  const stage = scope.childStage(total, "exact decimal arithmetic");
  try {
    const output = stage.allocate(outputBytes, "exact decimal result", () => {
      if (operation === "multiply") return new DecimalValue(a * b, scale);
      const x = a * 10n ** BigInt(scale - left.scale);
      const y = b * 10n ** BigInt(scale - right.scale);
      return new DecimalValue(operation === "add" ? x + y : x - y, scale);
    });
    return scope.adopt(output.retain()).value;
  } finally {
    stage.dispose();
  }
}

function coefficientDigits(value: bigint): number {
  return (decimalStringAllocationBytes(value) - c.string) / c.stringCodeUnit - Number(value < 0n);
}
