import type { Expr, PhysicalColumnShape, PhysicalFragment, PhysicalVectorShape } from "lakeql";

export interface WebGpuPredicateColumn {
  readonly name: string;
  readonly shape: "bool" | "f32" | "i32" | "u32" | "u8";
  readonly ordinal: number;
}

export interface CompiledWebGpuPredicate {
  readonly kind: "selection";
  readonly columns: readonly WebGpuPredicateColumn[];
  readonly outputBinding: number;
  readonly wgsl: string;
  readonly cacheKey: string;
  readonly valueExpression: string;
  readonly validExpression: string;
}

export type WebGpuPredicateCompilation =
  | { supported: true; compiled: CompiledWebGpuPredicate }
  | { supported: false; reason: string };

type KernelShape = WebGpuPredicateColumn["shape"];
type WgslShape = "bool" | "f32" | "i32" | "u32";

interface ScalarCode {
  value: string;
  valid: string;
  shape: WgslShape | "null";
}

interface BoolCode {
  value: string;
  valid: string;
}

class PredicateCompileError extends Error {}

export function compileWebGpuPredicate(fragment: PhysicalFragment): WebGpuPredicateCompilation {
  if (
    fragment.operators.length !== 1 ||
    fragment.operators[0]?.kind !== "select" ||
    fragment.output.kind !== "selection" ||
    fragment.input.kind !== "batch"
  ) {
    return {
      supported: false,
      reason: "WebGPU selection requires one select operator and selection output",
    };
  }
  try {
    const expression = compileWebGpuPredicateExpression(
      fragment.operators[0].predicate,
      fragment.input.columns,
    );
    if (!expression.supported) return expression;
    return { supported: true, compiled: selectionKernel(expression.compiled) };
  } catch (error) {
    return {
      supported: false,
      reason: error instanceof Error ? error.message : "Unsupported WebGPU predicate",
    };
  }
}

export interface CompiledWebGpuPredicateExpression {
  readonly columns: readonly WebGpuPredicateColumn[];
  readonly valueExpression: string;
  readonly validExpression: string;
}

export type WebGpuPredicateExpressionCompilation =
  | { supported: true; compiled: CompiledWebGpuPredicateExpression }
  | { supported: false; reason: string };

export function compileWebGpuPredicateExpression(
  expr: Expr,
  columns: Record<string, PhysicalColumnShape>,
  requiredColumns: readonly string[] = [],
): WebGpuPredicateExpressionCompilation {
  try {
    const compiler = new PredicateCompiler(columns);
    const compiled = compiler.compile(expr);
    for (const name of requiredColumns) compiler.includeColumn(name);
    return { supported: true, compiled };
  } catch (error) {
    return {
      supported: false,
      reason: error instanceof Error ? error.message : "Unsupported WebGPU predicate",
    };
  }
}

function selectionKernel(expression: CompiledWebGpuPredicateExpression): CompiledWebGpuPredicate {
  const outputBinding = 2;
  const wgsl = `struct SqlBool {
  value: bool,
  valid: bool,
}

@group(0) @binding(0)
var<storage, read> values: array<u32>;
@group(0) @binding(1)
var<storage, read> validity: array<u32>;
@group(0) @binding(${outputBinding})
var<storage, read_write> selection: array<u32>;

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
  let row = id.x;
  let row_count = arrayLength(&selection);
  if (row >= row_count) {
    return;
  }
  let predicate = SqlBool(${expression.valueExpression}, ${expression.validExpression});
  selection[row] = select(0u, 1u, predicate.valid && predicate.value);
}`;
  return {
    kind: "selection",
    columns: expression.columns,
    outputBinding,
    wgsl,
    cacheKey: wgsl,
    valueExpression: expression.valueExpression,
    validExpression: expression.validExpression,
  };
}

class PredicateCompiler {
  readonly #shapes: Record<string, PhysicalColumnShape>;
  readonly #columns: WebGpuPredicateColumn[] = [];
  readonly #columnByName = new Map<string, WebGpuPredicateColumn>();

  constructor(shapes: Record<string, PhysicalColumnShape>) {
    this.#shapes = shapes;
  }

  compile(expr: Expr): CompiledWebGpuPredicateExpression {
    const predicate = this.#bool(expr);
    return {
      columns: this.#columns,
      valueExpression: predicate.value,
      validExpression: predicate.valid,
    };
  }

  includeColumn(name: string): void {
    this.#column(name);
  }

  #bool(expr: Expr): BoolCode {
    switch (expr.kind) {
      case "literal": {
        if (expr.value === null) return { value: "false", valid: "false" };
        if (typeof expr.value !== "boolean") {
          throw new PredicateCompileError("Only boolean literals are predicates on WebGPU");
        }
        return { value: String(expr.value), valid: "true" };
      }
      case "column": {
        const scalar = this.#scalar(expr);
        if (scalar.shape !== "bool") {
          throw new PredicateCompileError(`Column ${expr.name} is not boolean`);
        }
        return { value: scalar.value, valid: scalar.valid };
      }
      case "compare": {
        const leftShape = expressionShape(expr.left, this.#shapes);
        const rightShape = expressionShape(expr.right, this.#shapes);
        const expected = commonShape(leftShape, rightShape);
        const left = this.#scalar(expr.left, expected);
        const right = this.#scalar(expr.right, expected);
        if (left.shape === "null" || right.shape === "null") {
          return { value: "false", valid: "false" };
        }
        if (left.shape !== right.shape) {
          throw new PredicateCompileError("WebGPU comparisons require matching physical shapes");
        }
        return {
          value: `(${left.value} ${compareOperator(expr.op)} ${right.value})`,
          valid: `(${left.valid} && ${right.valid})`,
        };
      }
      case "between": {
        return this.#and([
          this.#bool({
            kind: "compare",
            op: "gte",
            left: expr.target,
            right: expr.low,
          }),
          this.#bool({
            kind: "compare",
            op: "lte",
            left: expr.target,
            right: expr.high,
          }),
        ]);
      }
      case "in": {
        if (expr.values.length === 0) {
          return { value: String(expr.negated), valid: "true" };
        }
        const comparisons = expr.values.map((value) =>
          this.#bool({ kind: "compare", op: "eq", left: expr.target, right: value }),
        );
        const combined = this.#or(comparisons);
        return expr.negated ? { value: `!(${combined.value})`, valid: combined.valid } : combined;
      }
      case "null-check": {
        const scalar = this.#scalar(expr.target);
        return {
          value: expr.negated ? scalar.valid : `!(${scalar.valid})`,
          valid: "true",
        };
      }
      case "logical":
        return expr.op === "and"
          ? this.#and(expr.operands.map((operand) => this.#bool(operand)))
          : this.#or(expr.operands.map((operand) => this.#bool(operand)));
      case "not": {
        const operand = this.#bool(expr.operand);
        return { value: `!(${operand.value})`, valid: operand.valid };
      }
      case "arithmetic":
      case "call":
      case "case":
      case "like":
        throw new PredicateCompileError(`Predicate ${expr.kind} is not supported by WebGPU`);
    }
  }

  #and(operands: readonly BoolCode[]): BoolCode {
    const first = operands[0];
    if (first === undefined) throw new PredicateCompileError("AND requires operands");
    return operands.slice(1).reduce(
      (left, right) => ({
        value: `((${left.value}) && (${right.value}))`,
        valid: `((${left.valid} && ${right.valid}) || (${left.valid} && !(${left.value})) || (${right.valid} && !(${right.value})))`,
      }),
      first,
    );
  }

  #or(operands: readonly BoolCode[]): BoolCode {
    const first = operands[0];
    if (first === undefined) throw new PredicateCompileError("OR requires operands");
    return operands.slice(1).reduce(
      (left, right) => ({
        value: `((${left.value}) || (${right.value}))`,
        valid: `((${left.valid} && ${right.valid}) || (${left.valid} && ${left.value}) || (${right.valid} && ${right.value}))`,
      }),
      first,
    );
  }

  #scalar(expr: Expr, expected?: WgslShape): ScalarCode {
    if (expr.kind === "column") {
      const column = this.#column(expr.name);
      return {
        value:
          column.shape === "bool"
            ? `(${this.#columnValue(column)} != 0u)`
            : wgslColumnValue(column, this.#columnValue(column)),
        valid: `(validity[${this.#columnIndex(column)}] != 0u)`,
        shape: wgslShape(column.shape),
      };
    }
    if (expr.kind !== "literal") {
      throw new PredicateCompileError(`Scalar expression ${expr.kind} is not supported by WebGPU`);
    }
    if (expr.value === null) return { value: "0u", valid: "false", shape: "null" };
    if (expected === undefined) {
      if (typeof expr.value === "boolean") {
        return { value: String(expr.value), valid: "true", shape: "bool" };
      }
      throw new PredicateCompileError("Numeric WebGPU literals require a typed column operand");
    }
    return literalCode(expr.value, expected);
  }

  #column(name: string): WebGpuPredicateColumn {
    const existing = this.#columnByName.get(name);
    if (existing !== undefined) return existing;
    const shape = this.#shapes[name];
    if (shape === undefined) throw new PredicateCompileError(`Unknown column ${name}`);
    if (!isKernelShape(shape.shape)) {
      throw new PredicateCompileError(`Column ${name} has unsupported WebGPU shape ${shape.shape}`);
    }
    const column = {
      name,
      shape: shape.shape,
      ordinal: this.#columns.length,
    };
    this.#columns.push(column);
    this.#columnByName.set(name, column);
    return column;
  }

  #columnIndex(column: WebGpuPredicateColumn): string {
    return `(${column.ordinal}u * row_count + row)`;
  }

  #columnValue(column: WebGpuPredicateColumn): string {
    return `values[${this.#columnIndex(column)}]`;
  }
}

function expressionShape(
  expr: Expr,
  columns: Record<string, PhysicalColumnShape>,
): PhysicalVectorShape | "literal" | "null" {
  if (expr.kind === "column") return columns[expr.name]?.shape ?? "literal";
  if (expr.kind === "literal") return expr.value === null ? "null" : "literal";
  return "literal";
}

function commonShape(
  left: PhysicalVectorShape | "literal" | "null",
  right: PhysicalVectorShape | "literal" | "null",
): WgslShape | undefined {
  if (left === "null" || right === "null") return undefined;
  if (left === "literal" && right === "literal") return undefined;
  const physical = left === "literal" ? right : left;
  if (!isKernelShape(physical)) {
    throw new PredicateCompileError(`Physical shape ${physical} is not supported by WebGPU`);
  }
  return wgslShape(physical);
}

function isKernelShape(value: string): value is KernelShape {
  return (
    value === "bool" || value === "f32" || value === "i32" || value === "u32" || value === "u8"
  );
}

function wgslShape(shape: KernelShape): WgslShape {
  return shape === "u8" ? "u32" : shape;
}

function literalCode(value: unknown, shape: WgslShape): ScalarCode {
  if (shape === "bool") {
    if (typeof value !== "boolean") {
      throw new PredicateCompileError("Boolean columns require boolean literals");
    }
    return { value: String(value), valid: "true", shape };
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new PredicateCompileError("Numeric WebGPU literals must be finite numbers");
  }
  switch (shape) {
    case "f32":
      if (Math.fround(value) !== value) {
        throw new PredicateCompileError(
          "f32 predicate literals must be exactly representable to preserve CPU semantics",
        );
      }
      return { value: `${formatFloat(value)}f`, valid: "true", shape };
    case "i32":
      if (!Number.isInteger(value) || value < -0x8000_0000 || value > 0x7fff_ffff) {
        throw new PredicateCompileError("i32 predicate literal is out of range");
      }
      return { value: `${value}i`, valid: "true", shape };
    case "u32":
      if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
        throw new PredicateCompileError("u32 predicate literal is out of range");
      }
      return { value: `${value}u`, valid: "true", shape };
  }
}

function wgslColumnValue(column: WebGpuPredicateColumn, value: string): string {
  switch (column.shape) {
    case "f32":
      return `bitcast<f32>(${value})`;
    case "i32":
      return `bitcast<i32>(${value})`;
    case "u32":
    case "u8":
      return value;
    case "bool":
      return `(${value} != 0u)`;
  }
}

function formatFloat(value: number): string {
  if (Object.is(value, -0)) return "-0.0";
  return Number.isInteger(value) ? `${value}.0` : String(value);
}

function compareOperator(op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte"): string {
  switch (op) {
    case "eq":
      return "==";
    case "ne":
      return "!=";
    case "lt":
      return "<";
    case "lte":
      return "<=";
    case "gt":
      return ">";
    case "gte":
      return ">=";
  }
}
