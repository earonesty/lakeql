import { type AggregateExpr, type AggregateSpec, lit, type PhysicalFragment } from "lakeql";
import {
  type CompiledWebGpuPredicateExpression,
  compileWebGpuPredicateExpression,
  type WebGpuPredicateColumn,
} from "./predicate.js";

export interface WebGpuReductionAggregate {
  readonly alias: string;
  readonly op: "count" | "min" | "max";
  readonly column?: WebGpuPredicateColumn;
}

export interface WebGpuAggregateDefinition {
  readonly alias: string;
  readonly op: "count" | "min" | "max";
  readonly columnName?: string;
}

export type WebGpuAggregateDefinitions =
  | {
      supported: true;
      definitions: readonly WebGpuAggregateDefinition[];
      requiredColumns: readonly string[];
    }
  | { supported: false; reason: string };

export interface CompiledWebGpuReduction {
  readonly kind: "reduction";
  readonly columns: readonly WebGpuPredicateColumn[];
  readonly aggregates: readonly WebGpuReductionAggregate[];
  readonly outputBinding: 3;
  readonly outputWordsPerTile: number;
  readonly tileRows: number;
  readonly wgsl: string;
  readonly cacheKey: string;
}

export type WebGpuReductionCompilation =
  | { supported: true; compiled: CompiledWebGpuReduction }
  | { supported: false; reason: string };

const REDUCTION_TILE_ROWS = 1024;

export function compileWebGpuReduction(fragment: PhysicalFragment): WebGpuReductionCompilation {
  const reduce = fragment.operators.at(-1);
  const prefix = fragment.operators.slice(0, -1);
  if (
    reduce?.kind !== "reduce" ||
    fragment.output.kind !== "aggregate-snapshot" ||
    fragment.input.kind !== "batch" ||
    prefix.some((operator) => operator.kind !== "select") ||
    prefix.length > 1
  ) {
    return {
      supported: false,
      reason: "WebGPU reduction requires an optional select followed by reduce",
    };
  }
  const aggregateCompilation = compileWebGpuAggregateDefinitions(reduce.aggregates);
  if (!aggregateCompilation.supported) return aggregateCompilation;
  const { definitions, requiredColumns } = aggregateCompilation;
  const select = prefix[0];
  const predicate = compileWebGpuPredicateExpression(
    select?.kind === "select" ? select.predicate : lit(true),
    fragment.input.columns,
    requiredColumns,
  );
  if (!predicate.supported) return predicate;
  const aggregates = bindWebGpuAggregates(definitions, predicate.compiled.columns);
  const outputWordsPerTile = 1 + aggregates.length * 2;
  const wgsl = reductionShader(predicate.compiled, aggregates, outputWordsPerTile);
  return {
    supported: true,
    compiled: {
      kind: "reduction",
      columns: predicate.compiled.columns,
      aggregates,
      outputBinding: 3,
      outputWordsPerTile,
      tileRows: REDUCTION_TILE_ROWS,
      wgsl,
      cacheKey: wgsl,
    },
  };
}

export function compileWebGpuAggregateDefinitions(
  aggregates: AggregateSpec,
): WebGpuAggregateDefinitions {
  const aggregateEntries = Object.entries(aggregates);
  if (aggregateEntries.length === 0) {
    return { supported: false, reason: "WebGPU reduction requires at least one aggregate" };
  }
  const requiredColumns: string[] = [];
  const definitions: WebGpuAggregateDefinition[] = [];
  for (const [alias, aggregate] of aggregateEntries) {
    if (aggregate.op !== "count" && aggregate.op !== "min" && aggregate.op !== "max") {
      return {
        supported: false,
        reason: `Aggregate ${aggregate.op} is not supported by WebGPU reduction`,
      };
    }
    const columnName = aggregateColumn(aggregate);
    if ((aggregate.op === "min" || aggregate.op === "max") && columnName === undefined) {
      return {
        supported: false,
        reason: `Aggregate ${aggregate.op} requires a physical column`,
      };
    }
    if (aggregate.expr !== undefined && columnName === undefined) {
      return {
        supported: false,
        reason: `Aggregate ${aggregate.op} expression must be a direct column`,
      };
    }
    if (columnName !== undefined) requiredColumns.push(columnName);
    definitions.push({
      alias,
      op: aggregate.op,
      ...(columnName === undefined ? {} : { columnName }),
    });
  }
  return { supported: true, definitions, requiredColumns };
}

export function bindWebGpuAggregates(
  definitions: readonly WebGpuAggregateDefinition[],
  columns: readonly WebGpuPredicateColumn[],
): WebGpuReductionAggregate[] {
  const columnsByName = new Map(columns.map((column) => [column.name, column]));
  return definitions.map((definition) => {
    const column =
      definition.columnName === undefined ? undefined : columnsByName.get(definition.columnName);
    if (definition.columnName !== undefined && column === undefined) {
      throw new Error(`WebGPU reduction column ${definition.columnName} was not compiled`);
    }
    return {
      alias: definition.alias,
      op: definition.op,
      ...(column === undefined ? {} : { column }),
    };
  });
}

function reductionShader(
  predicate: CompiledWebGpuPredicateExpression,
  aggregates: readonly WebGpuReductionAggregate[],
  outputWordsPerTile: number,
): string {
  const declarations = aggregates
    .map((aggregate, index) => aggregateDeclarations(aggregate, index))
    .join("\n");
  const updates = aggregates
    .map((aggregate, index) => aggregateUpdate(aggregate, index))
    .join("\n");
  const writes = aggregates.map((aggregate, index) => aggregateWrite(aggregate, index)).join("\n");
  return `struct Params {
  row_count: u32,
  tile_count: u32,
}

@group(0) @binding(0)
var<storage, read> values: array<u32>;
@group(0) @binding(1)
var<storage, read> validity: array<u32>;
@group(0) @binding(2)
var<uniform> params: Params;
@group(0) @binding(3)
var<storage, read_write> partials: array<u32>;

@compute @workgroup_size(1)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
  let tile = id.x;
  if (tile >= params.tile_count) {
    return;
  }
  let row_count = params.row_count;
  let start = tile * ${REDUCTION_TILE_ROWS}u;
  let end = min(start + ${REDUCTION_TILE_ROWS}u, row_count);
  var selected_count = 0u;
${indent(declarations, 2)}
  for (var row = start; row < end; row += 1u) {
    let predicate_value = ${predicate.valueExpression};
    let predicate_valid = ${predicate.validExpression};
    if (!(predicate_valid && predicate_value)) {
      continue;
    }
    selected_count += 1u;
${indent(updates, 4)}
  }
  let output_offset = tile * ${outputWordsPerTile}u;
  partials[output_offset] = selected_count;
${indent(writes, 2)}
}`;
}

function aggregateDeclarations(aggregate: WebGpuReductionAggregate, index: number): string {
  if (aggregate.op === "count") return `var aggregate_${index}_count = 0u;`;
  const type = wgslColumnType(requiredColumn(aggregate));
  return `var aggregate_${index}_valid = false;
var aggregate_${index}_value = ${zeroValue(type)};`;
}

function aggregateUpdate(aggregate: WebGpuReductionAggregate, index: number): string {
  if (aggregate.op === "count") {
    if (aggregate.column === undefined) return `aggregate_${index}_count += 1u;`;
    return `if (${columnValid(aggregate.column)}) {
  aggregate_${index}_count += 1u;
}`;
  }
  const column = requiredColumn(aggregate);
  const value = columnValue(column);
  const compare = aggregate.op === "min" ? "<" : ">";
  return `if (${columnValid(column)}) {
  let aggregate_${index}_next = ${value};
  if (!aggregate_${index}_valid) {
    aggregate_${index}_valid = true;
    aggregate_${index}_value = aggregate_${index}_next;
  } else if (aggregate_${index}_next ${compare} aggregate_${index}_value) {
    aggregate_${index}_value = aggregate_${index}_next;
  }
}`;
}

function aggregateWrite(aggregate: WebGpuReductionAggregate, index: number): string {
  const offset = 1 + index * 2;
  if (aggregate.op === "count") {
    return `partials[output_offset + ${offset}u] = 1u;
partials[output_offset + ${offset + 1}u] = aggregate_${index}_count;`;
  }
  return `partials[output_offset + ${offset}u] = select(0u, 1u, aggregate_${index}_valid);
partials[output_offset + ${offset + 1}u] = ${columnBits(
    requiredColumn(aggregate),
    `aggregate_${index}_value`,
  )};`;
}

function aggregateColumn(aggregate: AggregateExpr): string | undefined {
  if (aggregate.expr?.kind === "column") return aggregate.expr.name;
  return aggregate.expr === undefined ? aggregate.column : undefined;
}

function requiredColumn(aggregate: WebGpuReductionAggregate): WebGpuPredicateColumn {
  if (aggregate.column === undefined) throw new Error(`Aggregate ${aggregate.alias} has no column`);
  return aggregate.column;
}

function columnIndex(column: WebGpuPredicateColumn): string {
  return `(${column.ordinal}u * row_count + row)`;
}

function columnValid(column: WebGpuPredicateColumn): string {
  return `(validity[${columnIndex(column)}] != 0u)`;
}

function columnValue(column: WebGpuPredicateColumn): string {
  const value = `values[${columnIndex(column)}]`;
  switch (column.shape) {
    case "f32":
      return `bitcast<f32>(${value})`;
    case "i32":
      return `bitcast<i32>(${value})`;
    case "bool":
    case "u32":
    case "u8":
      return value;
  }
}

function columnBits(column: WebGpuPredicateColumn, value: string): string {
  return column.shape === "f32" || column.shape === "i32" ? `bitcast<u32>(${value})` : value;
}

function wgslColumnType(column: WebGpuPredicateColumn): "f32" | "i32" | "u32" {
  if (column.shape === "f32" || column.shape === "i32") return column.shape;
  return "u32";
}

function zeroValue(type: "f32" | "i32" | "u32"): string {
  if (type === "f32") return "0.0f";
  if (type === "i32") return "0i";
  return "0u";
}

function indent(value: string, spaces: number): string {
  const prefix = " ".repeat(spaces);
  return value
    .split("\n")
    .map((line) => `${prefix}${line}`)
    .join("\n");
}
