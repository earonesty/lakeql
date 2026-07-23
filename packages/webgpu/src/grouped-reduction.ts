import { lit, type PhysicalFragment } from "lakeql";
import {
  type CompiledWebGpuPredicateExpression,
  compileWebGpuPredicateExpression,
  type WebGpuPredicateColumn,
} from "./predicate.js";
import {
  bindWebGpuAggregates,
  compileWebGpuAggregateDefinitions,
  type WebGpuReductionAggregate,
} from "./reduction.js";

export interface CompiledWebGpuGroupedReduction {
  readonly kind: "grouped-reduction";
  readonly columns: readonly WebGpuPredicateColumn[];
  readonly key: WebGpuPredicateColumn;
  readonly aggregates: readonly WebGpuReductionAggregate[];
  readonly maxGroups: number;
  readonly outputBinding: 3;
  readonly outputWordsPerGroup: number;
  readonly outputWordsPerTile: number;
  readonly tileRows: number;
  readonly wgsl: string;
  readonly cacheKey: string;
}

export type WebGpuGroupedReductionCompilation =
  | { supported: true; compiled: CompiledWebGpuGroupedReduction }
  | { supported: false; reason: string };

const GROUPED_REDUCTION_TILE_ROWS = 1024;
const MAX_GROUPS = 32;
const MAX_AGGREGATES = 16;

export function compileWebGpuGroupedReduction(
  fragment: PhysicalFragment,
): WebGpuGroupedReductionCompilation {
  const grouped = fragment.operators.at(-1);
  const prefix = fragment.operators.slice(0, -1);
  if (
    grouped?.kind !== "grouped-reduce" ||
    fragment.output.kind !== "grouped-aggregate-snapshot" ||
    fragment.input.kind !== "batch" ||
    prefix.some((operator) => operator.kind !== "select") ||
    prefix.length > 1
  ) {
    return {
      supported: false,
      reason: "WebGPU grouped reduction requires an optional select followed by grouped-reduce",
    };
  }
  if (grouped.keys.length !== 1) {
    return {
      supported: false,
      reason: "WebGPU grouped reduction requires exactly one physical key column",
    };
  }
  if (
    grouped.maxGroups === undefined ||
    !Number.isInteger(grouped.maxGroups) ||
    grouped.maxGroups < 1 ||
    grouped.maxGroups > MAX_GROUPS
  ) {
    return {
      supported: false,
      reason: `WebGPU grouped reduction requires maxGroups between 1 and ${MAX_GROUPS}`,
    };
  }
  const aggregateCompilation = compileWebGpuAggregateDefinitions(grouped.aggregates);
  if (!aggregateCompilation.supported) return aggregateCompilation;
  if (aggregateCompilation.definitions.length > MAX_AGGREGATES) {
    return {
      supported: false,
      reason: `WebGPU grouped reduction supports at most ${MAX_AGGREGATES} aggregates`,
    };
  }
  const keyName = grouped.keys[0];
  if (keyName === undefined) {
    return { supported: false, reason: "WebGPU grouped reduction key is missing" };
  }
  const requiredColumns = [keyName, ...aggregateCompilation.requiredColumns];
  const select = prefix[0];
  const predicate = compileWebGpuPredicateExpression(
    select?.kind === "select" ? select.predicate : lit(true),
    fragment.input.columns,
    requiredColumns,
  );
  if (!predicate.supported) return predicate;
  const key = predicate.compiled.columns.find((column) => column.name === keyName);
  if (key === undefined) {
    return { supported: false, reason: `WebGPU grouped reduction key ${keyName} is unavailable` };
  }
  const aggregates = bindWebGpuAggregates(
    aggregateCompilation.definitions,
    predicate.compiled.columns,
  );
  const outputWordsPerGroup = 3 + aggregates.length * 2;
  const outputWordsPerTile = 3 + grouped.maxGroups * outputWordsPerGroup;
  const wgsl = groupedReductionShader(
    predicate.compiled,
    key,
    aggregates,
    grouped.maxGroups,
    outputWordsPerGroup,
    outputWordsPerTile,
  );
  return {
    supported: true,
    compiled: {
      kind: "grouped-reduction",
      columns: predicate.compiled.columns,
      key,
      aggregates,
      maxGroups: grouped.maxGroups,
      outputBinding: 3,
      outputWordsPerGroup,
      outputWordsPerTile,
      tileRows: GROUPED_REDUCTION_TILE_ROWS,
      wgsl,
      cacheKey: wgsl,
    },
  };
}

function groupedReductionShader(
  predicate: CompiledWebGpuPredicateExpression,
  key: WebGpuPredicateColumn,
  aggregates: readonly WebGpuReductionAggregate[],
  maxGroups: number,
  outputWordsPerGroup: number,
  outputWordsPerTile: number,
): string {
  const declarations = aggregates
    .map((aggregate, index) => aggregateDeclarations(aggregate, index, maxGroups))
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
  let start = tile * ${GROUPED_REDUCTION_TILE_ROWS}u;
  let end = min(start + ${GROUPED_REDUCTION_TILE_ROWS}u, row_count);
  var selected_count = 0u;
  var group_count = 0u;
  var overflow = false;
  var key_valid: array<bool, ${maxGroups}>;
  var key_bits: array<u32, ${maxGroups}>;
${indent(declarations, 2)}
  for (var row = start; row < end; row += 1u) {
    let predicate_value = ${predicate.valueExpression};
    let predicate_valid = ${predicate.validExpression};
    if (!(predicate_valid && predicate_value)) {
      continue;
    }
    selected_count += 1u;
    let next_key_valid = ${columnValid(key)};
    let next_key_bits = ${columnBitsAt(key)};
    var group = ${maxGroups}u;
    for (var candidate = 0u; candidate < group_count; candidate += 1u) {
      if (key_valid[candidate] == next_key_valid &&
          (!next_key_valid || ${keyEquals(key, "key_bits[candidate]", "next_key_bits")})) {
        group = candidate;
        break;
      }
    }
    if (group == ${maxGroups}u) {
      if (group_count >= ${maxGroups}u) {
        overflow = true;
        continue;
      }
      group = group_count;
      group_count += 1u;
      key_valid[group] = next_key_valid;
      key_bits[group] = next_key_bits;
    }
${indent(updates, 4)}
  }
  let output_offset = tile * ${outputWordsPerTile}u;
  partials[output_offset] = selected_count;
  partials[output_offset + 1u] = group_count;
  partials[output_offset + 2u] = select(0u, 1u, overflow);
  for (var group = 0u; group < group_count; group += 1u) {
    let group_offset = output_offset + 3u + group * ${outputWordsPerGroup}u;
    partials[group_offset] = 1u;
    partials[group_offset + 1u] = select(0u, 1u, key_valid[group]);
    partials[group_offset + 2u] = key_bits[group];
${indent(writes, 4)}
  }
}`;
}

function aggregateDeclarations(
  aggregate: WebGpuReductionAggregate,
  index: number,
  maxGroups: number,
): string {
  if (aggregate.op === "count") return `var aggregate_${index}_count: array<u32, ${maxGroups}>;`;
  const type = wgslColumnType(requiredColumn(aggregate));
  return `var aggregate_${index}_valid: array<bool, ${maxGroups}>;
var aggregate_${index}_value: array<${type}, ${maxGroups}>;`;
}

function aggregateUpdate(aggregate: WebGpuReductionAggregate, index: number): string {
  if (aggregate.op === "count") {
    if (aggregate.column === undefined) return `aggregate_${index}_count[group] += 1u;`;
    return `if (${columnValid(aggregate.column)}) {
  aggregate_${index}_count[group] += 1u;
}`;
  }
  const column = requiredColumn(aggregate);
  const value = columnValue(column);
  const compare = aggregate.op === "min" ? "<" : ">";
  return `if (${columnValid(column)}) {
  let aggregate_${index}_next = ${value};
  if (!aggregate_${index}_valid[group]) {
    aggregate_${index}_valid[group] = true;
    aggregate_${index}_value[group] = aggregate_${index}_next;
  } else if (aggregate_${index}_next ${compare} aggregate_${index}_value[group]) {
    aggregate_${index}_value[group] = aggregate_${index}_next;
  }
}`;
}

function aggregateWrite(aggregate: WebGpuReductionAggregate, index: number): string {
  const offset = 3 + index * 2;
  if (aggregate.op === "count") {
    return `partials[group_offset + ${offset}u] = 1u;
partials[group_offset + ${offset + 1}u] = aggregate_${index}_count[group];`;
  }
  return `partials[group_offset + ${offset}u] = select(0u, 1u, aggregate_${index}_valid[group]);
partials[group_offset + ${offset + 1}u] = ${columnBits(
    requiredColumn(aggregate),
    `aggregate_${index}_value[group]`,
  )};`;
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

function columnBitsAt(column: WebGpuPredicateColumn): string {
  return `values[${columnIndex(column)}]`;
}

function columnValue(column: WebGpuPredicateColumn): string {
  const value = columnBitsAt(column);
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

function keyEquals(column: WebGpuPredicateColumn, left: string, right: string): string {
  if (column.shape !== "f32") return `${left} == ${right}`;
  const leftValue = `bitcast<f32>(${left})`;
  const rightValue = `bitcast<f32>(${right})`;
  const leftNaN = `((${left} & 0x7f800000u) == 0x7f800000u && (${left} & 0x007fffffu) != 0u)`;
  const rightNaN = `((${right} & 0x7f800000u) == 0x7f800000u && (${right} & 0x007fffffu) != 0u)`;
  return `((${leftValue} == ${rightValue}) || (${leftNaN} && ${rightNaN}))`;
}

function wgslColumnType(column: WebGpuPredicateColumn): "f32" | "i32" | "u32" {
  if (column.shape === "f32" || column.shape === "i32") return column.shape;
  return "u32";
}

function indent(value: string, spaces: number): string {
  const prefix = " ".repeat(spaces);
  return value
    .split("\n")
    .map((line) => `${prefix}${line}`)
    .join("\n");
}
