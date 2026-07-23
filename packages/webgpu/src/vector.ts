import type { PhysicalFragment, PhysicalVectorMetric } from "lakeql-core";

export interface CompiledWebGpuVectorTopK {
  readonly kind: "vector-top-k";
  readonly dimensions: number;
  readonly limit: number;
  readonly metric: PhysicalVectorMetric;
  readonly query: Float32Array;
  readonly tileRows: number;
  readonly outputWordsPerCandidate: 5;
  readonly outputBinding: 6;
  readonly wgsl: string;
  readonly cacheKey: string;
}

export type WebGpuVectorCompilation =
  | { supported: true; compiled: CompiledWebGpuVectorTopK }
  | { supported: false; reason: string };

const MAX_VECTOR_TILE_ROWS = 1024;
const MAX_TILE_COMPONENTS = 32_768;
const MAX_VECTOR_TOP_K = 32;

export function compileWebGpuVectorTopK(fragment: PhysicalFragment): WebGpuVectorCompilation {
  const distance = fragment.operators[0];
  const topK = fragment.operators[1];
  if (
    (fragment.input.kind !== "vector-candidates" &&
      fragment.input.kind !== "resident-vector-candidates") ||
    distance?.kind !== "vector-distance" ||
    topK?.kind !== "bounded-top-k" ||
    fragment.operators.length !== 2 ||
    fragment.output.kind !== "vector-candidates"
  ) {
    return {
      supported: false,
      reason:
        "WebGPU vector execution requires vector-candidates input, vector-distance -> bounded-top-k, and vector-candidates output",
    };
  }
  if (topK.limit > MAX_VECTOR_TOP_K) {
    return {
      supported: false,
      reason: `WebGPU bounded top-k supports at most ${MAX_VECTOR_TOP_K} candidates`,
    };
  }
  const query = Float32Array.from(distance.query);
  const tileRows = Math.max(
    1,
    Math.min(MAX_VECTOR_TILE_ROWS, Math.floor(MAX_TILE_COMPONENTS / fragment.input.dimensions)),
  );
  const wgsl = vectorTopKShader();
  return {
    supported: true,
    compiled: {
      kind: "vector-top-k",
      dimensions: fragment.input.dimensions,
      limit: topK.limit,
      metric: distance.metric,
      query,
      tileRows,
      outputWordsPerCandidate: 5,
      outputBinding: 6,
      wgsl,
      cacheKey: wgsl,
    },
  };
}

function vectorTopKShader(): string {
  return `struct Params {
  row_count: u32,
  dimensions: u32,
  tile_count: u32,
  limit: u32,
  metric: u32,
  tile_rows: u32,
}

@group(0) @binding(0)
var<storage, read> vectors: array<f32>;
@group(0) @binding(1)
var<storage, read> row_ids_low: array<u32>;
@group(0) @binding(2)
var<storage, read> row_ids_high: array<u32>;
@group(0) @binding(3)
var<storage, read> validity: array<u32>;
@group(0) @binding(4)
var<storage, read> query: array<f32>;
@group(0) @binding(5)
var<uniform> params: Params;
@group(0) @binding(6)
var<storage, read_write> output: array<u32>;

fn candidate_better(
  score: f32,
  index: u32,
  current_score: f32,
  current_index: u32,
  metric: u32,
) -> bool {
  let score_nan = score != score;
  let current_nan = current_score != current_score;
  if (score_nan != current_nan) {
    return !score_nan;
  }
  if (!score_nan && score != current_score) {
    return select((score < current_score), (score > current_score), metric == 0u);
  }
  return index < current_index;
}

fn vector_score(row: u32) -> f32 {
  var dot = 0.0f;
  var vector_norm = 0.0f;
  var query_norm = 0.0f;
  var l2 = 0.0f;
  let offset = row * params.dimensions;
  for (var dimension = 0u; dimension < params.dimensions; dimension += 1u) {
    let value = vectors[offset + dimension];
    let query_value = query[dimension];
    let product = value * query_value;
    dot = dot + product;
    if (params.metric == 1u) {
      let delta = value - query_value;
      l2 = l2 + delta * delta;
    } else if (params.metric == 2u) {
      vector_norm = vector_norm + value * value;
      query_norm = query_norm + query_value * query_value;
    }
  }
  if (params.metric == 0u) {
    return dot;
  }
  if (params.metric == 1u) {
    return l2;
  }
  if (vector_norm == 0.0f || query_norm == 0.0f) {
    return dot / min(vector_norm, query_norm);
  }
  return 1.0f - dot / sqrt(vector_norm * query_norm);
}

@compute @workgroup_size(1)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
  let tile = id.x;
  if (tile >= params.tile_count) {
    return;
  }
  var scores: array<f32, ${MAX_VECTOR_TOP_K}>;
  var indices: array<u32, ${MAX_VECTOR_TOP_K}>;
  var lows: array<u32, ${MAX_VECTOR_TOP_K}>;
  var highs: array<u32, ${MAX_VECTOR_TOP_K}>;
  var retained = 0u;
  let start = tile * params.tile_rows;
  let end = min(start + params.tile_rows, params.row_count);
  for (var row = start; row < end; row += 1u) {
    if (validity[row] == 0u) {
      continue;
    }
    let score = vector_score(row);
    if (retained < params.limit) {
      scores[retained] = score;
      indices[retained] = row;
      lows[retained] = row_ids_low[row];
      highs[retained] = row_ids_high[row];
      retained += 1u;
      continue;
    }
    var worst = 0u;
    for (var candidate = 1u; candidate < retained; candidate += 1u) {
      if (candidate_better(scores[worst], indices[worst], scores[candidate], indices[candidate], params.metric)) {
        worst = candidate;
      }
    }
    if (candidate_better(score, row, scores[worst], indices[worst], params.metric)) {
      scores[worst] = score;
      indices[worst] = row;
      lows[worst] = row_ids_low[row];
      highs[worst] = row_ids_high[row];
    }
  }
  for (var candidate = 0u; candidate < params.limit; candidate += 1u) {
    let valid = candidate < retained;
    let output_offset = (tile * params.limit + candidate) * 5u;
    output[output_offset] = bitcast<u32>(select(0.0f, scores[candidate], valid));
    output[output_offset + 1u] = select(0u, indices[candidate], valid);
    output[output_offset + 2u] = select(0u, lows[candidate], valid);
    output[output_offset + 3u] = select(0u, highs[candidate], valid);
    output[output_offset + 4u] = select(0u, 1u, valid);
  }
}`;
}

export function webGpuVectorMetricCode(metric: PhysicalVectorMetric): number {
  switch (metric) {
    case "dot":
      return 0;
    case "l2":
      return 1;
    case "cosine-distance":
      return 2;
  }
}
