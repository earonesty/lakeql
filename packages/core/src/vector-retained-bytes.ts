import type { Batch, Vector } from "./batch.js";
import { decimalValueAllocationBytes } from "./decimal-value.js";

/** Payload retained by a batch, including entire backing buffers of views.
 * JS object/array headers and allocator overhead are not measured here.
 */
export function retainedBatchBytes(batch: Batch): number {
  const measure = retainedMeasure();
  let bytes = 0;
  for (const vector of Object.values(batch.columns)) bytes += measure(vector);
  return bytes;
}

export function retainedVectorBytes(vector: Vector): number {
  return retainedMeasure()(vector);
}

function retainedMeasure(): (vector: Vector) => number {
  const buffers = new Set<ArrayBufferLike>();
  const vectors = new Set<Vector>();
  function backing(view: ArrayBufferView): number {
    if (buffers.has(view.buffer)) return 0;
    buffers.add(view.buffer);
    return view.buffer.byteLength;
  }
  function measure(vector: Vector): number {
    if (vectors.has(vector)) return 0;
    vectors.add(vector);
    let bytes = "valid" in vector && vector.valid !== undefined ? backing(vector.valid) : 0;
    switch (vector.type) {
      case "union":
        bytes += backing(vector.tags) + backing(vector.offsets);
        for (const child of vector.children) bytes += measure(child);
        return bytes;
      case "null":
        return bytes;
      case "f32":
      case "f64":
      case "i32":
      case "u32":
      case "u8":
      case "i64":
      case "u64":
      case "timestamp":
      case "bool":
        return bytes + backing(vector.values);
      case "timestamp96":
        return bytes + backing(vector.low) + backing(vector.high);
      case "decimal":
        for (const value of vector.values) bytes += decimalValueAllocationBytes(value.coefficient);
        return bytes;
      case "utf8":
        for (const value of vector.values) bytes += value.length * 2;
        return bytes;
      case "binary-view":
      case "utf8-view":
        return (
          bytes + backing(vector.data) + (vector.ranges === undefined ? 0 : backing(vector.ranges))
        );
      case "binary":
        for (const value of vector.values) bytes += backing(value);
        return bytes;
      case "dict":
        return bytes + backing(vector.indices) + measure(vector.dictionary);
      case "list":
        return bytes + backing(vector.offsets) + measure(vector.child);
      case "struct":
        for (const field of Object.values(vector.fields)) bytes += measure(field);
        return bytes;
      case "map":
        return bytes + backing(vector.offsets) + measure(vector.keys) + measure(vector.values);
    }
  }
  return measure;
}
