import type {
  Batch,
  CachePolicy,
  ObjectStoreCacheOptions,
  SharedMemoryCache,
  Vector,
} from "lakeql-core";

export class DecodedColumnCache {
  constructor(
    private readonly cache: SharedMemoryCache,
    private readonly options: ObjectStoreCacheOptions,
  ) {}

  get(key: string): Batch | undefined {
    const entry = this.cache.get<Batch>(decodedKey(key));
    if (entry === undefined) return undefined;
    return entry.value;
  }

  set(key: string, batch: Batch): void {
    this.cache.set(decodedKey(key), batch, estimateBatchBytes(batch), {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
  }

  getVector(key: string): Vector | undefined {
    const entry = this.cache.get<Vector>(decodedVectorKey(key));
    if (entry === undefined) return undefined;
    return entry.value;
  }

  setVector(key: string, vector: Vector): boolean {
    const bytes = estimateVectorBytes(vector);
    if (!this.shouldAdmitDecodedVector(bytes)) return false;
    this.cache.set(decodedVectorKey(key), vector, bytes, {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
    return true;
  }

  getValue<T>(key: string): T | undefined {
    const entry = this.cache.get<T>(decodedValueKey(key));
    if (entry === undefined) return undefined;
    return entry.value;
  }

  setValue<T>(key: string, value: T, bytes: number): void {
    if (!this.shouldAdmitDecodedWorkProduct(bytes)) return;
    this.cache.set(decodedValueKey(key), value, bytes, {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
  }

  private shouldAdmitDecodedVector(bytes: number): boolean {
    return this.shouldAdmitDecodedWorkProduct(bytes);
  }

  private shouldAdmitDecodedWorkProduct(bytes: number): boolean {
    const maxBytes = this.options.maxBytes ?? 64 * 1024 * 1024;
    const policy = this.options.policy ?? "balanced";
    if (policy === "latency") return bytes <= maxBytes;
    if (maxBytes < 128 * 1024 * 1024) return false;
    return bytes * 8 <= maxBytes;
  }
}

export function decodedColumnCacheKey(options: {
  path: string;
  byteLength: number;
  etag?: string;
  columns: readonly string[];
  rowStart: number;
  rowEnd: number;
}): string {
  return [
    options.path,
    options.byteLength,
    options.etag ?? "",
    options.rowStart,
    options.rowEnd,
    ...options.columns,
  ].join("\u001f");
}

export function decodedColumnPageCacheKey(options: {
  path: string;
  byteLength: number;
  etag?: string;
  column: string;
  rowGroupStart: number;
  pageRowStart: number;
  pageRowEnd: number;
  pageOffset: number;
  compressedPageSize: number;
}): string {
  return [
    options.path,
    options.byteLength,
    options.etag ?? "",
    options.column,
    options.rowGroupStart,
    options.pageRowStart,
    options.pageRowEnd,
    options.pageOffset,
    options.compressedPageSize,
  ].join("\u001f");
}

export function decodedDictionaryPageCacheKey(options: {
  path: string;
  byteLength: number;
  etag?: string;
  column: string;
  rowGroupStart: number;
  pageOffset: number;
  compressedPageSize: number;
  values: number;
}): string {
  return [
    options.path,
    options.byteLength,
    options.etag ?? "",
    options.column,
    options.rowGroupStart,
    options.pageOffset,
    options.compressedPageSize,
    options.values,
  ].join("\u001f");
}

function decodedKey(key: string): string {
  return `decoded-column:${key}`;
}

function decodedVectorKey(key: string): string {
  return `decoded-vector:${key}`;
}

function decodedValueKey(key: string): string {
  return `decoded-value:${key}`;
}

function decodedPriority(policy: CachePolicy): number {
  if (policy === "latency") return 4;
  if (policy === "io") return 1;
  return 2;
}

function estimateBatchBytes(batch: Batch): number {
  let bytes = 0;
  for (const vector of Object.values(batch.columns)) {
    bytes += estimateVectorBytes(vector);
  }
  return bytes;
}

function estimateVectorBytes(vector: Vector): number {
  let bytes = "valid" in vector && vector.valid !== undefined ? vector.valid.byteLength : 0;
  switch (vector.type) {
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
      return bytes + vector.values.byteLength;
    case "utf8":
      for (const value of vector.values) bytes += value.length * 2;
      return bytes;
    case "binary":
      for (const value of vector.values) bytes += value.byteLength;
      return bytes;
    case "dict":
      return bytes + vector.indices.byteLength + estimateVectorBytes(vector.dictionary);
    case "list":
      return bytes + vector.offsets.byteLength + estimateVectorBytes(vector.child);
    case "struct":
      for (const field of Object.values(vector.fields)) bytes += estimateVectorBytes(field);
      return bytes;
    case "map":
      return (
        bytes +
        vector.offsets.byteLength +
        estimateVectorBytes(vector.keys) +
        estimateVectorBytes(vector.values)
      );
  }
}
