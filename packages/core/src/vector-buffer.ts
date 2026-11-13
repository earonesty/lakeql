import type { MemoryScope } from "./memory-scope.js";

export function admitVectorBuffer<T>(
  scope: MemoryScope | undefined,
  bytes: number,
  allocate: () => T,
): T {
  return scope === undefined ? allocate() : scope.allocate(bytes, "vector backing", allocate).value;
}
