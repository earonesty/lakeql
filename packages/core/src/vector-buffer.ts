/** Payload allocator shared by owned scopes and synchronous borrowed readers. */
export interface VectorAllocator {
  allocate<T>(bytes: number, purpose: string, allocate: () => T): { value: T };
}

export function admitVectorBuffer<T>(
  scope: VectorAllocator | undefined,
  bytes: number,
  allocate: () => T,
): T {
  return scope === undefined ? allocate() : scope.allocate(bytes, "vector backing", allocate).value;
}
