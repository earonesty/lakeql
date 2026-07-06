const PROTOTYPE_MUTATION_KEYS = new Set(["__proto__", "constructor", "prototype"]);

export function isPrototypeMutationKey(key: string): boolean {
  return PROTOTYPE_MUTATION_KEYS.has(key);
}
