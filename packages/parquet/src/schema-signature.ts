import {
  MATERIALIZATION_CHARGES as c,
  jsonStringUtf8Bytes,
  LakeqlError,
  type MemoryScope,
} from "lakeql-core";

/** Canonical schema JSON. Values are borrowed, immutable metadata during formatting. */
export function stableSchemaSignature(value: unknown, parent?: MemoryScope): string {
  const output = parent?.childScope();
  const scratch = output?.childScope();
  try {
    scratch?.allocate(2 * c.array + c.object, "schema signature directories", () => undefined);
    const chunks: string[] = [];
    const active = new Set<object>();
    const stack: {
      value: object;
      keys: string[] | undefined;
      index: number;
      length: number;
      scope: MemoryScope | undefined;
    }[] = [];
    let capacity = 0,
      length = 0;
    const append = (text: string) => {
      scratch?.allocate(c.arraySlot, "schema signature chunk slot", () => undefined);
      chunks.push(text);
      length += text.length;
    };
    const scalar = (item: unknown) => {
      const bytes = typeof item === "string" ? jsonStringUtf8Bytes(item) : 128;
      const create = () => JSON.stringify(item) ?? "";
      append(
        scratch === undefined
          ? create()
          : scratch.allocate(c.string + bytes * c.stringCodeUnit, "schema signature scalar", create)
              .value,
      );
    };
    const enter = (item: unknown) => {
      if (item === null || typeof item !== "object") {
        scalar(item);
        return;
      }
      if (active.has(item))
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic schema signature input");
      const scope = scratch?.childScope();
      scope?.allocate(c.object + 6 * c.property, "schema signature frame", () => undefined);
      let keys: string[] | undefined;
      if (!Array.isArray(item)) {
        const record = item as Record<string, unknown>;
        let count = 0,
          keyUnits = 0;
        for (const key in record)
          if (Object.hasOwn(record, key) && record[key] !== undefined) {
            count++;
            keyUnits += key.length;
          }
        scope?.allocate(
          c.array + count * (c.arraySlot + c.string) + keyUnits * c.stringCodeUnit,
          "schema signature keys",
          () => undefined,
        );
        keys = [];
        for (const key in record)
          if (Object.hasOwn(record, key) && record[key] !== undefined) keys.push(key);
        const sorting = scope?.reserve(
          c.array + count * 2 * c.arraySlot,
          "schema signature key ordering",
        );
        try {
          keys.sort();
        } finally {
          sorting?.release();
        }
      }
      if (stack.length === capacity) {
        scratch?.allocate(c.arraySlot, "schema signature stack slot", () => undefined);
        capacity++;
      }
      active.add(item);
      stack.push({
        value: item,
        keys,
        index: 0,
        length: keys?.length ?? (item as unknown[]).length,
        scope,
      });
      append(keys === undefined ? "[" : "{");
    };
    enter(value);
    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      if (frame === undefined) break;
      if (frame.index === frame.length) {
        append(frame.keys === undefined ? "]" : "}");
        active.delete(frame.value);
        stack.pop();
        frame.scope?.dispose();
        continue;
      }
      if (frame.index > 0) append(",");
      const index = frame.index++;
      if (frame.keys === undefined) enter((frame.value as unknown[])[index]);
      else {
        const key = frame.keys[index];
        if (key === undefined)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing schema signature key");
        scalar(key);
        append(":");
        enter((frame.value as Record<string, unknown>)[key]);
      }
    }
    const create = () => chunks.join("");
    const result =
      output === undefined
        ? create()
        : output.allocate(c.string + length * c.stringCodeUnit, "schema signature output", create)
            .value;
    if (output !== undefined && parent !== undefined) {
      const owner = output.retainAllocations(result);
      try {
        return parent.adopt(owner).value;
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return result;
  } finally {
    scratch?.dispose();
    output?.dispose();
  }
}
