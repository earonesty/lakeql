import { batchFromVectors, gt, type PhysicalFragment, physicalInputFromBatch } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { WebGpuPhysicalBackend } from "./backend.js";

describe("lakeql-webgpu workerd packaging", () => {
  it("plans and compiles without browser globals or native dependencies", async () => {
    expect((globalThis as Record<string, unknown>).WebSocketPair).toBeTypeOf("function");
    const batch = batchFromVectors({
      score: { type: "f32", values: Float32Array.of(0.25, 0.75) },
    });
    const fragment: PhysicalFragment = {
      id: "workerd-webgpu-contract",
      input: physicalInputFromBatch(batch),
      operators: [{ kind: "select", predicate: gt("score", 0.5) }],
      output: { kind: "selection" },
      estimates: {
        rowCount: 2,
        inputBytes: 8,
        outputBytes: 2,
        dispatchCount: 1,
      },
    };
    const backend = new WebGpuPhysicalBackend(() => {
      throw new Error("Workerd packaging test must not acquire a device");
    });
    expect(backend.assess(fragment, {}).supported).toBe(true);
    await expect(backend.compile(fragment)).resolves.toMatchObject({
      backendId: "webgpu",
      fragment,
    });
    backend.close();
  });
});
