import { describe, expect, it } from "vitest";
import { browserWebGpuRuntime } from "./browser.js";

describe("browserWebGpuRuntime", () => {
  it("requires an injected GPU and passes through injected constants", () => {
    expect(() =>
      browserWebGpuRuntime(
        {},
        {
          GPUBufferUsage: { STORAGE: 1 } as typeof GPUBufferUsage,
          GPUMapMode: { READ: 2 } as typeof GPUMapMode,
        },
      ),
    ).toThrow("WebGPU is not available");

    const gpu = {} as GPU;
    const runtime = browserWebGpuRuntime(
      { gpu },
      {
        GPUBufferUsage: {
          MAP_READ: 1,
          COPY_SRC: 2,
          COPY_DST: 4,
          STORAGE: 8,
        } as typeof GPUBufferUsage,
        GPUMapMode: { READ: 1 } as typeof GPUMapMode,
      },
    );
    expect(runtime.gpu).toBe(gpu);
    expect(runtime.constants.bufferUsage.STORAGE).toBe(8);
  });
});
