import type { WebGpuRuntime } from "./runtime.js";

export interface BrowserWebGpuSource {
  readonly gpu?: GPU;
}

export interface BrowserWebGpuConstants {
  readonly GPUBufferUsage: typeof GPUBufferUsage;
  readonly GPUMapMode: typeof GPUMapMode;
}

export function browserWebGpuRuntime(
  source: BrowserWebGpuSource,
  constants: BrowserWebGpuConstants,
): WebGpuRuntime {
  if (source.gpu === undefined) {
    throw new Error("WebGPU is not available in this browser context");
  }
  return {
    gpu: source.gpu,
    constants: {
      bufferUsage: constants.GPUBufferUsage,
      mapMode: constants.GPUMapMode,
    },
  };
}
