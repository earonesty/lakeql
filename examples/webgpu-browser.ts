import { createLake, gt, httpStore } from "../packages/lakeql/src/fetch.js";
import { WebGpuPhysicalBackend } from "../packages/webgpu/src/backend.js";
import { browserWebGpuRuntime } from "../packages/webgpu/src/browser.js";

export async function queryScoresWithWebGpu(baseUrl: string) {
  if (navigator.gpu === undefined) {
    throw new Error("This browser does not expose WebGPU");
  }

  const runtime = browserWebGpuRuntime(navigator, { GPUBufferUsage, GPUMapMode });
  const webgpu = new WebGpuPhysicalBackend(() => runtime);
  const lake = createLake({
    store: httpStore({ baseUrl }),
    physicalExecution: {
      backends: [webgpu],
      acceleratorPolicy: "auto",
      replayOnCpu: true,
    },
    budget: {
      maxOutputRows: 100,
      maxAcceleratorMemoryBytes: 64 * 1024 * 1024,
      maxAcceleratorUploadBytes: 64 * 1024 * 1024,
      maxAcceleratorReadbackBytes: 4 * 1024 * 1024,
      maxAcceleratorDispatches: 32,
    },
  });

  try {
    const result = lake
      .path("scores.parquet")
      .select(["item_id", "score"])
      .where(gt("score", 0.5))
      .limit(100)
      .run();

    return {
      rows: await result.toArray(),
      explain: await result.explain(),
    };
  } finally {
    webgpu.close();
  }
}
