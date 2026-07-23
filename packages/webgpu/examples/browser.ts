import { createLake, gt, httpStore } from "lakeql/fetch";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";
import { browserWebGpuRuntime } from "lakeql-webgpu/browser";

export async function queryScores() {
  if (navigator.gpu === undefined) {
    throw new Error("This browser does not expose WebGPU");
  }
  const runtime = browserWebGpuRuntime(navigator, { GPUBufferUsage, GPUMapMode });
  const webgpu = new WebGpuPhysicalBackend(() => runtime);
  const lake = createLake({
    store: httpStore({ baseUrl: "https://example.com/data/" }),
    physicalExecution: {
      backends: [webgpu],
      acceleratorPolicy: "auto",
      replayOnCpu: true,
    },
  });

  try {
    return await lake
      .path("scores.parquet")
      .select(["item_id", "score"])
      .where(gt("score", 0.5))
      .toArray();
  } finally {
    webgpu.close();
  }
}
