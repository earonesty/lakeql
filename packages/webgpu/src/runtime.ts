import { LakeqlError, PhysicalBackendExecutionError, throwIfAborted } from "lakeql";

export interface WebGpuConstants {
  readonly bufferUsage: {
    readonly MAP_READ: GPUBufferUsageFlags;
    readonly COPY_SRC: GPUBufferUsageFlags;
    readonly COPY_DST: GPUBufferUsageFlags;
    readonly STORAGE: GPUBufferUsageFlags;
    readonly UNIFORM: GPUBufferUsageFlags;
  };
  readonly mapMode: {
    readonly READ: GPUMapModeFlags;
  };
}

export interface WebGpuRuntime {
  readonly gpu: GPU;
  readonly constants: WebGpuConstants;
}

export type WebGpuRuntimeProvider = () => WebGpuRuntime | Promise<WebGpuRuntime>;

export interface WebGpuDeviceOptions {
  adapter?: GPURequestAdapterOptions;
  device?: GPUDeviceDescriptor;
}

export interface WebGpuDeviceLease {
  readonly runtime: WebGpuRuntime;
  readonly adapter: GPUAdapter;
  readonly device: GPUDevice;
  readonly generation: number;
}

export class WebGpuDeviceManager {
  readonly #provider: WebGpuRuntimeProvider;
  readonly #options: WebGpuDeviceOptions;
  #lease: WebGpuDeviceLease | undefined;
  #opening: Promise<WebGpuDeviceLease> | undefined;
  #generation = 0;
  #closed = false;
  #invalidators = new Set<(generation: number) => void>();
  #scopeTail: Promise<void> = Promise.resolve();

  constructor(provider: WebGpuRuntimeProvider, options: WebGpuDeviceOptions = {}) {
    this.#provider = provider;
    this.#options = options;
  }

  get generation(): number {
    return this.#generation;
  }

  onInvalidated(listener: (generation: number) => void): () => void {
    this.#invalidators.add(listener);
    return () => this.#invalidators.delete(listener);
  }

  async acquire(signal?: AbortSignal): Promise<WebGpuDeviceLease> {
    throwIfAborted(signal);
    if (this.#closed) {
      throw new LakeqlError("LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE", "WebGPU backend is closed");
    }
    if (this.#lease !== undefined) return this.#lease;
    this.#opening ??= this.#open();
    try {
      const lease = await this.#opening;
      throwIfAborted(signal);
      return lease;
    } finally {
      this.#opening = undefined;
    }
  }

  async scoped<T>(
    backendId: string,
    operation: (lease: WebGpuDeviceLease) => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    const release = await this.#acquireScope(signal);
    try {
      const lease = await this.acquire(signal);
      lease.device.pushErrorScope("internal");
      lease.device.pushErrorScope("out-of-memory");
      lease.device.pushErrorScope("validation");
      let value: T | undefined;
      let failure: unknown;
      try {
        value = await operation(lease);
        throwIfAborted(signal);
      } catch (error) {
        failure = error;
      }
      const errors: GPUError[] = [];
      for (let index = 0; index < 3; index += 1) {
        try {
          const error = await lease.device.popErrorScope();
          if (error !== null) errors.push(error);
        } catch (error) {
          failure ??= error;
        }
      }
      if (failure !== undefined || errors.length > 0) {
        if (errors.length === 0 && failure instanceof LakeqlError) throw failure;
        throw new PhysicalBackendExecutionError(
          backendId,
          errors.map((error) => error.message).join("; ") || errorMessage(failure),
          {
            replayable: lease.generation !== this.#generation || isDeviceLoss(failure),
            cause: failure,
            details: {
              deviceGeneration: lease.generation,
              currentDeviceGeneration: this.#generation,
              gpuErrors: errors.map((error) => error.message),
            },
          },
        );
      }
      return value as T;
    } finally {
      release();
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#lease?.device.destroy();
    this.#lease = undefined;
    this.#invalidate();
  }

  async #open(): Promise<WebGpuDeviceLease> {
    const runtime = await this.#provider();
    const adapter = await runtime.gpu.requestAdapter(this.#options.adapter);
    if (adapter === null) {
      throw new LakeqlError(
        "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
        "No compatible WebGPU adapter is available",
      );
    }
    const device = await adapter.requestDevice(this.#options.device);
    if (this.#closed) {
      device.destroy();
      throw new LakeqlError("LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE", "WebGPU backend is closed");
    }
    const lease: WebGpuDeviceLease = {
      runtime,
      adapter,
      device,
      generation: this.#generation,
    };
    this.#lease = lease;
    void device.lost.then((info) => {
      if (this.#lease?.device !== device) return;
      this.#lease = undefined;
      this.#invalidate();
      if (info.reason === "destroyed" && this.#closed) return;
    });
    return lease;
  }

  #invalidate(): void {
    this.#generation += 1;
    for (const listener of this.#invalidators) listener(this.#generation);
  }

  async #acquireScope(signal?: AbortSignal): Promise<() => void> {
    const previous = this.#scopeTail;
    let release!: () => void;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.#scopeTail = previous.then(() => current);
    try {
      await waitForScope(previous, signal);
      throwIfAborted(signal);
      return release;
    } catch (error) {
      void previous.then(release);
      throw error;
    }
  }
}

function waitForScope(previous: Promise<void>, signal?: AbortSignal): Promise<void> {
  if (signal === undefined) return previous;
  throwIfAborted(signal);
  return new Promise<void>((resolve, reject) => {
    const aborted = () => {
      signal.removeEventListener("abort", aborted);
      try {
        throwIfAborted(signal);
      } catch (error) {
        reject(error);
      }
    };
    signal.addEventListener("abort", aborted, { once: true });
    void previous.then(() => {
      signal.removeEventListener("abort", aborted);
      resolve();
    });
  });
}

function isDeviceLoss(value: unknown): boolean {
  return (
    value instanceof Error &&
    (value.name === "GPUDeviceLostInfo" || value.message.toLowerCase().includes("device lost"))
  );
}

function errorMessage(value: unknown): string {
  return value instanceof Error ? value.message : String(value ?? "WebGPU operation failed");
}
