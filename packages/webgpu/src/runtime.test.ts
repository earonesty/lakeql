import type { LakeqlError, PhysicalBackendExecutionError } from "lakeql-core";
import { describe, expect, it, vi } from "vitest";
import { WebGpuDeviceManager, type WebGpuRuntime } from "./runtime.js";

describe("WebGpuDeviceManager", () => {
  it("shares an acquired device, reports invalidation, and closes deterministically", async () => {
    const fixture = fakeRuntime();
    const manager = new WebGpuDeviceManager(() => fixture.runtime);
    const invalidated = vi.fn();
    manager.onInvalidated(invalidated);

    const first = await manager.acquire();
    const second = await manager.acquire();
    expect(first).toBe(second);
    expect(fixture.requestAdapter).toHaveBeenCalledOnce();

    manager.close();
    expect(fixture.destroy).toHaveBeenCalledOnce();
    expect(manager.generation).toBe(1);
    expect(invalidated).toHaveBeenCalledWith(1);
    await expect(manager.acquire()).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
    });
  });

  it("returns scoped values and drains all nested error scopes", async () => {
    const fixture = fakeRuntime();
    const manager = new WebGpuDeviceManager(() => fixture.runtime);
    await expect(manager.scoped("gpu", async () => 42)).resolves.toBe(42);
    expect(fixture.pushErrorScope.mock.calls.map(([scope]) => scope)).toEqual([
      "internal",
      "out-of-memory",
      "validation",
    ]);
    expect(fixture.popErrorScope).toHaveBeenCalledTimes(3);
    manager.close();
  });

  it("serializes complete scoped operations on a shared device", async () => {
    const fixture = fakeRuntime();
    const manager = new WebGpuDeviceManager(() => fixture.runtime);
    const firstGate = deferred<void>();
    const firstStarted = deferred<void>();
    const events: string[] = [];

    const first = manager.scoped("gpu", async () => {
      events.push("first:start");
      firstStarted.resolve();
      await firstGate.promise;
      events.push("first:end");
      return 1;
    });
    await firstStarted.promise;
    const second = manager.scoped("gpu", async () => {
      events.push("second:start");
      events.push("second:end");
      return 2;
    });

    await Promise.resolve();
    expect(events).toEqual(["first:start"]);
    expect(fixture.pushErrorScope).toHaveBeenCalledTimes(3);

    firstGate.resolve();
    await expect(Promise.all([first, second])).resolves.toEqual([1, 2]);
    expect(events).toEqual(["first:start", "first:end", "second:start", "second:end"]);
    expect(fixture.pushErrorScope).toHaveBeenCalledTimes(6);
    expect(fixture.popErrorScope).toHaveBeenCalledTimes(6);
    manager.close();
  });

  it("cancels a queued scope without allowing later work to bypass the active scope", async () => {
    const fixture = fakeRuntime();
    const manager = new WebGpuDeviceManager(() => fixture.runtime);
    const firstGate = deferred<void>();
    const firstStarted = deferred<void>();
    let thirdStarted = false;

    const first = manager.scoped("gpu", async () => {
      firstStarted.resolve();
      await firstGate.promise;
    });
    await firstStarted.promise;

    const controller = new AbortController();
    const second = manager.scoped("gpu", async () => "unreachable", controller.signal);
    controller.abort("stop");
    await expect(second).rejects.toMatchObject<LakeqlError>({ code: "LAKEQL_ABORTED" });

    const third = manager.scoped("gpu", async () => {
      thirdStarted = true;
    });
    await Promise.resolve();
    expect(thirdStarted).toBe(false);

    firstGate.resolve();
    await Promise.all([first, third]);
    expect(thirdStarted).toBe(true);
    manager.close();
  });

  it("turns validation scope errors into non-replayable backend failures", async () => {
    const fixture = fakeRuntime([{ message: "invalid binding" } as GPUError, null, null]);
    const manager = new WebGpuDeviceManager(() => fixture.runtime);
    await expect(
      manager.scoped("gpu", async () => "unpublished"),
    ).rejects.toMatchObject<PhysicalBackendExecutionError>({
      code: "LAKEQL_PHYSICAL_BACKEND_FAILURE",
      backendId: "gpu",
      replayable: false,
      message: "invalid binding",
    });
    manager.close();
  });

  it("marks failures replayable when the device is lost during work", async () => {
    const fixture = fakeRuntime();
    const manager = new WebGpuDeviceManager(() => fixture.runtime);
    await expect(
      manager.scoped("gpu", async () => {
        fixture.lose();
        await Promise.resolve();
        throw new Error("device lost during dispatch");
      }),
    ).rejects.toMatchObject<PhysicalBackendExecutionError>({
      backendId: "gpu",
      replayable: true,
    });
    expect(manager.generation).toBe(1);
    manager.close();
  });

  it("rejects missing adapters and cancellation without opening a device", async () => {
    const fixture = fakeRuntime();
    fixture.requestAdapter.mockResolvedValueOnce(null);
    const manager = new WebGpuDeviceManager(() => fixture.runtime);
    await expect(manager.acquire()).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
    });

    const controller = new AbortController();
    controller.abort();
    await expect(manager.acquire(controller.signal)).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_ABORTED",
    });
    manager.close();
  });
});

function fakeRuntime(scopeErrors: Array<GPUError | null> = [null, null, null]) {
  let loseDevice: ((value: GPUDeviceLostInfo) => void) | undefined;
  const lost = new Promise<GPUDeviceLostInfo>((resolve) => {
    loseDevice = resolve;
  });
  const destroy = vi.fn();
  const pushErrorScope = vi.fn();
  const popErrorScope = vi.fn(async () => scopeErrors.shift() ?? null);
  const device = {
    destroy,
    lost,
    popErrorScope,
    pushErrorScope,
  } as unknown as GPUDevice;
  const requestDevice = vi.fn(async () => device);
  const adapter = { requestDevice } as unknown as GPUAdapter;
  const requestAdapter = vi.fn(async () => adapter as GPUAdapter | null);
  const gpu = { requestAdapter } as unknown as GPU;
  const runtime = {
    gpu,
    constants: {
      bufferUsage: {
        MAP_READ: 1,
        COPY_SRC: 2,
        COPY_DST: 4,
        STORAGE: 8,
        UNIFORM: 16,
      },
      mapMode: { READ: 1 },
    },
  } satisfies WebGpuRuntime;
  return {
    runtime,
    requestAdapter,
    requestDevice,
    destroy,
    pushErrorScope,
    popErrorScope,
    lose() {
      loseDevice?.({ reason: "unknown", message: "lost" } as GPUDeviceLostInfo);
    },
  };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}
