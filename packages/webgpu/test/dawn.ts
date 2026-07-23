import { create } from "webgpu";

let adapterAvailability: Promise<boolean> | undefined;

/**
 * Dawn can be installed successfully on a machine that has no usable backend.
 * Probe adapter acquisition before running hardware-backed differential tests.
 */
export function dawnAdapterAvailable(): Promise<boolean> {
  adapterAvailability ??= probeDawnAdapter();
  return adapterAvailability;
}

async function probeDawnAdapter(): Promise<boolean> {
  try {
    return (await create([]).requestAdapter()) !== null;
  } catch {
    return false;
  }
}
