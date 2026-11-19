import type { ObjectStore } from "../../packages/core/src/store.js";

interface Group {
  active: number;
  peak: number;
  waves: number;
}

/** A deterministic latency model: a turn completes all outstanding requests.
 * Dependent reads necessarily enter later waves. No wall-clock threshold or
 * public network dependency is needed in CI. Wrap a preloaded memory store.
 */
export function networkWaveStore(inner: ObjectStore, classify: (path: string) => string) {
  let pending: { release: () => void; group: string }[] = [];
  const trace = {
    waves: 0,
    active: 0,
    peak: 0,
    heads: [] as string[],
    requests: [] as { path: string; offset: number; length: number }[],
    groups: new Map<string, Group>(),
    group(name: string): Group {
      let group = this.groups.get(name);
      if (group === undefined) {
        group = { active: 0, peak: 0, waves: 0 };
        this.groups.set(name, group);
      }
      return group;
    },
    reset() {
      if (this.active !== 0) throw new Error("Cannot reset while reads are in flight");
      this.waves = this.peak = 0;
      this.requests = [];
      this.heads = [];
      this.groups.clear();
    },
  };
  const store: ObjectStore = {
    get: inner.get.bind(inner),
    put: inner.put.bind(inner),
    delete: inner.delete.bind(inner),
    list: inner.list.bind(inner),
    head: async (path) => {
      trace.heads.push(path);
      return await inner.head(path);
    },
    getRange: async (path, range) => {
      const name = classify(path);
      const group = trace.group(name);
      trace.requests.push({ path, ...range });
      trace.peak = Math.max(trace.peak, ++trace.active);
      group.peak = Math.max(group.peak, ++group.active);
      try {
        await new Promise<void>((release) => {
          pending.push({ release, group: name });
          if (pending.length === 1)
            setImmediate(() => {
              const batch = pending;
              pending = [];
              trace.waves++;
              for (const name of new Set(batch.map((entry) => entry.group)))
                trace.group(name).waves++;
              for (const entry of batch) entry.release();
            });
        });
        return await inner.getRange(path, range);
      } finally {
        trace.active--;
        group.active--;
      }
    },
  };
  return { store, trace };
}
