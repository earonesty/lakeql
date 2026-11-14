/** Inspector allocation samples are estimates, not allocation/peak guarantees.
 * Aggregate direct allocation sites without counting parent totals twice.
 */
export function summarizeHeapProfile(
  profile,
  script,
  { limit = 40, includeCollected = false } = {},
) {
  const lines = script.split("\n");
  const sources = [];
  let source;
  for (const line of lines) {
    if (/^\/\/ (packages|node_modules|bench)\//.test(line)) source = line.slice(3);
    sources.push(source);
  }
  const sites = new Map();
  const pending = [{ node: profile.head, caller: undefined }];
  let estimatedBytes = 0;
  while (pending.length > 0) {
    const { node, caller } = pending.pop();
    for (const child of node.children) pending.push({ node: child, caller: node.callFrame });
    if (node.selfSize === 0) continue;
    const frame = node.callFrame;
    const key = JSON.stringify([frame, caller]);
    let site = sites.get(key);
    if (site === undefined) {
      site = {
        ...frame,
        bundleSource: sources[frame.lineNumber],
        caller:
          caller === undefined
            ? undefined
            : { ...caller, bundleSource: sources[caller.lineNumber] },
        estimatedBytes: 0,
      };
      sites.set(key, site);
    }
    site.estimatedBytes += node.selfSize;
    estimatedBytes += node.selfSize;
  }
  const ranked = [...sites.values()].sort((a, b) => b.estimatedBytes - a.estimatedBytes);
  return {
    estimatedBytes,
    includesCollectedObjects: includeCollected,
    totalSites: ranked.length,
    sites: ranked.slice(0, limit),
    omittedEstimatedBytes: ranked.slice(limit).reduce((sum, site) => sum + site.estimatedBytes, 0),
    scope: includeCollected
      ? "Sampling estimates since this round started, including objects collected by minor/major GC. Estimated allocation volume, not live bytes or a peak. Profiling changes behavior; not comparable to ordinary latency runs."
      : "Sampling estimates since this round started that remain represented when output is retained. GC can remove samples. Not cumulative allocation volume, exact live bytes, or a peak. Profiling changes behavior; not comparable to ordinary latency runs.",
  };
}
