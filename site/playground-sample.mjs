import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

// Content-addressed objects can be cached indefinitely without mixing fixture revisions.
export const samplePath = new URL("./public/sales.parquet", import.meta.url);
export const sampleBytes = readFileSync(samplePath);
export const sampleHash = createHash("sha256").update(sampleBytes).digest("hex");
export const sampleKey = `playground/${sampleHash}/sales.parquet`;
export const sampleUrl = `https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/${sampleKey}`;
