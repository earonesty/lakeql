import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      miniflare: {
        r2Buckets: ["MEMORY_TEST_BUCKET"],
        compatibilityDate: "2026-06-13",
        compatibilityFlags: ["nodejs_compat"],
      },
    }),
  ],
  test: {
    include: ["packages/**/*.workerd.ts"],
  },
});
