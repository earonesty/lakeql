import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      miniflare: {
        compatibilityDate: "2026-06-13",
        compatibilityFlags: ["nodejs_compat"],
        r2Buckets: ["ORDERING_FIXTURES"],
      },
    }),
  ],
  test: {
    include: ["packages/**/*.workerd.ts"],
  },
});
