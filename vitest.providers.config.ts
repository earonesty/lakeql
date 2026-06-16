import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["packages/**/*.provider.test.ts"],
    testTimeout: 60_000,
  },
});
