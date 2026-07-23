import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: ["packages/*"],
    exclude: [
      "**/*.conformance.test.ts",
      "**/*.provider.test.ts",
      "**/*.reference.test.ts",
      "**/*.workerd.test.ts",
      "**/node_modules/**",
      "**/dist/**",
    ],
    coverage: {
      provider: "v8",
      include: ["packages/*/src/**"],
      exclude: ["**/bin.ts", "**/*.test.ts", "**/*.workerd.ts", "**/*.generated.ts"],
      thresholds: {
        lines: 90,
        functions: 90,
        branches: 84,
        statements: 90,
      },
    },
  },
});
