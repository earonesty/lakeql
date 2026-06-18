import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const require = createRequire(import.meta.url);
const lakeqlPackage = require("../packages/lakeql/package.json") as { version: string };

// Relative base so the same build works on GitHub Pages project pages
// (lakeql.com/) and on a custom domain (lakeql.com) without a rebuild.
export default defineConfig({
  base: "./",
  define: {
    __LAKEQL_VERSION__: JSON.stringify(lakeqlPackage.version),
  },
  build: {
    target: "es2022",
    outDir: "dist",
    sourcemap: false,
    rollupOptions: {
      input: {
        main: fileURLToPath(new URL("index.html", import.meta.url)),
        compare: fileURLToPath(new URL("compare.html", import.meta.url)),
      },
    },
  },
});
