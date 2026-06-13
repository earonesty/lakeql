import { expect, it } from "vitest";
import { and, createLake, eq, gt, LaQLError } from "./index.js";

it("re-exports the core surface", () => {
  const expr = and(eq("region", "west"), gt("amount", 100));
  expect(expr.kind).toBe("logical");
  expect(new LaQLError("LAQL_PARSE_ERROR", "x").code).toBe("LAQL_PARSE_ERROR");
  expect(createLake).toBeTypeOf("function");
});

it("exports runtime driver subpaths", async () => {
  const cloudflare = await import("./cloudflare.js");
  const node = await import("./node.js");

  expect(cloudflare.createLake).toBeTypeOf("function");
  expect(cloudflare.r2Store).toBeTypeOf("function");
  expect(node.createLake).toBeTypeOf("function");
  expect(node.httpStore).toBeTypeOf("function");
  expect(node.s3Store).toBeTypeOf("function");
});
