import { expect, it } from "vitest";
import { and, eq, gt, LaQLError } from "./index.js";

it("re-exports the core surface", () => {
  const expr = and(eq("region", "west"), gt("amount", 100));
  expect(expr.kind).toBe("logical");
  expect(new LaQLError("LAQL_PARSE_ERROR", "x").code).toBe("LAQL_PARSE_ERROR");
});
