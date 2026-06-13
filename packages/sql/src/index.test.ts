import { expect, it } from "vitest";
import { PACKAGE } from "./index.js";

it("exports the package marker", () => {
  expect(PACKAGE).toBe("@laql/sql");
});
