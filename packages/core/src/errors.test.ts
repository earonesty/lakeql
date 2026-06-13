import { describe, expect, it } from "vitest";
import { ERROR_CODES, isLaQLError, LaQLError } from "./errors.js";

describe("LaQLError", () => {
  it("carries code, message, and details", () => {
    const err = new LaQLError("LAQL_BUDGET_EXCEEDED", "Query would scan 14,822 files.", {
      filesPlanned: 14822,
      maxFiles: 500,
    });
    expect(err.code).toBe("LAQL_BUDGET_EXCEEDED");
    expect(err.message).toContain("14,822");
    expect(err.details.maxFiles).toBe(500);
    expect(err.name).toBe("LaQLError");
  });

  it("defaults details to an empty object", () => {
    const err = new LaQLError("LAQL_PARSE_ERROR", "bad input");
    expect(err.details).toEqual({});
  });

  it("is an Error and survives instanceof across catch", () => {
    try {
      throw new LaQLError("LAQL_UNKNOWN_TABLE", "no such table");
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect(e).toBeInstanceOf(LaQLError);
    }
  });

  it("isLaQLError narrows correctly", () => {
    expect(isLaQLError(new LaQLError("LAQL_TYPE_ERROR", "x"))).toBe(true);
    expect(isLaQLError(new Error("x"))).toBe(false);
    expect(isLaQLError(null)).toBe(false);
    expect(isLaQLError("LAQL_TYPE_ERROR")).toBe(false);
  });

  it("every spec error code is unique and LAQL_-prefixed", () => {
    expect(new Set(ERROR_CODES).size).toBe(ERROR_CODES.length);
    for (const code of ERROR_CODES) {
      expect(code).toMatch(/^LAQL_[A-Z_]+$/);
    }
  });
});
