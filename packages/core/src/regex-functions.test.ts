import { describe, expect, it } from "vitest";
import { regexpMatchesValue, regexpReplaceValue } from "./regex-functions.js";

describe("regex functions", () => {
  it("matches DuckDB regex option semantics", () => {
    expect(regexpMatchesValue("Alpha\nbeta", "^beta", "im")).toBe(true);
    expect(regexpMatchesValue("Alpha\nbeta", "Alpha.beta", "s")).toBe(true);
    expect(regexpMatchesValue("a.c", ".", "l")).toBe(true);
    expect(regexpMatchesValue("ABC", "abc", "ic")).toBe(false);
    expect(regexpReplaceValue("aba", "a", "x", "g")).toBe("xbx");
    expect(regexpReplaceValue("abc", "(a)(b)", "\\2\\1")).toBe("bac");
  });

  it("throws typed errors for invalid regex options and patterns", () => {
    expect(() => regexpMatchesValue("abc", "a", "g")).toThrow(
      "g regex option is only valid for regexp_replace()",
    );
    expect(() => regexpMatchesValue("abc", "a", "z")).toThrow(
      "received an unsupported regex option",
    );
    expect(() => regexpMatchesValue("abc", "[", "")).toThrow(
      "received an invalid regular expression",
    );
  });
});
