import { describe, expect, it } from "vitest";
import { extractWindowFrames } from "./window-prepass.js";

describe("window prepass", () => {
  it("leaves SQL without windows unchanged", () => {
    expect(extractWindowFrames("select window_name from t")).toEqual({
      sql: "select window_name from t",
      frames: [],
    });
  });

  it("extracts frames while ignoring keywords in nested expressions, comments, and strings", () => {
    const result = extractWindowFrames(`
        select sum(x) over (
          partition by a
          order by coalesce(label, 'rows between')
          -- rows between should be ignored here
          rows between 2 preceding and current row
        ) as total
        from t
      `);
    expect(result.frames).toEqual([{ text: "rows between 2 preceding and current row" }]);
    expect(result.sql).toContain("-- rows between should be ignored here\n");
    expect(result.sql).not.toContain("-- rows between should be ignored here)");
  });

  it("extracts null treatment before OVER and inside value arguments", () => {
    expect(
      extractWindowFrames(
        "select first_value(x ignore nulls) over (order by y), lag(x) respect nulls over (order by y) from t",
      ),
    ).toEqual({
      sql: "select first_value(x) over (order by y), lag(x) over (order by y) from t",
      frames: [{ ignoreNulls: true }, { ignoreNulls: false }],
    });
  });

  it("expands quoted and inherited named windows", () => {
    expect(
      extractWindowFrames(`
        select row_number() over "ranked",
          sum(x) over ("running" range between 1 preceding and current row)
        from t
        window "ranked" as (partition by a order by y),
          "running" as ("ranked")
        order by a
      `),
    ).toEqual({
      sql: `
        select row_number() over (partition by a order by y),
          sum(x) over (partition by a order by y)
        from t
         order by a
      `,
      frames: [undefined, { text: "range between 1 preceding and current row" }],
    });
  });

  it("keeps incomplete OVER clauses parseable by the downstream parser", () => {
    expect(extractWindowFrames("select sum(x) over (partition by a from t")).toEqual({
      sql: "select sum(x) over (partition by a from t",
      frames: [],
    });
  });

  it("handles quoted named-window edge cases and unterminated block comments", () => {
    expect(
      extractWindowFrames(`
        select sum(x) over "a""b" from t
        window "a""b" as (partition by a order by b)
      `),
    ).toMatchObject({
      sql: expect.stringContaining("over (partition by a order by b)"),
      frames: [undefined],
    });

    expect(
      extractWindowFrames(`
        select sum(x) over (partition by a /* rows between ignored
      `),
    ).toEqual({
      sql: `
        select sum(x) over (partition by a /* rows between ignored
      `,
      frames: [],
    });

    expect(
      extractWindowFrames(`
        select sum(x) over broken from t
        window "unterminated as (partition by a)
      `),
    ).toMatchObject({
      frames: [undefined],
    });
  });
});
