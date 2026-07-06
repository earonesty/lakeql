import { describe, expect, it } from "vitest";
import { extractTopLevelQualify } from "./qualify-prepass.js";

describe("QUALIFY prepass", () => {
  it("returns unchanged SQL when no top-level QUALIFY exists", () => {
    expect(extractTopLevelQualify("select qualify_col from t where note = 'qualify'")).toEqual({
      sql: "select qualify_col from t where note = 'qualify'",
    });
  });

  it("extracts top-level QUALIFY before trailing clauses", () => {
    expect(
      extractTopLevelQualify(
        "select * from t qualify row_number() over (partition by a order by b) = 1 order by b limit 5 offset 1",
      ),
    ).toEqual({
      sql: "select * from t  order by b limit 5 offset 1",
      qualify: "row_number() over (partition by a order by b) = 1",
    });
  });

  it("ignores QUALIFY inside nested expressions and comments", () => {
    expect(
      extractTopLevelQualify(`
        select *
        from t
        where exists (select 1 from u where note = 'qualify')
        -- qualify ignored
        qualify rank() over (order by score) <= 3
      `),
    ).toMatchObject({
      qualify: "rank() over (order by score) <= 3",
    });
  });

  it("keeps empty QUALIFY predicates for downstream parser errors", () => {
    const sql = "select * from t qualify order by id";
    expect(extractTopLevelQualify(sql)).toEqual({ sql });
  });
});
