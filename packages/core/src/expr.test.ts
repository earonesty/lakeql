import { describe, expect, it } from "vitest";
import { LaQLError } from "./errors.js";
import {
  and,
  between,
  col,
  eq,
  fn,
  gt,
  gte,
  ilike,
  isIn,
  isNotNull,
  isNull,
  like,
  lit,
  lt,
  lte,
  ne,
  not,
  notIn,
  or,
} from "./expr.js";

describe("comparison builders", () => {
  it("treats bare strings as column names on the left, literals on the right", () => {
    expect(eq("region", "west")).toEqual({
      kind: "compare",
      op: "eq",
      left: { kind: "column", name: "region" },
      right: { kind: "literal", value: "west" },
    });
  });

  it("builds every comparison op", () => {
    expect(ne("a", 1).op).toBe("ne");
    expect(lt("a", 1).op).toBe("lt");
    expect(lte("a", 1).op).toBe("lte");
    expect(gt("a", 1).op).toBe("gt");
    expect(gte("a", 1).op).toBe("gte");
  });

  it("accepts explicit col/lit and nested expressions", () => {
    const e = eq(col("h3_8"), fn("h3_cell", lit(34.05), lit(-118.24), lit(8)));
    expect(e.left).toEqual({ kind: "column", name: "h3_8" });
    expect(e.right.kind).toBe("call");
  });

  it("supports scalar variety: number, boolean, bigint, null", () => {
    expect(gt("amount", 100).right).toEqual({ kind: "literal", value: 100 });
    expect(eq("active", true).right).toEqual({ kind: "literal", value: true });
    expect(eq("big", 9007199254740993n).right).toEqual({
      kind: "literal",
      value: 9007199254740993n,
    });
    expect(eq("maybe", null).right).toEqual({ kind: "literal", value: null });
  });
});

describe("in / between / null checks", () => {
  it("isIn and notIn", () => {
    const e = isIn("state", ["CA", "OR"]);
    expect(e).toMatchObject({ kind: "in", negated: false });
    expect(e.values).toHaveLength(2);
    expect(notIn("state", ["WA"]).negated).toBe(true);
  });

  it("between carries low and high", () => {
    const e = between("date", "2026-01-01", "2026-06-01");
    expect(e.low).toEqual({ kind: "literal", value: "2026-01-01" });
    expect(e.high).toEqual({ kind: "literal", value: "2026-06-01" });
  });

  it("isNull and isNotNull", () => {
    expect(isNull("email").negated).toBe(false);
    expect(isNotNull("email").negated).toBe(true);
  });
});

describe("logical builders", () => {
  it("and/or are variadic", () => {
    const e = and(eq("a", 1), eq("b", 2), or(eq("c", 3), eq("d", 4)));
    expect(e.op).toBe("and");
    expect(e.operands).toHaveLength(3);
  });

  it("and/or reject fewer than 2 operands with a typed error", () => {
    expect(() => and(eq("a", 1))).toThrowError(LaQLError);
    try {
      or();
    } catch (e) {
      expect(e).toBeInstanceOf(LaQLError);
      expect((e as LaQLError).code).toBe("LAQL_TYPE_ERROR");
      expect((e as LaQLError).details.received).toBe(0);
    }
  });

  it("not wraps any expression", () => {
    expect(not(isNull("x"))).toEqual({
      kind: "not",
      operand: { kind: "null-check", negated: false, target: { kind: "column", name: "x" } },
    });
  });
});

describe("like / ilike / fn", () => {
  it("like is case-sensitive, ilike is not", () => {
    expect(like("name", "%plumber%").caseInsensitive).toBe(false);
    expect(ilike("name", "%Plumber%").caseInsensitive).toBe(true);
  });

  it("fn builds generic calls with auto-wrapped args", () => {
    const e = fn("h3_within", col("h3_8"), "8829a1d757fffff", 2);
    expect(e.fn).toBe("h3_within");
    expect(e.args).toEqual([
      { kind: "column", name: "h3_8" },
      { kind: "literal", value: "8829a1d757fffff" },
      { kind: "literal", value: 2 },
    ]);
  });
});
