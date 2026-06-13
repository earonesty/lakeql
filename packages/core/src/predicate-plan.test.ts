import { describe, expect, it } from "vitest";
import {
  and,
  between,
  col,
  eq,
  fn,
  gt,
  isIn,
  isNotNull,
  like,
  lit,
  not,
  notIn,
  or,
} from "./expr.js";
import { classifyPredicate } from "./predicate-plan.js";

describe("classifyPredicate", () => {
  it("returns empty buckets without a predicate", () => {
    expect(classifyPredicate()).toEqual({
      partition: [],
      fileStats: [],
      rowGroupStats: [],
      residual: [],
    });
  });

  it("splits supported predicate shapes by pruning surface", () => {
    const plan = classifyPredicate(
      and(
        eq("country", "US"),
        gt("amount", 100),
        between("date", "2026-01-01", "2026-01-31"),
        like("name", "A%"),
      ),
      {
        partitionColumns: ["country"],
        fileStatsColumns: ["date"],
        rowGroupStatsColumns: ["amount"],
      },
    );

    expect(plan.partition).toMatchObject([{ kind: "compare", op: "eq" }]);
    expect(plan.rowGroupStats).toMatchObject([{ kind: "compare", op: "gt" }]);
    expect(plan.fileStats).toMatchObject([{ kind: "between" }]);
    expect(plan.residual).toMatchObject([{ kind: "like" }]);
  });

  it("keeps unsupported or nonliteral stats shapes residual", () => {
    const plan = classifyPredicate(
      and(notIn("amount", [1]), isIn("amount", [col("id")]), fn("noop")),
    );

    expect(plan.partition).toEqual([]);
    expect(plan.fileStats).toEqual([]);
    expect(plan.rowGroupStats).toEqual([]);
    expect(plan.residual).toMatchObject([
      { kind: "in", negated: true },
      { kind: "in", negated: false },
      { kind: "call" },
    ]);
  });

  it("classifies right-side column comparisons and in-list stats predicates", () => {
    const plan = classifyPredicate(
      and(eq(lit(100), col("amount")), isIn("status", ["open", "new"])),
      {
        rowGroupStatsColumns: ["amount", "status"],
      },
    );

    expect(plan.rowGroupStats).toMatchObject([
      { kind: "compare", op: "eq" },
      { kind: "in", negated: false },
    ]);
    expect(plan.residual).toEqual([]);
  });

  it("treats partition-only expression families as partition predicates", () => {
    const plan = classifyPredicate(
      and(isNotNull("country"), not(eq("country", "CA")), like("country", "U%")),
      { partitionColumns: ["country"] },
    );

    expect(plan.partition).toMatchObject([
      { kind: "null-check" },
      { kind: "not" },
      { kind: "like" },
    ]);
    expect(plan.residual).toEqual([]);
  });

  it("keeps mixed boolean structure and null literal stats predicates residual", () => {
    const plan = classifyPredicate(and(or(eq("amount", 1), eq("amount", 2)), eq("amount", null)), {
      rowGroupStatsColumns: ["amount"],
    });

    expect(plan.rowGroupStats).toEqual([]);
    expect(plan.residual).toMatchObject([
      { kind: "logical", op: "or" },
      { kind: "compare", op: "eq", right: lit(null) },
    ]);
  });
});
