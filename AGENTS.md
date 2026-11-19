# Repository Rules

- Treat "for now", "MVP", "v1", "minimal", "temporary", "hack", and similar framing as design-smell language in implementation work. Stop and recast the change as a durable general contract, or explicitly document why the boundary is a real product/architecture boundary rather than a shortcut.
- Prefer broadly applicable physical operators and planner rules over query-specific branches. Operator limits must be expressed as explicit vector-shape capabilities in the generic execution engine, not as product-scope shortcuts.
- If something is deeper/harder and requires more work, but has a bigger payoff, always pursue that first.  Avoid minimal fixes.
- Never open draft pull requests for this repository. Always open real pull requests.

- Changes to indexed or random-access execution must include deterministic performance contracts: cold/repeat/nearby correctness, request and byte ceilings, page/candidate/decoded-row bounds, accounted-memory limits, and bounded concurrency. Use a delayed or network-wave store to cap dependent round trips; filesystem timings alone do not validate remote performance. Verify that regression tests fail against the old execution path. Record public-network timings separately from CI thresholds, and distinguish accounted operation memory from actual process memory.
