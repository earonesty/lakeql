import { expect, it } from "vitest";
import { col, lit } from "./expr.js";
import type { WindowExpr } from "./window.js";
import { evaluateWindowRows } from "./window-backend.js";
import { frameRowIterator, frameSpan } from "./window-frame.js";
import { WindowPeers } from "./window-peers.js";
import { partitionRows } from "./window-utils.js";

it("matches an independent exclusion filter in both directions across frame modes", () => {
  const rows = [0, 0, 0, 1, 1, 2, 2, 2].map((key, id) => ({ key, id }));
  for (const mode of ["rows", "groups", "range"] as const) {
    for (const exclude of ["no-others", "current-row", "group", "ties"] as const) {
      for (const end of [
        { kind: "following", offset: lit(1) },
        { kind: "preceding", offset: lit(1) },
      ] as const) {
        const expr: WindowExpr = {
          fn: "first_value",
          args: [col("id")],
          over: {
            partitionBy: [],
            orderBy: [{ expr: col("key") }],
            frame: {
              mode,
              start: { kind: "preceding", offset: lit(2) },
              end,
              exclude,
            },
          },
        };
        const partition = partitionRows(rows, expr);
        const peers = new WindowPeers(partition);
        try {
          for (let index = 0; index < rows.length; index++) {
            const span = frameSpan(partition, index, expr);
            const expected = rows.filter(
              (row) =>
                row.id >= span.start &&
                row.id < span.end &&
                (exclude === "no-others" ||
                  (exclude === "current-row"
                    ? row.id !== index
                    : row.key !== rows[index]?.key || (exclude === "ties" && row.id === index))),
            );
            for (const cursor of [undefined, peers]) {
              expect([...frameRowIterator(partition, index, expr, cursor)]).toEqual(expected);
              expect([...frameRowIterator(partition, index, expr, cursor, true)]).toEqual(
                [...expected].reverse(),
              );
            }
          }
        } finally {
          peers.dispose();
        }
      }
    }
  }
});

it("snapshots exclusion boundaries before another consumer moves the peer cursor", () => {
  const rows = [0, 0, 0, 1, 1, 2, 2, 2].map((key, id) => ({ key, id }));
  const expr: WindowExpr = {
    fn: "first_value",
    args: [col("id")],
    over: {
      partitionBy: [],
      orderBy: [{ expr: col("key") }],
      frame: {
        mode: "rows",
        start: { kind: "unbounded-preceding" },
        end: { kind: "unbounded-following" },
        exclude: "ties",
      },
    },
  };
  const partition = partitionRows(rows, expr);
  const peers = new WindowPeers(partition);
  const iterator = frameRowIterator(partition, 4, expr, peers);
  expect(iterator.next().value).toEqual(rows[0]);
  peers.peer(7);
  expect([...iterator].map((row) => row.id)).toEqual([1, 2, 4, 5, 6, 7]);
  peers.dispose();
});

it.each([
  "first_value",
  "last_value",
  "nth_value",
] as const)("stops %s once its required value is found", (fn) => {
  const length = 128;
  for (const ignoreNulls of [false, true]) {
    let reads = 0;
    const values = Array.from({ length }, (_, id) => (id < 2 || id >= length - 2 ? null : id));
    const rows = values.map((value, id) => ({
      id,
      get value() {
        reads++;
        return value;
      },
    }));
    const expr: WindowExpr = {
      fn,
      args: fn === "nth_value" ? [col("value"), lit(3)] : [col("value")],
      ignoreNulls,
      over: {
        partitionBy: [],
        orderBy: [],
        frame: {
          mode: "rows",
          start: { kind: "unbounded-preceding" },
          end: { kind: "unbounded-following" },
        },
      },
    };
    const partition = partitionRows(rows, expr);
    const eligible = ignoreNulls ? values.filter((value) => value !== null) : values;
    const expected =
      fn === "first_value" ? eligible[0] : fn === "last_value" ? eligible.at(-1) : eligible[2];
    expect(evaluateWindowRows(length, partition, [{ start: 0, end: length }], expr, {})).toEqual(
      Array(length).fill(expected),
    );
    expect(reads).toBeLessThanOrEqual(length * 5);
  }
});
