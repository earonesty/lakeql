import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import type { Scalar } from "./expr.js";
import type { MemoryBudget, MemoryReservation } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { AggregateOp } from "./query.js";
import { compareAggregateScalars, numericAggregateValue } from "./scalar-order.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { createWindowAggregateAccumulator } from "./window-aggregate.js";
import { frameSpan } from "./window-frame.js";
import { WindowPeers } from "./window-peers.js";
import { filterRow, type WindowRow } from "./window-utils.js";
import { retainWindowValue } from "./window-value-ownership.js";

interface SegmentNode {
  count: number;
  sum: number;
  mean: number;
  m2: number;
  min: Scalar | undefined;
  max: Scalar | undefined;
}

const EMPTY_NODE: SegmentNode = {
  count: 0,
  sum: 0,
  mean: 0,
  m2: 0,
  min: undefined,
  max: undefined,
};

export function segmentTreeAggregateValues(
  partition: readonly WindowRow[],
  expr: WindowExpr,
  peers: WindowPeers | undefined = undefined,
  budget?: MemoryBudget,
  resultScope?: MemoryScope,
): Scalar[] | undefined {
  if (typeof expr.fn !== "object" || expr.distinct === true) return undefined;
  if ((expr.over.frame?.exclude ?? "no-others") !== "no-others") return undefined;
  const op = expr.fn.aggregate;
  if (!isSegmentAggregate(op)) return undefined;
  if (partition.length === 0) return [];
  if (peers === undefined) {
    const ownedPeers = new WindowPeers(partition, budget);
    try {
      return segmentTreeAggregateValues(partition, expr, ownedPeers, budget, resultScope);
    } finally {
      ownedPeers.dispose();
    }
  }
  const accumulator = createWindowAggregateAccumulator(expr, budget, resultScope);
  const frameScope = budget === undefined ? undefined : new MemoryScope(budget);
  let treeScope = budget === undefined ? undefined : new MemoryScope(budget);
  let previousStart = -1;
  let previousEnd = 0;
  let visits = 0;
  let attemptedTree = false;
  let tree: SegmentTree | undefined;
  let reservation: MemoryReservation | undefined;
  try {
    return partition.map((_row, index) => {
      const span = frameSpan(partition, index, expr, peers, frameScope);
      if (tree !== undefined)
        return retainWindowValue(
          aggregateValue(op, tree.query(span)),
          resultScope,
          treeScope,
        ) as Scalar;
      if (span.start !== previousStart || span.end < previousEnd) {
        accumulator.reset();
        previousEnd = span.start;
      }
      visits += span.end - previousEnd;
      // A tree costs a full input pass and a retained directory. Avoid that cost
      // for monotone frames; attempt it only after measured repeated traversal.
      if (!attemptedTree && visits > partition.length * 4) {
        attemptedTree = true;
        reservation = budget?.tryReserve(
          segmentTreeWorkspaceBytes(partition.length),
          "window segment tree workspace",
        );
        if (budget === undefined || reservation !== undefined) {
          accumulator.reset();
          previousEnd = span.start;
          try {
            tree = new SegmentTree(partition, expr, op, treeScope);
          } catch (error) {
            if (!(error instanceof LakeqlError) || error.code !== "LAKEQL_BUDGET_EXCEEDED")
              throw error;
            // Optional indexing must not consume the headroom needed by the scan.
            treeScope?.dispose();
            treeScope = undefined;
            reservation?.release();
            reservation = undefined;
          }
          if (tree !== undefined)
            return retainWindowValue(
              aggregateValue(op, tree.query(span)),
              resultScope,
              treeScope,
            ) as Scalar;
        }
      }
      const value = accumulator.append(spanRows(partition, previousEnd, span.end));
      previousStart = span.start;
      previousEnd = span.end;
      return value;
    });
  } finally {
    accumulator.reset();
    treeScope?.dispose();
    frameScope?.dispose();
    reservation?.release();
  }
}

/** Tree directory, all possible leaf/internal nodes and three live query
 * accumulators. Scalar payload ownership and returned output are separate. */
export function segmentTreeWorkspaceBytes(length: number): number {
  if (!Number.isSafeInteger(length) || length < 0)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid segment tree length");
  if (length === 0) return 0;
  let size = 1;
  while (size < length) size *= 2;
  const node = c.object + 6 * c.property;
  const bytes =
    c.object + 2 * c.property + c.array + 2 * size * c.arraySlot + (2 * size - 1 + 3) * node;
  if (!Number.isSafeInteger(bytes))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Segment tree workspace exceeds safe extent");
  return bytes;
}

function* spanRows(partition: readonly WindowRow[], start: number, end: number): Iterable<Row> {
  for (let index = start; index < end; index++) {
    const item = partition[index];
    if (item !== undefined) yield item.row;
  }
}

function isSegmentAggregate(op: AggregateOp): boolean {
  switch (op) {
    case "count":
    case "sum":
    case "avg":
    case "min":
    case "max":
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
      return true;
    default:
      return false;
  }
}

function leafNode(
  windowRow: WindowRow,
  expr: WindowExpr,
  op: AggregateOp,
  scope?: MemoryScope,
): SegmentNode {
  const temporary = scope?.childScope();
  try {
    if (!filterRow(expr, windowRow.row, temporary)) return EMPTY_NODE;
    const arg = expr.args[0];
    const value = arg === undefined ? 1 : evaluate(arg, windowRow.row, temporary);
    if (op === "count") return value === null ? EMPTY_NODE : { ...EMPTY_NODE, count: 1 };
    if (value === null) return EMPTY_NODE;
    if (op === "min" || op === "max") {
      const retained = retainWindowValue(value, scope, temporary) as Scalar;
      return { ...EMPTY_NODE, count: 1, min: retained, max: retained };
    }
    const numeric = numericValue(op, value);
    return { count: 1, sum: numeric, mean: numeric, m2: 0, min: undefined, max: undefined };
  } finally {
    temporary?.dispose();
  }
}

function aggregateValue(op: AggregateOp, node: SegmentNode): Scalar {
  switch (op) {
    case "count":
      return node.count;
    case "sum":
      return node.count === 0 ? null : node.sum;
    case "avg":
      return node.count === 0 ? null : node.sum / node.count;
    case "min":
      return node.min ?? null;
    case "max":
      return node.max ?? null;
    case "var_samp":
      return node.count <= 1 ? null : variance(node) / (node.count - 1);
    case "var_pop":
      return node.count === 0 ? null : variance(node) / node.count;
    case "stddev_samp":
      return node.count <= 1 ? null : Math.sqrt(variance(node) / (node.count - 1));
    case "stddev_pop":
      return node.count === 0 ? null : Math.sqrt(variance(node) / node.count);
    default:
      throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", `Unsupported segment aggregate ${op}`);
  }
}

function variance(node: SegmentNode): number {
  return Math.max(0, node.m2);
}

function numericValue(op: AggregateOp, value: Scalar): number {
  return numericAggregateValue(op, value);
}

class SegmentTree {
  private readonly size: number;
  private readonly nodes: SegmentNode[];

  constructor(
    partition: readonly WindowRow[],
    expr: WindowExpr,
    op: AggregateOp,
    scope?: MemoryScope,
  ) {
    let size = 1;
    while (size < partition.length) size *= 2;
    this.size = size;
    this.nodes = Array.from({ length: size * 2 }, () => EMPTY_NODE);
    for (let index = 0; index < partition.length; index += 1) {
      const row = partition[index];
      this.nodes[size + index] = row === undefined ? EMPTY_NODE : leafNode(row, expr, op, scope);
    }
    for (let index = size - 1; index > 0; index -= 1) {
      this.nodes[index] = mergeNodes(
        this.nodes[index * 2] ?? EMPTY_NODE,
        this.nodes[index * 2 + 1] ?? EMPTY_NODE,
      );
    }
  }

  query(span: { start: number; end: number }): SegmentNode {
    let left = span.start + this.size;
    let right = span.end + this.size;
    let leftNode = EMPTY_NODE;
    let rightNode = EMPTY_NODE;
    while (left < right) {
      if (left % 2 === 1) {
        leftNode = mergeNodes(leftNode, this.nodes[left] ?? EMPTY_NODE);
        left += 1;
      }
      if (right % 2 === 1) {
        right -= 1;
        rightNode = mergeNodes(this.nodes[right] ?? EMPTY_NODE, rightNode);
      }
      left = Math.floor(left / 2);
      right = Math.floor(right / 2);
    }
    return mergeNodes(leftNode, rightNode);
  }
}

function mergeNodes(left: SegmentNode, right: SegmentNode): SegmentNode {
  if (left.count === 0) return right;
  if (right.count === 0) return left;
  const count = left.count + right.count;
  const delta = right.mean - left.mean;
  return {
    count,
    sum: left.sum + right.sum,
    mean: left.mean + (delta * right.count) / count,
    m2: left.m2 + right.m2 + (delta * delta * (left.count * right.count)) / count,
    min: mergeOrderValue(left.min, right.min, "min"),
    max: mergeOrderValue(left.max, right.max, "max"),
  };
}

function mergeOrderValue(
  left: Scalar | undefined,
  right: Scalar | undefined,
  op: "min" | "max",
): Scalar | undefined {
  if (left === undefined) return right;
  if (right === undefined) return left;
  const comparison = compareScalar(left, right);
  return op === "min" ? (comparison <= 0 ? left : right) : comparison >= 0 ? left : right;
}

function compareScalar(left: Scalar, right: Scalar): number {
  return compareAggregateScalars(left, right);
}
