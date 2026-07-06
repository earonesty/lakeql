import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import type { Scalar } from "./expr.js";
import type { AggregateOp } from "./query.js";
import { compareAggregateScalars, numericAggregateValue } from "./scalar-order.js";
import type { WindowExpr } from "./window.js";
import { frameSpan } from "./window-frame.js";
import { filterRow, type WindowRow } from "./window-utils.js";

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
): Scalar[] | undefined {
  if (typeof expr.fn !== "object" || expr.distinct === true) return undefined;
  if ((expr.over.frame?.exclude ?? "no-others") !== "no-others") return undefined;
  const op = expr.fn.aggregate;
  if (!isSegmentAggregate(op)) return undefined;
  const tree = new SegmentTree(partition.map((row) => leafNode(row, expr, op)));
  return partition.map((_row, index) =>
    aggregateValue(op, tree.query(frameSpan(partition, index, expr))),
  );
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

function leafNode(windowRow: WindowRow, expr: WindowExpr, op: AggregateOp): SegmentNode {
  if (!filterRow(expr, windowRow.row)) return EMPTY_NODE;
  const value = aggregateInputValue(expr, windowRow);
  if (op === "count") return value === null ? EMPTY_NODE : { ...EMPTY_NODE, count: 1 };
  if (value === null) return EMPTY_NODE;
  if (op === "min" || op === "max") return { ...EMPTY_NODE, count: 1, min: value, max: value };
  const numeric = numericValue(op, value);
  return { count: 1, sum: numeric, mean: numeric, m2: 0, min: undefined, max: undefined };
}

function aggregateInputValue(expr: WindowExpr, windowRow: WindowRow): Scalar {
  const arg = expr.args[0];
  if (arg === undefined) return 1;
  return evaluate(arg, windowRow.row);
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

  constructor(leaves: readonly SegmentNode[]) {
    let size = 1;
    while (size < leaves.length) size *= 2;
    this.size = size;
    this.nodes = Array.from({ length: size * 2 }, () => EMPTY_NODE);
    for (let index = 0; index < leaves.length; index += 1) {
      this.nodes[size + index] = leaves[index] ?? EMPTY_NODE;
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
