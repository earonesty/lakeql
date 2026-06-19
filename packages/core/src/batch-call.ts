import type { BatchExprValues } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import { regexpMatchesValue, regexpReplaceValue } from "./regex-functions.js";

export function batchCallExprValues(
  rowCount: number,
  name: string,
  args: BatchExprValues[],
  compareEq: (left: Scalar, right: Scalar) => boolean | null,
): BatchExprValues {
  const fn = name.toLowerCase();
  switch (fn) {
    case "lower":
      return unaryStringCallValues(fn, args, (value) => value.toLowerCase());
    case "upper":
      return unaryStringCallValues(fn, args, (value) => value.toUpperCase());
    case "trim":
      return unaryStringCallValues(fn, args, (value) => value.trim());
    case "substr":
      return substrCallValues(args);
    case "replace":
      return replaceCallValues(args);
    case "regexp_matches":
      return regexpMatchesCallValues(args);
    case "regexp_replace":
      return regexpReplaceCallValues(args);
    case "cast":
      return castCallValues(args);
    case "round":
      return roundCallValues(args);
    case "floor":
      return unaryNumberCallValues(fn, args, Math.floor);
    case "ceil":
      return unaryNumberCallValues(fn, args, Math.ceil);
    case "abs":
      return unaryNumberCallValues(fn, args, Math.abs);
    case "coalesce":
      return coalesceCallValues(rowCount, args);
    case "nullif":
      return nullifCallValues(rowCount, args, compareEq);
    default:
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_PUSHDOWN",
        "Vector predicate evaluation does not support call expressions",
        { kind: "call" },
      );
  }
}

export function vectorCallExprSupported(name: string): boolean {
  switch (name.toLowerCase()) {
    case "lower":
    case "upper":
    case "trim":
    case "substr":
    case "replace":
    case "regexp_matches":
    case "regexp_replace":
    case "cast":
    case "round":
    case "floor":
    case "ceil":
    case "abs":
    case "coalesce":
    case "nullif":
      return true;
    default:
      return false;
  }
}

function coalesceCallValues(rowCount: number, args: BatchExprValues[]): BatchExprValues {
  return {
    rowCount,
    valueAt(index) {
      return args.map((arg) => arg.valueAt(index)).find((value) => value !== null) ?? null;
    },
  };
}

function nullifCallValues(
  rowCount: number,
  args: BatchExprValues[],
  compareEq: (left: Scalar, right: Scalar) => boolean | null,
): BatchExprValues {
  requireVectorArgCount("nullif", args, 2);
  return {
    rowCount,
    valueAt(index) {
      const left = args[0]?.valueAt(index) ?? null;
      const right = args[1]?.valueAt(index) ?? null;
      return compareEq(left, right) === true ? null : left;
    },
  };
}

function unaryStringCallValues(
  name: string,
  args: BatchExprValues[],
  cb: (value: string) => string,
): BatchExprValues {
  requireVectorArgCount(name, args, 1);
  const source = args[0];
  return {
    rowCount: source?.rowCount ?? 0,
    valueAt(index) {
      const value = source?.valueAt(index) ?? null;
      if (value === null) return null;
      if (typeof value !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects string`, {
          expected: "string",
          actual: typeof value,
        });
      }
      return cb(value);
    },
  };
}

function unaryNumberCallValues(
  name: string,
  args: BatchExprValues[],
  cb: (value: number) => number,
): BatchExprValues {
  requireVectorArgCount(name, args, 1);
  const source = args[0];
  return {
    rowCount: source?.rowCount ?? 0,
    valueAt(index) {
      const value = source?.valueAt(index) ?? null;
      if (value === null) return null;
      if (typeof value !== "number") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects number`, {
          expected: "number",
          actual: typeof value,
        });
      }
      return cb(value);
    },
  };
}

function substrCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("substr", args, 3);
  const valueArg = args[0];
  const startArg = args[1];
  const lengthArg = args[2];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const start = startArg?.valueAt(index) ?? null;
      const length = lengthArg?.valueAt(index) ?? null;
      if (value === null || start === null || length === null) return null;
      if (typeof value !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "substr() expects string", {
          expected: "string",
          actual: typeof value,
        });
      }
      if (typeof start !== "number" || typeof length !== "number") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "substr() start and length must be numbers");
      }
      return value.slice(start, start + length);
    },
  };
}

function replaceCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("replace", args, 3);
  const valueArg = args[0];
  const searchArg = args[1];
  const replacementArg = args[2];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const search = searchArg?.valueAt(index) ?? null;
      const replacement = replacementArg?.valueAt(index) ?? null;
      if (value === null || search === null || replacement === null) return null;
      if (typeof value !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "replace() expects string", {
          expected: "string",
          actual: typeof value,
        });
      }
      if (typeof search !== "string" || typeof replacement !== "string") {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "replace() search and replacement must be strings",
        );
      }
      return value.replaceAll(search, replacement);
    },
  };
}

function regexpMatchesCallValues(args: BatchExprValues[]): BatchExprValues {
  if (args.length < 2 || args.length > 3) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "regexp_matches() expects 2 or 3 arguments", {
      received: args.length,
    });
  }
  const valueArg = args[0];
  const patternArg = args[1];
  const optionsArg = args[2];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const pattern = patternArg?.valueAt(index) ?? null;
      const options = optionsArg?.valueAt(index) ?? "";
      if (value === null || pattern === null || options === null) return null;
      if (typeof value !== "string" || typeof pattern !== "string" || typeof options !== "string") {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "regexp_matches() value, pattern, and options must be strings",
        );
      }
      return regexpMatchesValue(value, pattern, options);
    },
  };
}

function regexpReplaceCallValues(args: BatchExprValues[]): BatchExprValues {
  if (args.length < 3 || args.length > 4) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "regexp_replace() expects 3 or 4 arguments", {
      received: args.length,
    });
  }
  const valueArg = args[0];
  const patternArg = args[1];
  const replacementArg = args[2];
  const optionsArg = args[3];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const pattern = patternArg?.valueAt(index) ?? null;
      const replacement = replacementArg?.valueAt(index) ?? null;
      const options = optionsArg?.valueAt(index) ?? "";
      if (value === null || pattern === null || replacement === null || options === null) {
        return null;
      }
      if (
        typeof value !== "string" ||
        typeof pattern !== "string" ||
        typeof replacement !== "string" ||
        typeof options !== "string"
      ) {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "regexp_replace() value, pattern, replacement, and options must be strings",
        );
      }
      return regexpReplaceValue(value, pattern, replacement, options);
    },
  };
}

function castCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("cast", args, 2);
  const valueArg = args[0];
  const targetArg = args[1];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      if (value === null) return null;
      const target = targetArg?.valueAt(index) ?? null;
      if (typeof target !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "cast() expects string type name", {
          expected: "string type name",
          actual: typeof target,
        });
      }
      switch (target) {
        case "string":
          return String(value);
        case "float64":
        case "number": {
          const number = Number(value);
          return Number.isNaN(number) ? null : number;
        }
        case "boolean":
          return Boolean(value);
        default:
          throw new LakeqlError("LAKEQL_TYPE_ERROR", `Unsupported cast target ${target}`, {
            target,
          });
      }
    },
  };
}

function roundCallValues(args: BatchExprValues[]): BatchExprValues {
  if (args.length < 1 || args.length > 2) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "round() expects 1 or 2 arguments", {
      received: args.length,
    });
  }
  const valueArg = args[0];
  const placesArg = args[1];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const places = placesArg?.valueAt(index) ?? 0;
      if (value === null) return null;
      if (typeof value !== "number" || typeof places !== "number") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "round() arguments must be numbers");
      }
      const scale = 10 ** places;
      return Math.round(value * scale) / scale;
    },
  };
}

function requireVectorArgCount(name: string, args: unknown[], expected: number): void {
  if (args.length !== expected) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects ${expected} arguments`, {
      expected,
      received: args.length,
    });
  }
}
