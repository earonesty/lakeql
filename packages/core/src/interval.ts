import { LakeqlError } from "./errors.js";
import { TimestampValue } from "./timestamp.js";

export interface IntervalValue {
  kind: "interval";
  months: number;
  days: number;
  nanoseconds: string;
}

const NANOS_PER_SECOND = 1_000_000_000n;
const NANOS_PER_MILLI = 1_000_000n;
const NANOS_PER_MICRO = 1_000n;

export function intervalValue(input: string): IntervalValue {
  const trimmed = input.trim();
  if (trimmed.length === 0) throwInterval(input);
  let rest = trimmed;
  let months = 0;
  let days = 0;
  let nanoseconds = 0n;
  let matchedAny = false;

  const leadingTime = /^([+-]?\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?)\b/u.exec(rest);
  if (leadingTime?.[1] !== undefined) {
    nanoseconds += parseIntervalTime(leadingTime[1], input);
    rest = rest.slice(leadingTime[0].length).trim();
    matchedAny = true;
  }

  const partPattern =
    /([+-]?(?:\d+(?:\.\d+)?|\.\d+))\s*(years?|mons?|months?|days?|hours?|hrs?|minutes?|mins?|seconds?|secs?|milliseconds?|millis?|microseconds?|micros?|nanoseconds?|nanos?)\b/giu;
  let cursor = 0;
  for (const match of rest.matchAll(partPattern)) {
    if (match.index === undefined || rest.slice(cursor, match.index).trim().length > 0) {
      throwInterval(input);
    }
    const amount = Number(match[1]);
    const unit = match[2]?.toLowerCase() ?? "";
    if (!Number.isFinite(amount)) throwInterval(input);
    if (unit.startsWith("year")) months += integerAmount(amount, unit, input) * 12;
    else if (unit === "mon" || unit === "mons" || unit.startsWith("month"))
      months += integerAmount(amount, unit, input);
    else if (unit.startsWith("day")) days += integerAmount(amount, unit, input);
    else if (unit.startsWith("hour") || unit === "hr" || unit === "hrs")
      nanoseconds += decimalNanoseconds(amount, 60n * 60n * NANOS_PER_SECOND);
    else if (unit.startsWith("minute") || unit === "min" || unit === "mins")
      nanoseconds += decimalNanoseconds(amount, 60n * NANOS_PER_SECOND);
    else if (unit.startsWith("second") || unit === "sec" || unit === "secs")
      nanoseconds += decimalNanoseconds(amount, NANOS_PER_SECOND);
    else if (unit.startsWith("millisecond") || unit === "milli" || unit === "millis")
      nanoseconds += decimalNanoseconds(amount, NANOS_PER_MILLI);
    else if (unit.startsWith("microsecond") || unit === "micro" || unit === "micros")
      nanoseconds += decimalNanoseconds(amount, NANOS_PER_MICRO);
    else if (unit.startsWith("nanosecond") || unit === "nano" || unit === "nanos")
      nanoseconds += BigInt(integerAmount(amount, unit, input));
    else throwInterval(input);
    matchedAny = true;
    cursor = match.index + match[0].length;

    const after = rest.slice(cursor).trimStart();
    const time = /^(\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?)\b/u.exec(after);
    if (time?.[1] !== undefined) {
      nanoseconds += parseIntervalTime(time[1], input);
      cursor = rest.length - after.slice(time[0].length).length;
    }
  }
  if (!matchedAny || rest.slice(cursor).trim().length > 0) throwInterval(input);
  return { kind: "interval", months, days, nanoseconds: nanoseconds.toString() };
}

export function isIntervalValue(value: unknown): value is IntervalValue {
  return (
    typeof value === "object" &&
    value !== null &&
    "kind" in value &&
    value.kind === "interval" &&
    "months" in value &&
    Number.isInteger(value.months) &&
    "days" in value &&
    Number.isInteger(value.days) &&
    "nanoseconds" in value &&
    typeof value.nanoseconds === "string" &&
    /^-?[0-9]+$/u.test(value.nanoseconds)
  );
}

export function isNonNegativeInterval(value: IntervalValue): boolean {
  return value.months >= 0 && value.days >= 0 && BigInt(value.nanoseconds) >= 0n;
}

export function intervalToString(value: IntervalValue): string {
  const parts: string[] = [];
  if (value.months !== 0) parts.push(`${value.months} months`);
  if (value.days !== 0) parts.push(`${value.days} days`);
  const nanos = BigInt(value.nanoseconds);
  if (nanos !== 0n || parts.length === 0) parts.push(`${nanos} nanoseconds`);
  return parts.join(" ");
}

export function applyIntervalToTimestamp(
  value: TimestampValue,
  interval: IntervalValue,
  direction: -1 | 1,
): TimestampValue {
  const date = new Date(Number(value.epochNanoseconds / NANOS_PER_MILLI));
  if (interval.months !== 0) date.setUTCMonth(date.getUTCMonth() + direction * interval.months);
  if (interval.days !== 0) date.setUTCDate(date.getUTCDate() + direction * interval.days);
  const nanosWithinMilli = value.epochNanoseconds % NANOS_PER_MILLI;
  const epochNanoseconds =
    BigInt(date.getTime()) * NANOS_PER_MILLI +
    nanosWithinMilli +
    BigInt(direction) * BigInt(interval.nanoseconds);
  return new TimestampValue(epochNanoseconds, value.unit, value.isAdjustedToUTC);
}

function integerAmount(value: number, unit: string, input: string): number {
  if (!Number.isInteger(value)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `Interval ${unit} component must be an integer`, {
      interval: input,
      unit,
    });
  }
  return value;
}

function decimalNanoseconds(value: number, unitNanos: bigint): bigint {
  const scaled = value * Number(unitNanos);
  if (!Number.isFinite(scaled) || !Number.isInteger(scaled)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Interval component exceeds nanosecond precision");
  }
  return BigInt(scaled);
}

function parseIntervalTime(value: string, input: string): bigint {
  const match = /^([+-]?\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?$/u.exec(value);
  if (match === null || match[1] === undefined || match[2] === undefined) throwInterval(input);
  const sign = match[1].startsWith("-") ? -1n : 1n;
  const hours = BigInt(Math.abs(Number(match[1])));
  const minutes = BigInt(Number(match[2]));
  const seconds = BigInt(Number(match[3] ?? "0"));
  if (minutes >= 60n || seconds >= 60n) throwInterval(input);
  const fraction = BigInt((match[4] ?? "").padEnd(9, "0") || "0");
  return (
    sign *
    ((hours * 60n + minutes) * 60n * NANOS_PER_SECOND + seconds * NANOS_PER_SECOND + fraction)
  );
}

function throwInterval(input: string): never {
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `Invalid interval literal ${input}`, {
    interval: input,
  });
}
