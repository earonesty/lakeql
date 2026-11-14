import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope } from "./memory-scope.js";
import { admitVectorBuffer } from "./vector-buffer.js";

export type TimestampUnit = "millis" | "micros" | "nanos";

const NANOS_PER_MILLI = 1_000_000n;
const NANOS_PER_MICRO = 1_000n;

export class TimestampValue {
  readonly epochNanoseconds: bigint;
  readonly unit: TimestampUnit;
  readonly isAdjustedToUTC: boolean;

  constructor(epochNanoseconds: bigint, unit: TimestampUnit, isAdjustedToUTC = true) {
    this.epochNanoseconds = epochNanoseconds;
    this.unit = unit;
    this.isAdjustedToUTC = isAdjustedToUTC;
  }

  toJSON(): string {
    return timestampToIsoString(this);
  }

  toString(): string {
    return timestampToIsoString(this);
  }
}

export function timestampFromEpoch(
  value: bigint,
  unit: TimestampUnit,
  isAdjustedToUTC = true,
): TimestampValue {
  return new TimestampValue(epochNanoseconds(value, unit), unit, isAdjustedToUTC);
}

export function timestampValueFromIso(
  value: string,
  scope?: MemoryScope,
): TimestampValue | undefined {
  // The accepted grammar has 20–30 code units. Reject impossible lengths before
  // regex work; matching groups and conversion strings therefore have fixed bounds.
  if (value.length < 20 || value.length > 30) return undefined;
  const scratch = scope?.reserve(
    c.object + c.array + 3 * c.arraySlot + 4 * c.string + 2 * (19 + 9 + 24 + 9) + 4 * c.bigint,
    "ISO timestamp parsing workspace",
  );
  try {
    const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$/u.exec(value);
    if (!match) return undefined;
    const millis = Date.parse(`${match[1]}.000Z`);
    if (!Number.isFinite(millis)) return undefined;
    const fraction = (match[2] ?? "").padEnd(9, "0");
    return admitVectorBuffer(scope, c.timestamp + c.bigint, () => {
      const fractionNanos = BigInt(fraction);
      return new TimestampValue(
        BigInt(millis) * NANOS_PER_MILLI + fractionNanos,
        unitForFraction(match[2] ?? ""),
      );
    });
  } finally {
    scratch?.release();
  }
}

export function isTimestampValue(value: unknown): value is TimestampValue {
  return (
    typeof value === "object" &&
    value !== null &&
    "epochNanoseconds" in value &&
    typeof value.epochNanoseconds === "bigint" &&
    "unit" in value &&
    (value.unit === "millis" || value.unit === "micros" || value.unit === "nanos") &&
    "isAdjustedToUTC" in value &&
    typeof value.isAdjustedToUTC === "boolean"
  );
}

export function timestampToIsoString(value: TimestampValue): string {
  const nanosPerSecond = 1_000_000_000n;
  const seconds = floorDivide(value.epochNanoseconds, nanosPerSecond);
  const fractionNanos = value.epochNanoseconds - seconds * nanosPerSecond;
  const milliseconds = seconds * 1000n;
  const base =
    milliseconds >= -8_640_000_000_000_000n && milliseconds <= 8_640_000_000_000_000n
      ? new Date(Number(milliseconds)).toISOString().replace(/\.\d{3}Z$/u, "")
      : wideCalendarSecond(seconds);
  return `${base}${fractionForUnit(fractionNanos, value.unit)}Z`;
}

function floorDivide(value: bigint, divisor: bigint): bigint {
  const quotient = value / divisor;
  return value % divisor < 0n ? quotient - 1n : quotient;
}

/** Gregorian 400-year cycles bound calendar work independently of epoch size.
 * March-based years put the leap day at the end of the year. Day 11017 is
 * 2000-03-01; every cycle contains exactly 146097 days.
 */
function wideCalendarSecond(seconds: bigint): string {
  const days = floorDivide(seconds, 86400n);
  const secondOfDay = Number(seconds - days * 86400n);
  const marchDays = days - 11017n;
  const era = floorDivide(marchDays, 146097n);
  const dayOfEra = Number(marchDays - era * 146097n);
  const yearStart = (year: number) =>
    365 * year + Math.floor(year / 4) - Math.floor(year / 100) + Math.floor(year / 400);
  let low = 0,
    high = 400;
  while (low + 1 < high) {
    const middle = Math.floor((low + high) / 2);
    if (yearStart(middle) <= dayOfEra) low = middle;
    else high = middle;
  }
  const dayOfYear = dayOfEra - yearStart(low);
  const marchMonth = Math.floor((5 * dayOfYear + 2) / 153);
  const day = dayOfYear - Math.floor((153 * marchMonth + 2) / 5) + 1;
  const month = marchMonth < 10 ? marchMonth + 3 : marchMonth - 9;
  const year = 2000n + era * 400n + BigInt(low) + (month <= 2 ? 1n : 0n);
  const yearText =
    year >= 0n && year <= 9999n
      ? year.toString().padStart(4, "0")
      : `${year < 0n ? "-" : "+"}${(year < 0n ? -year : year).toString().padStart(6, "0")}`;
  const two = (value: number) => value.toString().padStart(2, "0");
  return `${yearText}-${two(month)}-${two(day)}T${two(Math.floor(secondOfDay / 3600))}:${two(Math.floor(secondOfDay / 60) % 60)}:${two(secondOfDay % 60)}`;
}

export function timestampEpochForUnit(value: TimestampValue, unit: TimestampUnit): bigint {
  switch (unit) {
    case "millis":
      return value.epochNanoseconds / NANOS_PER_MILLI;
    case "micros":
      return value.epochNanoseconds / NANOS_PER_MICRO;
    case "nanos":
      return value.epochNanoseconds;
  }
}

export function compareTimestampValues(left: TimestampValue, right: TimestampValue): number {
  if (left.epochNanoseconds < right.epochNanoseconds) return -1;
  if (left.epochNanoseconds > right.epochNanoseconds) return 1;
  return 0;
}

function epochNanoseconds(value: bigint, unit: TimestampUnit): bigint {
  switch (unit) {
    case "millis":
      return value * NANOS_PER_MILLI;
    case "micros":
      return value * NANOS_PER_MICRO;
    case "nanos":
      return value;
  }
}

function unitForFraction(fraction: string): TimestampUnit {
  if (fraction.length <= 3) return "millis";
  if (fraction.length <= 6) return "micros";
  return "nanos";
}

function fractionForUnit(nanos: bigint, unit: TimestampUnit): string {
  switch (unit) {
    case "millis": {
      const millis = nanos / NANOS_PER_MILLI;
      return `.${millis.toString().padStart(3, "0")}`;
    }
    case "micros": {
      const micros = nanos / NANOS_PER_MICRO;
      return `.${micros.toString().padStart(6, "0")}`;
    }
    case "nanos":
      return `.${nanos.toString().padStart(9, "0")}`;
  }
}
