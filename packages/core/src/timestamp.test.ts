import { describe, expect, it } from "vitest";
import {
  compareTimestampValues,
  isTimestampValue,
  TimestampValue,
  timestampEpochForUnit,
  timestampFromEpoch,
  timestampToIsoString,
  timestampValueFromIso,
} from "./timestamp.js";

describe("timestamp values", () => {
  it("round-trips epoch values across timestamp units", () => {
    const millis = timestampFromEpoch(1_700_000_000_123n, "millis");
    const micros = timestampFromEpoch(1_700_000_000_123_456n, "micros");
    const nanos = timestampFromEpoch(1_700_000_000_123_456_789n, "nanos", false);

    expect(millis.toString()).toBe("2023-11-14T22:13:20.123Z");
    expect(millis.toJSON()).toBe("2023-11-14T22:13:20.123Z");
    expect(timestampToIsoString(micros)).toBe("2023-11-14T22:13:20.123456Z");
    expect(timestampToIsoString(nanos)).toBe("2023-11-14T22:13:20.123456789Z");
    expect(timestampEpochForUnit(nanos, "millis")).toBe(1_700_000_000_123n);
    expect(timestampEpochForUnit(nanos, "micros")).toBe(1_700_000_000_123_456n);
    expect(timestampEpochForUnit(nanos, "nanos")).toBe(1_700_000_000_123_456_789n);
    expect(nanos.isAdjustedToUTC).toBe(false);
  });

  it("parses ISO strings with unit precision and rejects invalid values", () => {
    expect(timestampValueFromIso("not-a-timestamp")).toBeUndefined();
    expect(timestampValueFromIso("2024-99-99T00:00:00Z")).toBeUndefined();
    expect(timestampValueFromIso("2024-01-02T03:04:05Z")).toMatchObject({
      unit: "millis",
      epochNanoseconds: 1_704_164_645_000_000_000n,
    });
    expect(timestampValueFromIso("2024-01-02T03:04:05.123456Z")).toMatchObject({
      unit: "micros",
      epochNanoseconds: 1_704_164_645_123_456_000n,
    });
    expect(timestampValueFromIso("2024-01-02T03:04:05.123456789Z")).toMatchObject({
      unit: "nanos",
      epochNanoseconds: 1_704_164_645_123_456_789n,
    });
  });

  it("compares and identifies timestamp-like values structurally", () => {
    const earlier = new TimestampValue(10n, "nanos");
    const later = new TimestampValue(20n, "nanos");

    expect(compareTimestampValues(earlier, later)).toBe(-1);
    expect(compareTimestampValues(later, earlier)).toBe(1);
    expect(compareTimestampValues(earlier, new TimestampValue(10n, "millis"))).toBe(0);
    expect(isTimestampValue(earlier)).toBe(true);
    expect(isTimestampValue({ epochNanoseconds: 1n, unit: "days", isAdjustedToUTC: true })).toBe(
      false,
    );
    expect(isTimestampValue({ epochNanoseconds: 1, unit: "nanos", isAdjustedToUTC: true })).toBe(
      false,
    );
  });
});

it("formats negative fractional epochs using the preceding calendar second", () => {
  expect(timestampFromEpoch(-1n, "millis").toString()).toBe("1969-12-31T23:59:59.999Z");
  expect(timestampFromEpoch(-1n, "micros").toString()).toBe("1969-12-31T23:59:59.999999Z");
  expect(timestampFromEpoch(-1n, "nanos").toString()).toBe("1969-12-31T23:59:59.999999999Z");
});

it("formats outside Date range using exact Gregorian cycles", () => {
  const cycleNanos = 146097n * 86400n * 1_000_000_000n;
  for (const date of [
    "2000-02-29T23:59:59.123Z",
    "1900-03-01T00:00:00.000Z",
    "2001-01-01T00:00:00.000Z",
    "1970-01-01T00:00:00.000Z",
  ]) {
    const original = BigInt(Date.parse(date)) * 1_000_000n;
    for (const cycles of [-1_000_000_000n, -1000n, 1000n, 1_000_000_000n]) {
      const year = BigInt(date.slice(0, 4)) + cycles * 400n;
      const expectedYear = `${year < 0n ? "-" : "+"}${(year < 0n ? -year : year).toString().padStart(6, "0")}`;
      expect(new TimestampValue(original + cycles * cycleNanos, "millis").toString()).toBe(
        `${expectedYear}${date.slice(4)}`,
      );
    }
  }
});

it("matches Date within its range and wide conversion after full-cycle shifts", () => {
  let seed = 123456789;
  const cycleMillis = 146097n * 86400000n;
  for (let i = 0; i < 1000; i++) {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    const millis = BigInt((seed % 200000000) - 100000000) * 86400000n + BigInt(i);
    const oracle = new Date(Number(millis)).toISOString();
    expect(timestampFromEpoch(millis, "millis").toString()).toBe(oracle);
    const match = /^([+-]?\d+)(-\d\d-\d\dT.*)$/u.exec(oracle);
    if (match?.[1] === undefined || match[2] === undefined) throw new Error("Invalid oracle");
    for (const shift of [-1_000_000n, 1_000_000n]) {
      const year = BigInt(match[1]) + shift * 400n;
      const yearText = `${year < 0n ? "-" : "+"}${year < 0n ? -year : year}`;
      expect(timestampFromEpoch(millis + cycleMillis * shift, "millis").toString()).toBe(
        `${yearText}${match[2]}`,
      );
    }
  }
});
