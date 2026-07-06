import { describe, expect, it } from "vitest";
import {
  applyIntervalToTimestamp,
  intervalToString,
  intervalValue,
  isIntervalValue,
  isNonNegativeInterval,
} from "./interval.js";
import { timestampFromEpoch, timestampToIsoString } from "./timestamp.js";

describe("interval values", () => {
  it("parses calendar and time components into a JSON-stable scalar", () => {
    const interval = intervalValue("1 month 2 days 03:04:05.006007008");
    expect(interval).toEqual({
      kind: "interval",
      months: 1,
      days: 2,
      nanoseconds: "11045006007008",
    });
    expect(isIntervalValue(interval)).toBe(true);
    expect(isNonNegativeInterval(interval)).toBe(true);
    expect(intervalToString(interval)).toBe("1 months 2 days 11045006007008 nanoseconds");
  });

  it("parses supported unit aliases and leading time components", () => {
    expect(intervalValue("2 years")).toMatchObject({ months: 24, days: 0 });
    expect(intervalValue("3 mons")).toMatchObject({ months: 3, days: 0 });
    expect(intervalValue("4 hours")).toMatchObject({ nanoseconds: "14400000000000" });
    expect(intervalValue("5 mins")).toMatchObject({ nanoseconds: "300000000000" });
    expect(intervalValue("6 secs")).toMatchObject({ nanoseconds: "6000000000" });
    expect(intervalValue("7 millis")).toMatchObject({ nanoseconds: "7000000" });
    expect(intervalValue("8 micros")).toMatchObject({ nanoseconds: "8000" });
    expect(intervalValue("9 nanos")).toMatchObject({ nanoseconds: "9" });
    expect(intervalValue("01:02:03.004005006")).toMatchObject({
      nanoseconds: "3723004005006",
    });
    expect(intervalValue("-01:00")).toMatchObject({ nanoseconds: "-3600000000000" });
  });

  it("validates structural interval values", () => {
    expect(isIntervalValue(null)).toBe(false);
    expect(isIntervalValue({ kind: "duration", months: 0, days: 0, nanoseconds: "0" })).toBe(false);
    expect(isIntervalValue({ kind: "interval", months: 0.5, days: 0, nanoseconds: "0" })).toBe(
      false,
    );
    expect(isIntervalValue({ kind: "interval", months: 0, days: 0, nanoseconds: "1.5" })).toBe(
      false,
    );
  });

  it("applies intervals to timestamps with calendar-month semantics", () => {
    const start = timestampFromEpoch(BigInt(Date.parse("2026-01-31T00:00:00.000Z")), "millis");
    const shifted = applyIntervalToTimestamp(start, intervalValue("1 month 1 day"), 1);
    expect(timestampToIsoString(shifted)).toBe("2026-03-04T00:00:00.000Z");
  });

  it("rejects invalid interval literals", () => {
    expect(() => intervalValue("")).toThrow("Invalid interval literal");
    expect(() => intervalValue("1 day garbage")).toThrow("Invalid interval literal");
    expect(() => intervalValue("00:60")).toThrow("Invalid interval literal");
    expect(() => intervalValue("1.5 months")).toThrow("component must be an integer");
    expect(() => intervalValue("0.0000000001 seconds")).toThrow("nanosecond precision");
    expect(isNonNegativeInterval(intervalValue("-1 day"))).toBe(false);
  });
});
