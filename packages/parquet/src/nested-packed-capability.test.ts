import { expect, it } from "vitest";
import { nestedPackedCapability } from "./nested-packed-capability.js";

it("shares packed logical capabilities without decoding payloads", () => {
  const base = { name: "value", type: "BYTE_ARRAY" as const };
  expect(nestedPackedCapability(base)).toBe("binary");
  expect(nestedPackedCapability({ ...base, converted_type: "UTF8" })).toBe("text");
  expect(nestedPackedCapability({ ...base, logical_type: { type: "STRING" } })).toBe("text");
  expect(nestedPackedCapability({ ...base, converted_type: "JSON" })).toBe("converted");
  expect(nestedPackedCapability({ ...base, converted_type: "BSON" })).toBe("converted");
  expect(
    nestedPackedCapability({ ...base, converted_type: "DECIMAL", precision: 9, scale: 2 }),
  ).toBe("converted");
  expect(nestedPackedCapability({ ...base, logical_type: { type: "FLOAT16" } })).toBe("converted");
  expect(nestedPackedCapability({ ...base, converted_type: "INTERVAL" })).toBeUndefined();
  expect(nestedPackedCapability({ ...base, converted_type: "INTERVAL" })).toBeUndefined();
  // Conflicting annotations retain conversion precedence used by the decoder.
  expect(
    nestedPackedCapability({ ...base, converted_type: "JSON", logical_type: { type: "STRING" } }),
  ).toBe("converted");
});
