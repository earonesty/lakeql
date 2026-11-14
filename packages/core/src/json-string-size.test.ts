import { expect, it } from "vitest";
import { jsonStringUtf8Bytes } from "./json-string-size.js";

it("matches JSON UTF8 sizing for every UTF16 code unit", () => {
  const encoder = new TextEncoder();
  for (let code = 0; code <= 0xffff; code++) {
    const value = String.fromCharCode(code);
    const expected = encoder.encode(JSON.stringify(value)).byteLength;
    if (jsonStringUtf8Bytes(value) !== expected) throw new Error(`Mismatch at ${code}`);
  }
});

it("matches boundary surrogate pairs, escapes, and long mixed text", () => {
  const encoder = new TextEncoder();
  const cases = [
    "",
    "plain ASCII",
    '"\\\b\t\n\f\r\u0000',
    "é猫😀",
    "\ud800\udc00",
    "\udbff\udfff",
    "\ud800\ud800\udc00\udfff",
    "猫😀é\ud800\n".repeat(10000),
  ];
  for (const value of cases)
    expect(jsonStringUtf8Bytes(value)).toBe(encoder.encode(JSON.stringify(value)).byteLength);
});
