import { expect, it } from "vitest";
import { losslessUtf8Bytes, utf8Bytes } from "./utf8-size.js";

it.each([
  "",
  "ascii\0",
  "é",
  "中",
  "😀",
  "aé中😀",
  "\ufffd",
  "\ud7ff\ue000",
])("measures lossless UTF-8 for %j", (value) => {
  expect(losslessUtf8Bytes(value)).toBe(new TextEncoder().encode(value).byteLength);
});
it.each([
  "\ud800",
  "\udfff",
  "x\ud800y",
  "\ud800\ud800",
  "\udc00\ud800",
])("declines lossy surrogate encoding for %j", (value) => {
  expect(losslessUtf8Bytes(value)).toBeUndefined();
});

it("matches TextEncoder replacement sizing for every UTF16 code unit and surrogate boundary", () => {
  const encoder = new TextEncoder();
  for (let code = 0; code <= 0xffff; code++) {
    const text = String.fromCharCode(code);
    if (utf8Bytes(text) !== encoder.encode(text).length) throw Error(`Mismatch at ${code}`);
  }
  for (const text of ["\ud800\udc00", "\udbff\udfff", "\ud800x\udfff", "\ud800\ud800\udc00"])
    expect(utf8Bytes(text)).toBe(encoder.encode(text).length);
});
