import { expect, it } from "vitest";
import { losslessUtf8Bytes } from "./utf8-size.js";

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
