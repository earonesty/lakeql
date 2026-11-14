import { isPlainJsonAscii } from "./json-string-size.js";

/** Exact UTF-8 bytes without allocating. Undefined means encoding would replace a lone surrogate. */
export function losslessUtf8Bytes(value: string): number | undefined {
  return measureUtf8(value, true);
}

/** TextEncoder byte extent, including U+FFFD replacement for lone surrogates. */
export function utf8Bytes(value: string): number {
  return measureUtf8(value, false) as number;
}

function measureUtf8(value: string, lossless: boolean): number | undefined {
  if (isPlainJsonAscii(value)) return value.length;
  let bytes = 0;
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x80) bytes++;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        if (lossless) return undefined;
        bytes += 3;
        continue;
      }
      bytes += 4;
      i++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      if (lossless) return undefined;
      bytes += 3;
    } else bytes += 3;
  }
  return bytes;
}
