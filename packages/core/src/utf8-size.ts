/** Exact UTF-8 bytes without allocating. Undefined means encoding would replace a lone surrogate. */
export function losslessUtf8Bytes(value: string): number | undefined {
  let bytes = 0;
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x80) bytes++;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return undefined;
      bytes += 4;
      i++;
    } else if (code >= 0xdc00 && code <= 0xdfff) return undefined;
    else bytes += 3;
  }
  return bytes;
}
