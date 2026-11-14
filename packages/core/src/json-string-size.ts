// A native scan recognizes strings whose JSON body is already one-byte ASCII.
// This creates neither an escaped copy nor an encoded scratch buffer.
// biome-ignore lint/suspicious/noControlCharactersInRegex: JSON requires escaping every ASCII control code.
const jsonAsciiSpecial = /["\\\x00-\x1f\x80-\uffff]/;

export function isPlainJsonAscii(value: string): boolean {
  return !jsonAsciiSpecial.test(value);
}

/** UTF8 byte length of JSON-escaped text, including quotes, without creating a serialized copy. */
export function jsonStringUtf8Bytes(value: string): number {
  if (isPlainJsonAscii(value)) return value.length + 2;
  let bytes = 2;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (
      code === 34 ||
      code === 92 ||
      code === 8 ||
      code === 9 ||
      code === 10 ||
      code === 12 ||
      code === 13
    )
      bytes += 2;
    else if (code < 32) bytes += 6;
    else if (code < 128) bytes++;
    else if (code < 2048) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index++;
      } else bytes += 6;
    } else if (code >= 0xdc00 && code <= 0xdfff) bytes += 6;
    else bytes += 3;
  }
  return bytes;
}
