/** FNV-1a over TextEncoder-compatible UTF-8 without a full encoded copy.
 * ASCII runs use exact two-word arithmetic; the general UTF8 path retains the
 * measured BigInt implementation after the first non-ASCII code unit.
 */
export function fnv1a64Utf8(input: string): bigint {
  let high = 0xcbf29ce4;
  let low = 0x84222325;
  for (let index = 0; index < input.length; index++) {
    const value = input.charCodeAt(index);
    if (value >= 0x80)
      return hashUtf8(
        input,
        index,
        index === 0 ? 0xcbf29ce484222325n : (BigInt(high) << 32n) | BigInt(low),
      );
    low = (low ^ value) >>> 0;
    // FNV prime = 2^40 + 435. The low product is below 2^41, so
    // Number arithmetic preserves its carry exactly.
    const product = low * 435;
    high = (Math.imul(high, 435) + Math.floor(product / 0x100000000) + (low << 8)) >>> 0;
    low = product >>> 0;
  }
  return input.length === 0 ? 0xcbf29ce484222325n : (BigInt(high) << 32n) | BigInt(low);
}

function hashUtf8(input: string, index: number, hash: bigint): bigint {
  const byte = (value: number) => {
    hash = BigInt.asUintN(64, (hash ^ BigInt(value)) * 0x100000001b3n);
  };
  for (; index < input.length; index++) {
    let point = input.charCodeAt(index);
    if (point >= 0xd800 && point <= 0xdbff) {
      const low = input.charCodeAt(index + 1);
      if (low >= 0xdc00 && low <= 0xdfff) {
        point = 0x10000 + ((point - 0xd800) << 10) + low - 0xdc00;
        index++;
      } else point = 0xfffd;
    } else if (point >= 0xdc00 && point <= 0xdfff) point = 0xfffd;
    if (point < 0x80) byte(point);
    else if (point < 0x800) {
      byte(0xc0 | (point >> 6));
      byte(0x80 | (point & 0x3f));
    } else if (point < 0x10000) {
      byte(0xe0 | (point >> 12));
      byte(0x80 | ((point >> 6) & 0x3f));
      byte(0x80 | (point & 0x3f));
    } else {
      byte(0xf0 | (point >> 18));
      byte(0x80 | ((point >> 12) & 0x3f));
      byte(0x80 | ((point >> 6) & 0x3f));
      byte(0x80 | (point & 0x3f));
    }
  }
  return hash;
}
