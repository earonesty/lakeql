/** FNV-1a over TextEncoder-compatible UTF-8 without a full encoded copy. */
export function fnv1a64Utf8(input: string): bigint {
  let hash = 0xcbf29ce484222325n;
  const byte = (value: number) => {
    hash = BigInt.asUintN(64, (hash ^ BigInt(value)) * 0x100000001b3n);
  };
  for (let index = 0; index < input.length; index++) {
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
