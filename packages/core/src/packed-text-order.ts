import type { Vector } from "./batch.js";
import { packedByteLength, packedByteStart } from "./packed-byte-range.js";

export function supportsPackedTextOrder(vector: Vector): boolean {
  return (
    vector.type === "utf8-view" ||
    (vector.type === "dict" && vector.dictionary.type === "utf8-view")
  );
}

/** Compare valid UTF8 in UTF16 order without decoding or retaining keys. Undefined
 * requests the ordinary scalar comparator for malformed UTF8 encountered before
 * ordering is decided. Later bytes cannot change an earlier scalar difference.
 * Capabilities: flat packed UTF8 or one dictionary indirection; all others fall back.
 */
export function comparePackedTextPrefix(
  vector: Vector,
  left: number,
  right: number,
): number | undefined {
  if (!supportsPackedTextOrder(vector)) return undefined;
  if (!Number.isInteger(left) || !Number.isInteger(right) || left < 0 || right < 0)
    return undefined;
  if (vector.type === "dict") {
    if (
      left >= vector.indices.length ||
      right >= vector.indices.length ||
      vector.valid?.[left] === 0 ||
      vector.valid?.[right] === 0
    )
      return undefined;
    left = vector.indices[left] as number;
    right = vector.indices[right] as number;
    vector = vector.dictionary;
  }
  if (
    vector.type !== "utf8-view" ||
    left >= vector.length ||
    right >= vector.length ||
    vector.valid?.[left] === 0 ||
    vector.valid?.[right] === 0
  )
    return undefined;
  const leftLength = packedByteLength(vector, left);
  const rightLength = packedByteLength(vector, right);
  const leftStart = packedByteStart(vector, left);
  const rightStart = packedByteStart(vector, right);
  if (leftStart === rightStart && leftLength === rightLength) return 0;
  const leftEnd = leftStart + leftLength;
  const rightEnd = rightStart + rightLength;
  let a = leftStart;
  let b = rightStart;
  // Native decoding is faster for long common prefixes. Bound speculative
  // scalar work independently of value width before taking that path.
  const prefixEnd = leftStart + 16;
  while (a < leftEnd && b < rightEnd) {
    if (a >= prefixEnd) return undefined;
    const first = vector.data[a] as number;
    const second = vector.data[b] as number;
    if (first < 128 && second < 128) {
      if (first !== second) return first < second ? -1 : 1;
      a++;
      b++;
      continue;
    }
    const firstScalar = utf8Scalar(vector.data, a, leftEnd);
    const secondScalar = utf8Scalar(vector.data, b, rightEnd);
    if (firstScalar < 0 || secondScalar < 0) return undefined;
    const firstCodepoint = firstScalar >>> 3;
    const secondCodepoint = secondScalar >>> 3;
    if (firstCodepoint !== secondCodepoint) {
      // Supplementary scalars begin with a high surrogate in JS strings.
      // Equal high surrogates order by low surrogate, hence by code point.
      const firstUnit =
        firstCodepoint > 0xffff ? 0xd800 + ((firstCodepoint - 0x10000) >>> 10) : firstCodepoint;
      const secondUnit =
        secondCodepoint > 0xffff ? 0xd800 + ((secondCodepoint - 0x10000) >>> 10) : secondCodepoint;
      return firstUnit === secondUnit
        ? firstCodepoint < secondCodepoint
          ? -1
          : 1
        : firstUnit < secondUnit
          ? -1
          : 1;
    }
    a += firstScalar & 7;
    b += secondScalar & 7;
  }
  return a === leftEnd ? (b === rightEnd ? 0 : -1) : 1;
}

/** Pack a valid scalar and its byte width into one number; no cursor allocation. */
function utf8Scalar(data: Uint8Array, start: number, end: number): number {
  const first = data[start] as number;
  if (first < 128) return (first << 3) | 1;
  const width =
    first >= 0xc2 && first <= 0xdf
      ? 2
      : first >= 0xe0 && first <= 0xef
        ? 3
        : first >= 0xf0 && first <= 0xf4
          ? 4
          : 0;
  if (width === 0 || start + width > end) return -1;
  let scalar = first & (0x7f >>> width);
  for (let i = 1; i < width; i++) {
    const next = data[start + i] as number;
    if ((next & 0xc0) !== 0x80) return -1;
    scalar = (scalar << 6) | (next & 0x3f);
  }
  if (
    scalar < (width === 2 ? 0x80 : width === 3 ? 0x800 : 0x10000) ||
    scalar > 0x10ffff ||
    (scalar >= 0xd800 && scalar <= 0xdfff)
  )
    return -1;
  return (scalar << 3) | width;
}

/** Stop speculative prefix work after repeated unresolved comparisons. State is
 * constant-sized and local to one ordering operation; no decoded keys survive. */
export class PackedTextOrderProbe {
  private misses = 0;

  compare(vector: Vector, left: number, right: number): number | undefined {
    if (this.misses >= 8) return undefined;
    const result = comparePackedTextPrefix(vector, left, right);
    if (result !== undefined) this.misses = 0;
    else if (vector.type === "utf8-view") {
      if (vector.valid?.[left] !== 0 && vector.valid?.[right] !== 0) this.misses++;
    } else if (
      vector.type === "dict" &&
      vector.dictionary.type === "utf8-view" &&
      vector.valid?.[left] !== 0 &&
      vector.valid?.[right] !== 0 &&
      vector.dictionary.valid?.[vector.indices[left] as number] !== 0 &&
      vector.dictionary.valid?.[vector.indices[right] as number] !== 0
    )
      this.misses++;

    return result;
  }
}
