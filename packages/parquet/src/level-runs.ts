import { LakeqlError } from "lakeql-core";

/** Visit bounded hybrid RLE/bit-packed runs without materializing level arrays. */
export function visitLevelRuns(
  reader: { view: DataView; offset: number },
  width: number,
  outputLength: number,
  visit: (value: number, start: number, count: number) => void,
  encodedLength?: number,
  requireComplete = false,
): void {
  const invalid = () => new LakeqlError("LAKEQL_VALIDATION_ERROR", "Malformed Parquet level runs");
  if (
    !Number.isInteger(width) ||
    width < 0 ||
    width > 32 ||
    !Number.isSafeInteger(outputLength) ||
    outputLength < 0 ||
    outputLength > 0xffffffff ||
    !Number.isSafeInteger(reader.offset) ||
    reader.offset < 0
  )
    throw invalid();
  let length = encodedLength;
  if (length === undefined) {
    if (reader.offset + 4 > reader.view.byteLength) throw invalid();
    length = reader.view.getUint32(reader.offset, true);
    reader.offset += 4;
  }
  const end = reader.offset + length;
  if (!Number.isSafeInteger(length) || length < 0 || end > reader.view.byteLength) throw invalid();
  let seen = 0;
  const byte = () => {
    if (reader.offset >= end) throw invalid();
    return reader.view.getUint8(reader.offset++);
  };
  while (seen < outputLength) {
    let header = 0;
    let factor = 1;
    for (let index = 0; ; index += 1) {
      if (index === 5) throw invalid();
      const part = byte();
      header += (part & 127) * factor;
      if (header > 0xffffffff) throw invalid();
      if (part < 128) break;
      factor *= 128;
    }
    const run = Math.floor(header / 2);
    if (run === 0) throw invalid();
    if (header % 2 === 0) {
      if (run > outputLength - seen) throw invalid();
      let value = 0;
      for (let index = 0; index < Math.ceil(width / 8); index += 1)
        value += byte() * 2 ** (8 * index);
      if (value >= 2 ** width) throw invalid();
      visit(value, seen, run);
      seen += run;
    } else {
      const count = run * 8;
      const bytes = run * width;
      // Writers may pad the final bit-packed run by more than one group.
      // Page value count bounds output; encoded extent bounds input consumption.
      // Never allocate or visit the padded values.
      if (count > 0x7fffffff || reader.offset + bytes > end) throw invalid();
      const start = reader.offset;
      const needed = Math.min(count, outputLength - seen);
      if (width === 0) {
        visit(0, seen, needed);
        seen += needed;
      }
      for (let index = 0; width > 0 && index < needed; index += 1) {
        let value = 0;
        for (let bit = 0; bit < width; bit += 1) {
          const position = index * width + bit;
          value +=
            ((reader.view.getUint8(start + Math.floor(position / 8)) >> (position % 8)) & 1) *
            2 ** bit;
        }
        visit(value, seen++, 1);
      }
      reader.offset += bytes;
    }
  }
  if (requireComplete && reader.offset !== end) throw invalid();
  reader.offset = end;
}
