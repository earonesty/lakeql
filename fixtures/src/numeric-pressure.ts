// Independent Node compression keeps the real fixture producer outside Workers.
import { zstdCompressSync } from "node:zlib";
import { parquetWriteBuffer } from "hyparquet-writer";

export function createNumericPressureFixture(): Uint8Array {
  return new Uint8Array(
    parquetWriteBuffer({
      codec: "ZSTD",
      compressors: { ZSTD: (input) => zstdCompressSync(input) },
      columnData: [
        { name: "a", type: "INT32", encoding: "PLAIN", data: [0] },
        { name: "b", type: "DOUBLE", encoding: "PLAIN", data: [null] },
      ],
    }),
  );
}
