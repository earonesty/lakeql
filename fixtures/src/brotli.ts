// Real serialized Parquet fixture, compressed by Node's independent Brotli encoder.
// Keep this Node-only producer outside Worker bundles; pass its bytes as a binding.
import { brotliCompressSync } from "node:zlib";
import { parquetWriteBuffer } from "hyparquet-writer";

export function brotliFixtureRows() {
  return Array.from({ length: 513 }, (_, index) => ({
    id: index,
    text: index % 7 === 0 ? null : `dictionary text ${index % 19} 猫`,
  }));
}

export function createBrotliFixture(): Uint8Array {
  const rows = brotliFixtureRows();
  return new Uint8Array(
    parquetWriteBuffer({
      codec: "BROTLI",
      compressors: { BROTLI: (input) => brotliCompressSync(input) },
      rowGroupSize: 127,
      pageSize: 256,
      columnData: [
        { name: "id", type: "INT32", data: rows.map((row) => row.id) },
        { name: "text", type: "STRING", data: rows.map((row) => row.text) },
      ],
    }),
  );
}
