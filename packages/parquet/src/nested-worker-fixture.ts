// First data page from parquet-testing/data/json.parquet.
// Source SHA-256: 594f8dca52a6428e4350d12faeaca9e2155c77511abb9122b935bd5be1a26bf5
// GZIP bytes recompress that exact page for codec integration coverage.
import type { PageHeader, SchemaElement } from "hyparquet/src/types.js";
export const schema: SchemaElement[] = [
  { name: "json_fixture", num_children: 1 },
  {
    type: "BYTE_ARRAY",
    repetition_type: "OPTIONAL",
    name: "json_field",
    converted_type: "JSON",
    logical_type: { type: "JSON" },
  },
];
export const header: PageHeader = {
  type: "DATA_PAGE",
  compressed_page_size: 51,
  uncompressed_page_size: 51,
  data_page_header: {
    num_values: 4,
    encoding: "PLAIN",
    definition_level_encoding: "RLE",
    repetition_level_encoding: "BIT_PACKED",
  },
};
export const rawBytes = [
  2, 0, 0, 0, 3, 7, 7, 0, 0, 0, 123, 34, 97, 34, 58, 49, 125, 16, 0, 0, 0, 123, 34, 97, 34, 58, 49,
  44, 34, 98, 34, 58, 110, 117, 108, 108, 125, 10, 0, 0, 0, 91, 49, 44, 110, 117, 108, 108, 44, 51,
  93,
];
export const gzipBytes = [
  31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 99, 98, 96, 96, 96, 102, 103, 103, 96, 96, 168, 86, 74, 84, 178,
  50, 172, 21, 128, 51, 117, 148, 146, 148, 172, 242, 74, 115, 114, 106, 185, 24, 24, 24, 162, 13,
  117, 64, 108, 29, 227, 88, 0, 60, 77, 140, 131, 51, 0, 0, 0,
];
