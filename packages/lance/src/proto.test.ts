import { describe, expect, it } from "vitest";
import {
  parseColumnMetadata,
  parseFileDescriptor,
  parseIndexSection,
  parseManifest,
  parseRowIdSequence,
} from "./proto.js";

describe("Lance protobuf compatibility decoder", () => {
  it("decodes scalar-index metadata and self-described file schemas", () => {
    const uuid = Uint8Array.from({ length: 16 }, (_value, index) => index);
    const index = message(
      bytesField(1, message(bytesField(1, uuid), scalar(99, 1))),
      packed(2, [7]),
      scalar(2, 8),
      text(3, "serial_btree"),
      scalar(4, 12),
      bytesField(6, message(text(1, "/lance.table.BTreeIndexDetails"), scalar(99, 1))),
      scalar(7, 0),
      bytesField(10, message(text(1, "page_data.lance"), scalar(2, 4_096))),
      scalar(99, 1),
    );

    expect(parseIndexSection(message(bytesField(1, index), scalar(99, 1)))).toEqual([
      {
        uuid: "00010203-0405-0607-0809-0a0b0c0d0e0f",
        fields: [7, 8],
        name: "serial_btree",
        datasetVersion: 12n,
        detailsTypeUrl: "/lance.table.BTreeIndexDetails",
        indexVersion: 0,
        files: [{ path: "page_data.lance", sizeBytes: 4_096 }],
      },
    ]);

    const field = message(
      text(2, "serial"),
      scalar(3, 7),
      scalar(4, -1),
      text(5, "int64"),
      scalar(6, 1),
    );
    const schema = message(
      bytesField(1, field),
      bytesField(
        5,
        message(text(1, "batch_size"), bytesField(2, new TextEncoder().encode("4096"))),
      ),
    );
    expect(
      parseFileDescriptor(message(bytesField(1, schema), scalar(2, 64), scalar(99, 1))),
    ).toEqual({
      fields: [
        {
          id: 7,
          name: "serial",
          parentId: -1,
          logicalType: "int64",
          nullable: true,
        },
      ],
      metadata: { batch_size: new TextEncoder().encode("4096") },
      length: 64,
    });
  });

  it("rejects malformed scalar-index UUIDs", () => {
    const index = message(bytesField(1, message(bytesField(1, Uint8Array.of(1)))));
    expect(() => parseIndexSection(message(bytesField(1, index)))).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });

  it("preserves absent optional scalar-index and file-descriptor fields", () => {
    const uuid = Uint8Array.from({ length: 16 }, () => 1);
    const index = message(bytesField(1, message(bytesField(1, uuid))));
    expect(parseIndexSection(message(bytesField(1, index)))).toEqual([
      {
        uuid: "01010101-0101-0101-0101-010101010101",
        fields: [],
        name: "",
        datasetVersion: 0n,
        detailsTypeUrl: "",
        files: [],
      },
    ]);
    expect(parseFileDescriptor(new Uint8Array())).toEqual({
      fields: [],
      metadata: {},
      length: 0,
    });
    expect(
      parseFileDescriptor(
        message(
          bytesField(
            1,
            message(
              bytesField(5, message()),
              bytesField(5, message(text(1, "key-only"))),
              bytesField(5, message(bytesField(2, Uint8Array.of(9)))),
              scalar(99, 1),
            ),
          ),
        ),
      ),
    ).toEqual({
      fields: [],
      metadata: { "": Uint8Array.of(9), "key-only": new Uint8Array() },
      length: 0,
    });
  });

  it("decodes manifest fields, files, deletions, external row IDs, and format identity", () => {
    const field = message(
      text(2, "serial"),
      scalar(3, 7),
      scalar(4, -1),
      text(5, "int64"),
      scalar(6, 1),
      fixed64(99, 1n),
      fixed32(100, 2),
    );
    const dataFile = message(
      text(1, "data/a.lance"),
      packed(2, [7, 8]),
      scalar(2, 9),
      packed(3, [0, 1]),
      scalar(4, 2),
      scalar(5, 0),
      scalar(6, 4_096),
    );
    const deletion = message(scalar(1, 2), scalar(2, 4), scalar(3, 9), scalar(4, 3));
    const external = message(text(1, "_rowids/a"), scalar(2, 12), scalar(3, 88));
    const fragment = message(
      scalar(1, 11),
      bytesField(2, dataFile),
      bytesField(3, deletion),
      scalar(4, 10),
      bytesField(5, Uint8Array.of(1, 2)),
      bytesField(6, external),
    );
    const manifest = parseManifest(
      message(
        bytesField(1, field),
        bytesField(2, fragment),
        scalar(3, 5),
        scalar(9, 3),
        bytesField(13, message(text(1, "pylance"), text(2, "8.0.0"))),
        bytesField(15, message(text(1, "lance"), text(2, "2.0"))),
        scalar(100, 1),
      ),
    );

    expect(manifest).toEqual({
      fields: [
        {
          id: 7,
          name: "serial",
          parentId: -1,
          logicalType: "int64",
          nullable: true,
        },
      ],
      fragments: [
        {
          id: 11n,
          physicalRows: 10,
          files: [
            {
              path: "data/a.lance",
              fields: [7, 8, 9],
              columnIndices: [0, 1],
              fileMajorVersion: 2,
              fileMinorVersion: 0,
              fileSizeBytes: 4_096,
            },
          ],
          deletionFile: {
            fileType: 2,
            readVersion: 4n,
            id: 9n,
            numDeletedRows: 3n,
          },
          inlineRowIds: Uint8Array.of(1, 2),
          externalRowIds: { path: "_rowids/a", offset: 12, size: 88 },
        },
      ],
      version: 5n,
      readerFeatureFlags: 3n,
      writerLibrary: "pylance",
      writerVersion: "8.0.0",
      dataFileFormat: "lance",
      dataStorageVersion: "2.0",
    });
  });

  it("decodes all stable row-ID segment and integer-array forms", () => {
    const range = bytesField(1, message(scalar(1, 10), scalar(2, 12)));
    const holesArray = bytesField(
      1,
      message(scalar(1, 20), bytesField(2, Uint8Array.of(1, 0, 3, 0))),
    );
    const holes = bytesField(2, message(scalar(1, 20), scalar(2, 25), bytesField(3, holesArray)));
    const bitmap = bytesField(
      3,
      message(scalar(1, 30), scalar(2, 34), bytesField(3, Uint8Array.of(0xa0))),
    );
    const delta32Array = bytesField(
      2,
      message(scalar(1, 40), bytesField(2, littleEndian([1, 4], 4))),
    );
    const sorted = bytesField(4, delta32Array);
    const rawArray = bytesField(3, message(bytesField(2, littleEndian([60, 55], 8))));
    const array = bytesField(5, rawArray);
    const sequence = parseRowIdSequence(
      message(
        bytesField(1, range),
        bytesField(1, holes),
        bytesField(1, bitmap),
        bytesField(1, sorted),
        bytesField(1, array),
        scalar(99, 1),
      ),
    );

    expect(sequence).toEqual([
      { kind: "range", start: 10n, end: 12n },
      { kind: "range_with_holes", start: 20n, end: 25n, holes: [21n, 23n] },
      {
        kind: "range_with_bitmap",
        start: 30n,
        end: 34n,
        bitmap: Uint8Array.of(0xa0),
      },
      { kind: "sorted_array", values: [41n, 44n] },
      { kind: "array", values: [60n, 55n] },
    ]);
  });

  it("decodes flat, nullable, binary, constant, and compressed page encodings", () => {
    const flat = arrayEncoding(
      1,
      message(
        scalar(1, 64),
        bytesField(2, message(scalar(1, 2), scalar(2, 1))),
        bytesField(3, message(text(1, "zstd"))),
      ),
    );
    const noNulls = arrayEncoding(2, bytesField(1, bytesField(1, flatEncoding(32, 0))));
    const someNulls = arrayEncoding(
      2,
      bytesField(2, message(bytesField(1, flatEncoding(1, 0)), bytesField(2, flatEncoding(16, 1)))),
    );
    const allNulls = arrayEncoding(2, bytesField(3, new Uint8Array()));
    const binary = arrayEncoding(
      6,
      message(bytesField(1, flatEncoding(64, 0)), bytesField(2, flatEncoding(8, 1)), scalar(3, 9)),
    );
    const constant = arrayEncoding(13, bytesField(1, Uint8Array.of(9, 8)));
    const encodings = [flat, noNulls, someNulls, allNulls, binary, constant];
    const pages = encodings.map((encoding, index) =>
      bytesField(
        2,
        message(
          packed(1, [100 + index]),
          packed(2, [20]),
          scalar(3, 8),
          bytesField(4, fileEncoding(encoding)),
          scalar(5, index),
          scalar(90, 1),
        ),
      ),
    );
    const metadata = parseColumnMetadata(
      message(...pages, packed(3, [10, 20]), scalar(4, 5), packed(4, [6]), scalar(99, 1)),
    );

    expect(metadata.bufferOffsets).toEqual([10, 20]);
    expect(metadata.bufferSizes).toEqual([5, 6]);
    expect(metadata.pages.map((page) => page.encoding)).toEqual([
      {
        kind: "flat",
        bitsPerValue: 64,
        bufferIndex: 2,
        bufferType: 1,
        compression: "zstd",
      },
      {
        kind: "nullable",
        mode: "no_nulls",
        values: { kind: "flat", bitsPerValue: 32, bufferIndex: 0, bufferType: 1 },
      },
      {
        kind: "nullable",
        mode: "some_nulls",
        validity: { kind: "flat", bitsPerValue: 1, bufferIndex: 0, bufferType: 1 },
        values: { kind: "flat", bitsPerValue: 16, bufferIndex: 1, bufferType: 1 },
      },
      { kind: "nullable", mode: "all_nulls" },
      {
        kind: "binary",
        indices: { kind: "flat", bitsPerValue: 64, bufferIndex: 0, bufferType: 1 },
        bytes: { kind: "flat", bitsPerValue: 8, bufferIndex: 1, bufferType: 1 },
        nullAdjustment: 9n,
      },
      { kind: "constant", value: Uint8Array.of(9, 8) },
    ]);
  });

  it.each([
    {
      name: "mismatched column buffers",
      bytes: message(packed(3, [1]), packed(4, [1, 2])),
      code: "LAKEQL_LANCE_READ_ERROR",
    },
    {
      name: "page without an encoding",
      bytes: message(bytesField(2, message(scalar(3, 1)))),
      code: "LAKEQL_LANCE_READ_ERROR",
    },
    {
      name: "mismatched page buffers",
      bytes: message(
        bytesField(
          2,
          message(packed(1, [1]), packed(2, [1, 2]), bytesField(4, fileEncoding(flatEncoding()))),
        ),
      ),
      code: "LAKEQL_LANCE_READ_ERROR",
    },
    {
      name: "deferred page encoding",
      bytes: pageMetadata(bytesField(1, new Uint8Array())),
      code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
    },
    {
      name: "unknown Any type",
      bytes: pageMetadata(
        bytesField(2, bytesField(1, message(text(1, "other"), bytesField(2, flatEncoding())))),
      ),
      code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
    },
    {
      name: "unknown array encoding",
      bytes: pageMetadata(fileEncoding(bytesField(12, new Uint8Array()))),
      code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
    },
    {
      name: "incomplete binary",
      bytes: pageMetadata(fileEncoding(arrayEncoding(6, bytesField(1, flatEncoding())))),
      code: "LAKEQL_LANCE_READ_ERROR",
    },
    {
      name: "empty nullable",
      bytes: pageMetadata(fileEncoding(arrayEncoding(2, new Uint8Array()))),
      code: "LAKEQL_LANCE_READ_ERROR",
    },
  ])("rejects $name", ({ bytes, code }) => {
    expect(() => parseColumnMetadata(bytes)).toThrowError(expect.objectContaining({ code }));
  });

  it.each([
    Uint8Array.of(0),
    Uint8Array.of(0x08),
    Uint8Array.of(0x0e),
    Uint8Array.of(0x12, 0x02, 0xff),
    Uint8Array.from({ length: 11 }, () => 0x80),
  ])("rejects malformed protobuf bytes %#", (bytes) => {
    expect(() => parseManifest(bytes)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });

  it("rejects truncated integer-array encodings", () => {
    const truncated16 = bytesField(
      4,
      bytesField(1, message(scalar(1, 0), bytesField(2, Uint8Array.of(1)))),
    );
    const truncated64 = bytesField(5, bytesField(3, message(bytesField(2, Uint8Array.of(1)))));
    for (const segment of [truncated16, truncated64]) {
      expect(() => parseRowIdSequence(bytesField(1, segment))).toThrowError(
        expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
      );
    }
  });
});

function pageMetadata(encoding: Uint8Array): Uint8Array {
  return message(bytesField(2, message(scalar(3, 1), bytesField(4, encoding))));
}

function flatEncoding(bits = 8, bufferIndex = 0): Uint8Array {
  return arrayEncoding(
    1,
    message(scalar(1, bits), bytesField(2, message(scalar(1, bufferIndex), scalar(2, 1)))),
  );
}

function fileEncoding(encoding: Uint8Array): Uint8Array {
  const any = message(text(1, "/lance.encodings.ArrayEncoding"), bytesField(2, encoding));
  return bytesField(2, bytesField(1, any));
}

function arrayEncoding(field: number, value: Uint8Array): Uint8Array {
  return bytesField(field, value);
}

function text(field: number, value: string): Uint8Array {
  return bytesField(field, new TextEncoder().encode(value));
}

function scalar(field: number, value: number | bigint): Uint8Array {
  return concat(varint(BigInt(field << 3)), varint(BigInt.asUintN(64, BigInt(value))));
}

function packed(field: number, values: readonly number[]): Uint8Array {
  return bytesField(field, concat(...values.map((value) => varint(BigInt(value)))));
}

function bytesField(field: number, value: Uint8Array): Uint8Array {
  return concat(varint(BigInt((field << 3) | 2)), varint(BigInt(value.byteLength)), value);
}

function fixed64(field: number, value: bigint): Uint8Array {
  return concat(varint(BigInt((field << 3) | 1)), littleEndian([Number(value)], 8));
}

function fixed32(field: number, value: number): Uint8Array {
  return concat(varint(BigInt((field << 3) | 5)), littleEndian([value], 4));
}

function message(...fields: Uint8Array[]): Uint8Array {
  return concat(...fields);
}

function varint(value: bigint): Uint8Array {
  const output: number[] = [];
  let remaining = value;
  do {
    const byte = Number(remaining & 0x7fn);
    remaining >>= 7n;
    output.push(remaining === 0n ? byte : byte | 0x80);
  } while (remaining !== 0n);
  return Uint8Array.from(output);
}

function littleEndian(values: readonly number[], width: 2 | 4 | 8): Uint8Array {
  const bytes = new Uint8Array(values.length * width);
  const view = new DataView(bytes.buffer);
  values.forEach((value, index) => {
    if (width === 2) view.setUint16(index * width, value, true);
    else if (width === 4) view.setUint32(index * width, value, true);
    else view.setBigUint64(index * width, BigInt(value), true);
  });
  return bytes;
}

function concat(...values: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(values.reduce((total, value) => total + value.byteLength, 0));
  let offset = 0;
  for (const value of values) {
    output.set(value, offset);
    offset += value.byteLength;
  }
  return output;
}
