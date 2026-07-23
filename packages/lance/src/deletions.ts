import { BodyCompression } from "apache-arrow/fb/body-compression";
import { Buffer as ArrowBuffer } from "apache-arrow/fb/buffer";
import { FieldNode } from "apache-arrow/fb/field-node";
import { Message } from "apache-arrow/fb/message";
import { RecordBatch } from "apache-arrow/fb/record-batch";
import { Footer } from "apache-arrow/ipc/metadata/file";
import "apache-arrow/ipc/metadata/message";
import { ByteBuffer } from "flatbuffers";
import { decompress } from "fzstd";
import { LakeqlError } from "lakeql-core";
import type { LanceReadContext } from "./io.js";
import type { LanceFragment } from "./proto.js";

export async function readDeletedRowOffsets(
  context: LanceReadContext,
  root: string,
  fragment: LanceFragment,
): Promise<Set<number>> {
  const deletion = fragment.deletionFile;
  if (deletion === undefined) return new Set();
  if (deletion.fileType !== 0) {
    throw new LakeqlError(
      "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
      "Roaring-bitmap Lance deletion files are not supported",
      {
        fragmentId: fragment.id.toString(),
        deletionFileType: deletion.fileType,
      },
    );
  }
  const path = joinObjectPath(
    root,
    "_deletions",
    `${fragment.id}-${deletion.readVersion}-${deletion.id}.arrow`,
  );
  context.check();
  const head = await context.store.head(path);
  context.check();
  if (head === null) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Missing Lance deletion object ${path}`, {
      path,
    });
  }
  if (head.size <= 0) corrupt("Lance deletion file is empty", { path });
  const lease = await context.readRange(
    path,
    { offset: 0, length: head.size },
    "file_metadata",
    head.size,
  );
  try {
    let offsets: Set<number>;
    try {
      offsets = decodeArrowDeletionFile(
        copyBytes(lease.slice({ offset: 0, length: head.size })),
        fragment.physicalRows,
        Number(deletion.numDeletedRows),
      );
    } catch (cause) {
      if (cause instanceof LakeqlError) throw cause;
      corrupt("Invalid Arrow IPC Lance deletion file", {
        path,
        cause: cause instanceof Error ? cause.message : String(cause),
      });
    }
    return offsets;
  } finally {
    lease.release();
  }
}

export function decodeArrowDeletionFile(
  bytes: Uint8Array,
  physicalRows: number,
  expectedRows: number,
): Set<number> {
  if (bytes.byteLength < 18 || ascii(bytes.subarray(0, 6)) !== "ARROW1") {
    corrupt("Invalid Arrow IPC file magic");
  }
  const footerMagic = ascii(bytes.subarray(bytes.byteLength - 6));
  if (footerMagic !== "ARROW1") corrupt("Invalid Arrow IPC footer magic");
  const footerLength = dataView(bytes).getUint32(bytes.byteLength - 10, true);
  const footerStart = bytes.byteLength - 10 - footerLength;
  if (footerStart < 8) corrupt("Arrow IPC footer is out of bounds");
  const footer = Footer.decode(bytes.subarray(footerStart, bytes.byteLength - 10));
  if (
    footer.numRecordBatches !== 1 ||
    footer.numDictionaries !== 0 ||
    footer.schema.fields.length !== 1
  ) {
    corrupt("Invalid Lance deletion Arrow IPC structure");
  }
  const field = footer.schema.fields[0];
  if (field?.name !== "row_id" || field.type.toString() !== "Uint32" || field.nullable) {
    corrupt("Invalid Lance deletion Arrow IPC schema");
  }
  const block = footer.getRecordBatch(0);
  if (block === null) corrupt("Lance deletion Arrow IPC file has no record batch");
  const metadataStart = block.offset;
  const metadataEnd = metadataStart + block.metaDataLength;
  const bodyStart = metadataEnd;
  const bodyEnd = bodyStart + block.bodyLength;
  if (metadataStart < 8 || bodyEnd > footerStart) {
    corrupt("Lance deletion Arrow IPC block is out of bounds");
  }
  const prefix = dataView(bytes.subarray(metadataStart, metadataEnd));
  const continued = prefix.getUint32(0, true) === 0xffffffff;
  const lengthOffset = continued ? 4 : 0;
  const metadataLength = prefix.getUint32(lengthOffset, true);
  const payloadStart = lengthOffset + 4;
  if (payloadStart + metadataLength > block.metaDataLength) {
    corrupt("Lance deletion Arrow IPC metadata is truncated");
  }
  const message = Message.getRootAsMessage(
    new ByteBuffer(
      bytes.subarray(metadataStart + payloadStart, metadataStart + payloadStart + metadataLength),
    ),
  );
  const batch = message.header(new RecordBatch()) as RecordBatch | null;
  if (
    batch === null ||
    Number(batch.length()) !== expectedRows ||
    batch.nodesLength() !== 1 ||
    batch.buffersLength() !== 2
  ) {
    corrupt("Invalid Lance deletion Arrow IPC record batch");
  }
  const node = batch.nodes(0, new FieldNode());
  if (node === null || Number(node.length()) !== expectedRows || node.nullCount() !== 0n) {
    corrupt("Invalid Lance deletion Arrow IPC field node");
  }
  const validity = batch.buffers(0, new ArrowBuffer());
  const values = batch.buffers(1, new ArrowBuffer());
  if (validity === null || values === null) {
    corrupt("Invalid Lance deletion Arrow IPC buffers");
  }
  const validityOffset = safeInteger(validity.offset(), "Arrow validity-buffer offset");
  const validityLength = safeInteger(validity.length(), "Arrow validity-buffer length");
  if (validityOffset + validityLength > block.bodyLength) {
    corrupt("Lance deletion Arrow validity buffer is out of bounds");
  }
  if (validityLength > 0) {
    const encodedValidity = bytes.subarray(
      bodyStart + validityOffset,
      bodyStart + validityOffset + validityLength,
    );
    const decodedValidity = decodeArrowBuffer(encodedValidity, compressionCodec(batch));
    for (let index = 0; index < expectedRows; index += 1) {
      if (((decodedValidity[Math.floor(index / 8)] ?? 0) & (1 << (index % 8))) === 0) {
        corrupt("Lance deletion Arrow IPC row IDs must not be null");
      }
    }
  }
  const valueOffset = safeInteger(values.offset(), "Arrow value-buffer offset");
  const valueLength = safeInteger(values.length(), "Arrow value-buffer length");
  if (valueOffset + valueLength > block.bodyLength) {
    corrupt("Lance deletion Arrow value buffer is out of bounds");
  }
  const encoded = bytes.subarray(bodyStart + valueOffset, bodyStart + valueOffset + valueLength);
  const decoded = decodeArrowBuffer(encoded, compressionCodec(batch));
  if (decoded.byteLength !== expectedRows * 4) {
    corrupt("Lance deletion Arrow value buffer has the wrong length");
  }
  const view = dataView(decoded);
  const offsets = new Set<number>();
  for (let index = 0; index < expectedRows; index += 1) {
    const value = view.getUint32(index * 4, true);
    if (value >= physicalRows) {
      corrupt("Lance deletion file contains an invalid physical row offset", {
        value,
        physicalRows,
      });
    }
    offsets.add(value);
  }
  if (offsets.size !== expectedRows) {
    corrupt("Lance deletion file contains duplicate row offsets");
  }
  return offsets;
}

function compressionCodec(batch: RecordBatch): number | undefined {
  return batch.compression(new BodyCompression())?.codec();
}

export function decodeArrowBuffer(bytes: Uint8Array, codec: number | undefined): Uint8Array {
  if (codec === undefined) return bytes;
  if (bytes.byteLength < 8) corrupt("Compressed Arrow buffer is truncated");
  const uncompressedLength = dataView(bytes).getBigInt64(0, true);
  if (uncompressedLength === -1n) return bytes.subarray(8);
  const length = safeInteger(uncompressedLength, "Arrow uncompressed buffer length");
  if (codec !== 1) {
    throw new LakeqlError(
      "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
      "Only Zstandard-compressed Arrow deletion buffers are supported",
      { codec },
    );
  }
  const output = decompress(bytes.subarray(8), new Uint8Array(length));
  if (output.byteLength !== length) corrupt("Arrow deletion buffer decompressed length mismatch");
  return output;
}

function safeInteger(value: bigint, label: string): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    corrupt(`Invalid Lance ${label}`, { value: value.toString() });
  }
  return Number(value);
}

function dataView(bytes: Uint8Array): DataView {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function ascii(bytes: Uint8Array): string {
  return String.fromCharCode(...bytes);
}

function joinObjectPath(...parts: string[]): string {
  return parts
    .flatMap((part) => part.split("/"))
    .filter((part) => part.length > 0)
    .join("/");
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}
