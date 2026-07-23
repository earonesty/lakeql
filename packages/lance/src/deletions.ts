import { BodyCompression } from "apache-arrow/fb/body-compression";
import { Buffer as ArrowBuffer } from "apache-arrow/fb/buffer";
import { FieldNode } from "apache-arrow/fb/field-node";
import { Message } from "apache-arrow/fb/message";
import { RecordBatch } from "apache-arrow/fb/record-batch";
import { Footer } from "apache-arrow/ipc/metadata/file";
import "apache-arrow/ipc/metadata/message";
import { ByteBuffer } from "flatbuffers";
import { decompress } from "fzstd";
import { LakeqlError } from "lakeql";
import type { LanceReadContext } from "./io.js";
import type { LanceFragment } from "./proto.js";

export async function readDeletedRowOffsets(
  context: LanceReadContext,
  root: string,
  fragment: LanceFragment,
  requestedOffsets: ReadonlySet<number>,
): Promise<Set<number>> {
  const deletion = fragment.deletionFile;
  if (deletion === undefined || requestedOffsets.size === 0) return new Set();
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
  if (head.size < 18) corrupt("Lance deletion file is truncated", { path, size: head.size });
  const expectedRows = safeInteger(deletion.numDeletedRows, "deletion row count");
  if (expectedRows > fragment.physicalRows) {
    corrupt("Lance deletion row count exceeds fragment rows", {
      expectedRows,
      physicalRows: fragment.physicalRows,
    });
  }
  const edges = await context.readRanges(
    path,
    [
      { offset: 0, length: 6 },
      { offset: head.size - 10, length: 10 },
    ],
    "file_metadata",
    head.size,
    { coalesceGapBytes: 0, maxCoalescedRangeBytes: 10 },
  );
  try {
    if (ascii(edges.slice({ offset: 0, length: 6 })) !== "ARROW1") {
      corrupt("Invalid Arrow IPC file magic", { path });
    }
    const tail = edges.slice({ offset: head.size - 10, length: 10 });
    if (ascii(tail.subarray(4)) !== "ARROW1") {
      corrupt("Invalid Arrow IPC footer magic", { path });
    }
    const footerLength = dataView(tail).getUint32(0, true);
    const footerStart = head.size - 10 - footerLength;
    if (footerStart < 8) corrupt("Arrow IPC footer is out of bounds", { path });
    const footerLease = await context.readRange(
      path,
      { offset: footerStart, length: footerLength },
      "file_metadata",
      head.size,
    );
    try {
      const footer = Footer.decode(
        footerLease.slice({ offset: footerStart, length: footerLength }),
      );
      const block = validatedDeletionFooter(footer);
      const metadataStart = block.offset;
      const metadataEnd = metadataStart + block.metaDataLength;
      const bodyStart = metadataEnd;
      const bodyEnd = bodyStart + block.bodyLength;
      if (metadataStart < 8 || bodyEnd > footerStart) {
        corrupt("Lance deletion Arrow IPC block is out of bounds", { path });
      }
      const metadataLease = await context.readRange(
        path,
        { offset: metadataStart, length: block.metaDataLength },
        "file_metadata",
        head.size,
      );
      try {
        const batch = decodeDeletionBatch(
          metadataLease.slice({ offset: metadataStart, length: block.metaDataLength }),
          expectedRows,
        );
        return await readRequestedDeletionOffsets({
          context,
          path,
          fileSize: head.size,
          bodyStart,
          bodyLength: block.bodyLength,
          batch,
          physicalRows: fragment.physicalRows,
          expectedRows,
          requestedOffsets,
        });
      } finally {
        metadataLease.release();
      }
    } finally {
      footerLease.release();
    }
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    corrupt("Invalid Arrow IPC Lance deletion file", {
      path,
      cause: cause instanceof Error ? cause.message : String(cause),
    });
  } finally {
    edges.release();
  }
}

async function readRequestedDeletionOffsets(options: {
  context: LanceReadContext;
  path: string;
  fileSize: number;
  bodyStart: number;
  bodyLength: number;
  batch: RecordBatch;
  physicalRows: number;
  expectedRows: number;
  requestedOffsets: ReadonlySet<number>;
}): Promise<Set<number>> {
  const validity = options.batch.buffers(0, new ArrowBuffer());
  const values = options.batch.buffers(1, new ArrowBuffer());
  if (validity === null || values === null) {
    corrupt("Invalid Lance deletion Arrow IPC buffers");
  }
  const validityRange = arrowBodyRange(validity, options.bodyStart, options.bodyLength, "validity");
  const valueRange = arrowBodyRange(values, options.bodyStart, options.bodyLength, "value");
  if (validityRange.length > 0) {
    const lease = await options.context.readRange(
      options.path,
      validityRange,
      "data",
      options.fileSize,
    );
    try {
      const encoded = lease.slice(validityRange);
      const codec = compressionCodec(options.batch);
      const memory = options.context.leaseDecodedMemory(
        decodedArrowBufferAllocationBytes(encoded, codec),
      );
      try {
        validateDeletionValidity(decodeArrowBuffer(encoded, codec), options.expectedRows);
      } finally {
        memory.release();
      }
    } finally {
      lease.release();
    }
  }
  const lease = await options.context.readRange(options.path, valueRange, "data", options.fileSize);
  let matches: Set<number>;
  try {
    const encoded = lease.slice(valueRange);
    const codec = compressionCodec(options.batch);
    const memory = options.context.leaseDecodedMemory(
      decodedArrowBufferAllocationBytes(encoded, codec),
    );
    try {
      matches = decodeDeletionValues(
        decodeArrowBuffer(encoded, codec),
        options.physicalRows,
        options.expectedRows,
        options.requestedOffsets,
      );
    } finally {
      memory.release();
    }
  } finally {
    lease.release();
  }
  options.context.accountDecodedMemory(matches.size * 8);
  return matches;
}

function arrowBodyRange(
  buffer: ArrowBuffer,
  bodyStart: number,
  bodyLength: number,
  label: string,
): { offset: number; length: number } {
  const offset = safeInteger(buffer.offset(), `Arrow ${label}-buffer offset`);
  const length = safeInteger(buffer.length(), `Arrow ${label}-buffer length`);
  if (offset + length > bodyLength) {
    corrupt(`Lance deletion Arrow ${label} buffer is out of bounds`);
  }
  return { offset: bodyStart + offset, length };
}

function validatedDeletionFooter(footer: Footer) {
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
  return block;
}

function decodeDeletionBatch(metadata: Uint8Array, expectedRows: number): RecordBatch {
  const prefix = dataView(metadata);
  const continued = prefix.getUint32(0, true) === 0xffffffff;
  const lengthOffset = continued ? 4 : 0;
  const metadataLength = prefix.getUint32(lengthOffset, true);
  const payloadStart = lengthOffset + 4;
  if (payloadStart + metadataLength > metadata.byteLength) {
    corrupt("Lance deletion Arrow IPC metadata is truncated");
  }
  const message = Message.getRootAsMessage(
    new ByteBuffer(metadata.subarray(payloadStart, payloadStart + metadataLength)),
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
  return batch;
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
  const block = validatedDeletionFooter(footer);
  const metadataStart = block.offset;
  const metadataEnd = metadataStart + block.metaDataLength;
  const bodyStart = metadataEnd;
  const bodyEnd = bodyStart + block.bodyLength;
  if (metadataStart < 8 || bodyEnd > footerStart) {
    corrupt("Lance deletion Arrow IPC block is out of bounds");
  }
  const batch = decodeDeletionBatch(bytes.subarray(metadataStart, metadataEnd), expectedRows);
  const validity = batch.buffers(0, new ArrowBuffer());
  const values = batch.buffers(1, new ArrowBuffer());
  if (validity === null || values === null) {
    corrupt("Invalid Lance deletion Arrow IPC buffers");
  }
  const validityRange = arrowBodyRange(validity, bodyStart, block.bodyLength, "validity");
  if (validityRange.length > 0) {
    const encodedValidity = bytes.subarray(
      validityRange.offset,
      validityRange.offset + validityRange.length,
    );
    const decodedValidity = decodeArrowBuffer(encodedValidity, compressionCodec(batch));
    validateDeletionValidity(decodedValidity, expectedRows);
  }
  const valueRange = arrowBodyRange(values, bodyStart, block.bodyLength, "value");
  const encoded = bytes.subarray(valueRange.offset, valueRange.offset + valueRange.length);
  const decoded = decodeArrowBuffer(encoded, compressionCodec(batch));
  const offsets = decodeDeletionValues(decoded, physicalRows, expectedRows);
  if (offsets.size !== expectedRows) {
    corrupt("Lance deletion file contains duplicate row offsets");
  }
  return offsets;
}

function validateDeletionValidity(decoded: Uint8Array, expectedRows: number): void {
  for (let index = 0; index < expectedRows; index += 1) {
    if (((decoded[Math.floor(index / 8)] ?? 0) & (1 << (index % 8))) === 0) {
      corrupt("Lance deletion Arrow IPC row IDs must not be null");
    }
  }
}

function decodeDeletionValues(
  decoded: Uint8Array,
  physicalRows: number,
  expectedRows: number,
  requestedOffsets?: ReadonlySet<number>,
): Set<number> {
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
    if (requestedOffsets === undefined || requestedOffsets.has(value)) offsets.add(value);
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

function decodedArrowBufferAllocationBytes(bytes: Uint8Array, codec: number | undefined): number {
  if (codec === undefined) return 0;
  if (bytes.byteLength < 8) corrupt("Compressed Arrow buffer is truncated");
  const uncompressedLength = dataView(bytes).getBigInt64(0, true);
  return uncompressedLength === -1n
    ? 0
    : safeInteger(uncompressedLength, "Arrow uncompressed buffer length");
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

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}
