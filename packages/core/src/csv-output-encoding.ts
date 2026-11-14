import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { intervalToString, isIntervalValue } from "./interval.js";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import { isPlainJsonAscii } from "./json-string-size.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import {
  scalarStringAllocationBytes,
  scalarStringWithAdmission,
} from "./scalar-cast-allocation.js";
import { isTimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";
import { utf8Bytes } from "./utf8-size.js";

const empty = new Uint8Array(0);
interface Cell {
  value: string | Uint8Array;
  start: number;
  end: number;
  quoted: boolean;
}

/** Admit the cell directory, nested backing and final output before allocation.
 * Primitive text writes directly, without a per-cell UTF8 buffer or CSV row string.
 * Nested cells avoid a JSON-safe clone. Scalar formatting and
 * nested JSON traversal workspace retain the JSON encoder's separate limitations.
 */
export function encodeOwnedCsvOutput(
  row: Row | undefined,
  columns: readonly string[],
  header: boolean,
  scope: MemoryScope,
): OwnedMemory<Uint8Array> {
  const stage = scope.childScope();
  try {
    const lines = Number(header) + Number(row !== undefined);
    const count = lines * columns.length;
    const cells = stage.allocate(
      c.array + count * (c.arraySlot + c.object + 4 * c.property),
      "CSV cell directory",
      () => new Array<Cell>(count),
    ).value;
    let total = lines * (Math.max(0, columns.length - 1) + 1);
    for (let line = 0; line < lines; line++)
      for (let index = 0; index < columns.length; index++) {
        const key = columns[index] ?? "";
        let value: unknown = header && line === 0 ? key : row?.[key];
        let prepared: string | Uint8Array = empty;
        let start = 0;
        let end = 0;
        if (isTimestampValue(value)) {
          const timestamp = value;
          value = stage.allocate(
            scalarStringAllocationBytes(timestamp),
            "CSV timestamp formatting",
            () => timestamp.toJSON(),
          ).value;
        } else if (isIntervalValue(value)) value = intervalToString(value, stage);
        if (
          value !== null &&
          value !== undefined &&
          typeof value !== "function" &&
          typeof value !== "symbol"
        ) {
          if (typeof value === "object") {
            prepared = stage.adopt(encodeOwnedJsonOutput(value, stage)).value;
            start = value instanceof Uint8Array ? 1 : 0;
            end = prepared.length - start;
          } else if (
            typeof value === "string" ||
            typeof value === "number" ||
            typeof value === "boolean" ||
            typeof value === "bigint"
          ) {
            prepared = scalarStringWithAdmission(value, stage);
            end = utf8Bytes(prepared);
          }
        }
        let quoted = false,
          quotes = 0;
        if (typeof prepared === "string") {
          quoted = /[",\n\r]/u.test(prepared);
          for (let at = prepared.indexOf('"'); at !== -1; at = prepared.indexOf('"', at + 1))
            quotes++;
        } else {
          for (let i = start; i < end; i++) {
            const byte = prepared[i];
            if (byte === 34) quotes++;
            if (byte === 34 || byte === 44 || byte === 10 || byte === 13) quoted = true;
          }
        }
        const size = end - start + (quoted ? quotes + 2 : 0);
        cells[line * columns.length + index] = { value: prepared, start, end, quoted };
        total += size;
      }
    const output = stage.allocate(
      c.view + total,
      "CSV encoded output",
      () => new Uint8Array(total),
    );
    const buffer = output.value;
    let at = 0;
    for (let line = 0; line < lines; line++) {
      for (let index = 0; index < columns.length; index++) {
        if (index) buffer[at++] = 44;
        const cell = cells[line * columns.length + index];
        if (!cell) throw Error("Missing CSV cell");
        if (typeof cell.value === "string") {
          if (cell.quoted) buffer[at++] = 34;
          at = writeCsvText(cell.value, buffer, at, cell.quoted);
          if (cell.quoted) buffer[at++] = 34;
          continue;
        }
        if (!cell.quoted && cell.start === 0 && cell.end === cell.value.length) {
          buffer.set(cell.value, at);
          at += cell.end;
          continue;
        }
        if (cell.quoted) buffer[at++] = 34;
        for (let i = cell.start; i < cell.end; i++) {
          const byte = cell.value[i] ?? 0;
          buffer[at++] = byte;
          if (cell.quoted && byte === 34) buffer[at++] = 34;
        }
        if (cell.quoted) buffer[at++] = 34;
      }
      buffer[at++] = 10;
    }
    return output.retain();
  } finally {
    stage.dispose();
  }
}

/** TextEncoder-compatible UTF8 with optional CSV quote doubling, no scratch view. */
function writeCsvText(
  text: string,
  output: Uint8Array,
  offset: number,
  doubleQuotes: boolean,
): number {
  if (isPlainJsonAscii(text)) {
    const start = offset;
    for (let i = 0; i < text.length; i++) output[start + i] = text.charCodeAt(i);
    return offset + text.length;
  }
  for (let i = 0; i < text.length; i++) {
    let code = text.charCodeAt(i);
    if (doubleQuotes && code === 34) output[offset++] = 34;
    if (code >= 0xd800 && code <= 0xdfff) {
      const next = text.charCodeAt(i + 1);
      if (code <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
        code = 0x10000 + ((code - 0xd800) << 10) + next - 0xdc00;
        i++;
      } else code = 0xfffd;
    }
    if (code < 0x80) output[offset++] = code;
    else if (code < 0x800) {
      output[offset++] = 0xc0 | (code >> 6);
      output[offset++] = 0x80 | (code & 63);
    } else if (code < 0x10000) {
      output[offset++] = 0xe0 | (code >> 12);
      output[offset++] = 0x80 | ((code >> 6) & 63);
      output[offset++] = 0x80 | (code & 63);
    } else {
      output[offset++] = 0xf0 | (code >> 18);
      output[offset++] = 0x80 | ((code >> 12) & 63);
      output[offset++] = 0x80 | ((code >> 6) & 63);
      output[offset++] = 0x80 | (code & 63);
    }
  }
  return offset;
}
