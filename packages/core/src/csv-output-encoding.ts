import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { intervalToString, isIntervalValue } from "./interval.js";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { isTimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";
import { utf8Bytes } from "./utf8-size.js";

const encoder = new TextEncoder();
const empty = new Uint8Array(0);
interface Cell {
  bytes: Uint8Array;
  start: number;
  end: number;
  quoted: boolean;
}

/** Admit cell backing and the overlapping final output before allocation.
 * Avoids a JSON-safe clone and complete CSV row string. Scalar formatting and
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
        let bytes: Uint8Array = empty;
        let start = 0;
        let end = 0;
        if (isTimestampValue(value)) value = value.toJSON();
        else if (isIntervalValue(value)) value = intervalToString(value);
        if (
          value !== null &&
          value !== undefined &&
          typeof value !== "function" &&
          typeof value !== "symbol"
        ) {
          if (typeof value === "object") {
            bytes = stage.adopt(encodeOwnedJsonOutput(value, stage)).value;
            start = value instanceof Uint8Array ? 1 : 0;
            end = bytes.length - start;
          } else {
            const text = String(value);
            const length = utf8Bytes(text);
            bytes = stage.allocate(
              c.view + length,
              "CSV cell bytes",
              () => new Uint8Array(length),
            ).value;
            encoder.encodeInto(text, bytes);
            end = bytes.length;
          }
        }
        let quoted = false,
          quotes = 0;
        for (let i = start; i < end; i++) {
          const byte = bytes[i];
          if (byte === 34) quotes++;
          if (byte === 34 || byte === 44 || byte === 10 || byte === 13) quoted = true;
        }
        const size = end - start + (quoted ? quotes + 2 : 0);
        cells[line * columns.length + index] = { bytes, start, end, quoted };
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
        if (!cell.quoted && cell.start === 0 && cell.end === cell.bytes.length) {
          buffer.set(cell.bytes, at);
          at += cell.end;
          continue;
        }
        if (cell.quoted) buffer[at++] = 34;
        for (let i = cell.start; i < cell.end; i++) {
          const byte = cell.bytes[i] ?? 0;
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
