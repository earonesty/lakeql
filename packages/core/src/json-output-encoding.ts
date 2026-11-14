import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { isIntervalValue } from "./interval.js";
import { writeJsonInteger } from "./json-integer-output.js";
import { jsonOutputUtf8Bytes } from "./json-output-size.js";
import { JsonScalarFormatter } from "./json-scalar-output.js";
import { isPlainJsonAscii } from "./json-string-size.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { isTimestampValue } from "./timestamp.js";

/** Encode immutable JSON-safe values directly into one admitted output buffer.
 * No cloned value tree, complete JSON string, or per-string escaped copy.
 * Scalar formatting reuses one admitted workspace per traversal. Traversal and runtime
 * workspace remain separate from output backing.
 */
export function encodeOwnedJsonOutput(
  value: unknown,
  scope: MemoryScope,
  framing: { prefix?: "" | "," | "["; suffix?: "" | "\n" | "]" } = {},
): OwnedMemory<Uint8Array> {
  const size = jsonOutputUtf8Bytes(value, scope);
  if (size === undefined)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing JSON root output");
  const prefix = framing.prefix ?? "";
  const suffix = framing.suffix ?? "";
  const bytes = size + prefix.length + suffix.length;
  const construction = scope.childScope();
  const scalar = new JsonScalarFormatter(construction);
  try {
    const output = construction.allocate(
      bytes + c.view,
      "JSON encoded output",
      () => new Uint8Array(bytes),
    );
    const buffer = output.value;
    let offset = 0;
    const byte = (value: number) => {
      buffer[offset++] = value;
    };
    const ascii = (text: string) => {
      for (let i = 0; i < text.length; i++) byte(text.charCodeAt(i));
    };
    const hex = (value: number) => (value < 10 ? 48 + value : 87 + value);
    const unicodeEscape = (code: number) => {
      byte(92);
      byte(117);
      for (let shift = 12; shift >= 0; shift -= 4) byte(hex((code >> shift) & 15));
    };
    const string = (text: string) => {
      byte(34);
      if (isPlainJsonAscii(text)) {
        const start = offset;
        for (let i = 0; i < text.length; i++) buffer[start + i] = text.charCodeAt(i);
        offset += text.length;
        byte(34);
        return;
      }
      for (let i = 0; i < text.length; i++) {
        let code = text.charCodeAt(i);
        if (code === 34 || code === 92) {
          byte(92);
          byte(code);
        } else if (code === 8 || code === 9 || code === 10 || code === 12 || code === 13) {
          byte(92);
          byte(code === 8 ? 98 : code === 9 ? 116 : code === 10 ? 110 : code === 12 ? 102 : 114);
        } else if (code < 32) unicodeEscape(code);
        else if (code >= 0xd800 && code <= 0xdfff) {
          const next = text.charCodeAt(i + 1);
          if (code <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
            code = 0x10000 + ((code - 0xd800) << 10) + next - 0xdc00;
            byte(0xf0 | (code >> 18));
            byte(0x80 | ((code >> 12) & 63));
            byte(0x80 | ((code >> 6) & 63));
            byte(0x80 | (code & 63));
            i++;
          } else unicodeEscape(code);
        } else if (code < 128) byte(code);
        else if (code < 2048) {
          byte(0xc0 | (code >> 6));
          byte(0x80 | (code & 63));
        } else {
          byte(0xe0 | (code >> 12));
          byte(0x80 | ((code >> 6) & 63));
          byte(0x80 | (code & 63));
        }
      }
      byte(34);
    };
    const absent = (value: unknown) =>
      value === undefined || typeof value === "function" || typeof value === "symbol";
    const write = (value: unknown): void => {
      if (value === null || absent(value)) {
        ascii("null");
        return;
      }
      if (isTimestampValue(value)) {
        scalar.consume(value, string);
        return;
      }
      if (isIntervalValue(value)) {
        scalar.consume(value, string);
        return;
      }
      if (value instanceof Uint8Array) {
        byte(34);
        for (const item of value) {
          byte(hex(item >> 4));
          byte(hex(item & 15));
        }
        byte(34);
        return;
      }
      switch (typeof value) {
        case "string":
          string(value);
          return;
        case "boolean":
          ascii(value ? "true" : "false");
          return;
        case "number":
          if (Number.isSafeInteger(value)) offset = writeJsonInteger(value, buffer, offset);
          else if (Number.isFinite(value)) scalar.consume(value, ascii);
          else ascii("null");
          return;
        case "bigint": {
          const number = Number(value);
          if (Number.isSafeInteger(number)) offset = writeJsonInteger(number, buffer, offset);
          else scalar.consume(value, string);
          return;
        }
        case "object": {
          if (Array.isArray(value)) {
            byte(91);
            for (let i = 0; i < value.length; i++) {
              if (i) byte(44);
              write(value[i]);
            }
            byte(93);
          } else {
            byte(123);
            let first = true;
            for (const key in value) {
              if (!Object.hasOwn(value, key)) continue;
              const item = (value as Record<string, unknown>)[key];
              if (absent(item)) continue;
              if (!first) byte(44);
              first = false;
              string(key);
              byte(58);
              write(item);
            }
            byte(125);
          }
        }
      }
    };
    ascii(prefix);
    write(value);
    ascii(suffix);
    if (offset !== bytes)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "JSON value changed during encoding");
    return output.retain();
  } finally {
    scalar.dispose();
    construction.dispose();
  }
}
