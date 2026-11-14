import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  timestampFromEpoch,
} from "lakeql-core";
import { validateBsonFraming } from "./bson-framing.js";

const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
type Container = Map<string, unknown> | unknown[];

/** BSON documents use dynamic maps, as JSON logical columns do. Standard scalar
 * values remain native; BSON-only values use explicit $bson-tagged maps. Binary
 * subtypes and Decimal128 payloads remain exact bytes, never rounded numbers.
 * Code/regex are data and are never executed. Duplicate document keys reject.
 */
export function decodeBsonValue(bytes: Uint8Array, parent: MemoryScope): Map<string, unknown> {
  validateBsonFraming(bytes, parent);
  const output = parent.childScope();
  const scratch = output.childScope();
  try {
    const view = scratch.allocate(
      c.view,
      "BSON decode view",
      () => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
    ).value;
    const frames = scratch.allocate(
      c.array,
      "BSON decode stack",
      () =>
        [] as {
          end: number;
          value: Container;
          release: () => void;
        }[],
    ).value;
    let offset = 0;
    const number = (read: () => number) => output.allocate(c.number, "BSON scalar", read).value;
    const put = (map: Map<string, unknown>, key: string, value: unknown) => {
      if (map.has(key)) invalid("duplicate document key");
      output.allocate(c.property, "BSON map entry", () => map.set(key, value));
    };
    const tag = (name: string) => {
      const map = output.allocate(
        c.object,
        "BSON tagged value",
        () => new Map<string, unknown>(),
      ).value;
      put(map, "$bson", name);
      return map;
    };
    const text = (start: number, end: number) =>
      output.allocate(
        c.string + c.stringCodeUnit * (end - start) + c.view,
        "BSON decoded string",
        () => {
          try {
            return utf8.decode(bytes.subarray(start, end));
          } catch {
            return invalid("UTF-8");
          }
        },
      ).value;
    const cstring = () => {
      const start = offset;
      while (bytes[offset] !== 0) offset++;
      return text(start, offset++);
    };
    const string = () => {
      const length = view.getInt32(offset, true);
      offset += 4;
      const value = text(offset, offset + length - 1);
      offset += length;
      return value;
    };
    const binary = (length: number) => {
      const result = output.allocate(
        2 * c.view + length,
        "BSON binary value",
        () => new Uint8Array(new Uint8Array(bytes.buffer, bytes.byteOffset + offset, length)),
      ).value;
      offset += length;
      return result;
    };
    const document = (array: boolean): Container => {
      const end = offset + view.getInt32(offset, true);
      offset += 4;
      const value: Container = array
        ? output.allocate(c.array, "BSON array", () => []).value
        : output.allocate(c.object, "BSON document", () => new Map<string, unknown>()).value;
      const reservation = scratch.reserve(
        c.object + 4 * c.property + c.arraySlot,
        "BSON decode frame",
      );
      try {
        frames.push({ end, value, release: () => reservation.release() });
      } catch (error) {
        reservation.release();
        throw error;
      }
      return value;
    };
    try {
      const root = document(false) as Map<string, unknown>;
      while (frames.length > 0) {
        const frame = frames[frames.length - 1];
        if (!frame) invalid("traversal state");
        if (offset === frame.end - 1) {
          offset++;
          frames.pop();
          frame.release();
          continue;
        }
        const type = bytes[offset++];
        let key = "";
        if (frame.value instanceof Map) key = cstring();
        else {
          // Framing already checked sequential array indices; don't allocate
          // strings for keys that are not part of the decoded array.
          while (bytes[offset] !== 0) offset++;
          offset++;
        }
        if (frame.value instanceof Map && frame.value.has(key)) invalid("duplicate document key");
        let value: unknown;
        switch (type) {
          case 1:
            value = number(() => view.getFloat64(offset, true));
            offset += 8;
            break;
          case 2:
            value = string();
            break;
          case 3:
          case 4:
            value = document(type === 4);
            break;
          case 5: {
            let length = view.getInt32(offset, true);
            offset += 4;
            const subtype = bytes[offset++];
            if (subtype === 2) {
              length = view.getInt32(offset, true);
              offset += 4;
            }
            const tagged = tag("binary");
            put(
              tagged,
              "subtype",
              number(() => subtype ?? 0),
            );
            put(tagged, "bytes", binary(length));
            value = tagged;
            break;
          }
          case 6:
            value = tag("undefined");
            break;
          case 7: {
            const tagged = tag("objectId");
            put(tagged, "bytes", binary(12));
            value = tagged;
            break;
          }
          case 8:
            value = bytes[offset++] === 1;
            break;
          case 9:
            value = output.allocate(c.timestamp + c.bigint, "BSON UTC datetime", () =>
              timestampFromEpoch(view.getBigInt64(offset, true), "millis"),
            ).value;
            offset += 8;
            break;
          case 10:
            value = null;
            break;
          case 11: {
            const tagged = tag("regex");
            put(tagged, "pattern", cstring());
            const options = cstring();
            let previous = "";
            for (const option of options) {
              if (!"imsux".includes(option) || option <= previous) invalid("regex options");
              previous = option;
            }
            put(tagged, "options", options);
            value = tagged;
            break;
          }
          case 12: {
            const tagged = tag("dbPointer");
            put(tagged, "namespace", string());
            put(tagged, "id", binary(12));
            value = tagged;
            break;
          }
          case 13:
          case 14: {
            const tagged = tag(type === 13 ? "code" : "symbol");
            put(tagged, "value", string());
            value = tagged;
            break;
          }
          case 15: {
            offset += 4;
            const tagged = tag("codeWithScope");
            put(tagged, "code", string());
            put(tagged, "scope", document(false));
            value = tagged;
            break;
          }
          case 16:
            value = number(() => view.getInt32(offset, true));
            offset += 4;
            break;
          case 17: {
            const tagged = tag("timestamp");
            put(
              tagged,
              "increment",
              number(() => view.getUint32(offset, true)),
            );
            put(
              tagged,
              "seconds",
              number(() => view.getUint32(offset + 4, true)),
            );
            offset += 8;
            value = tagged;
            break;
          }
          case 18:
            value = output.allocate(c.bigint, "BSON int64", () =>
              view.getBigInt64(offset, true),
            ).value;
            offset += 8;
            break;
          case 19: {
            const tagged = tag("decimal128");
            put(tagged, "bytes", binary(16));
            value = tagged;
            break;
          }
          case 127:
            value = tag("maxKey");
            break;
          case 255:
            value = tag("minKey");
            break;
          default:
            invalid("element type");
        }
        if (frame.value instanceof Map) put(frame.value, key, value);
        else
          output.allocate(
            c.arraySlot,
            "BSON array element",
            () => Array.isArray(frame.value) && frame.value.push(value),
          );
      }
      scratch.dispose();
      const owner = output.retainAllocations(root);
      try {
        parent.adopt(owner);
      } catch (error) {
        owner.release();
        throw error;
      }
      return root;
    } finally {
      for (const frame of frames) frame.release();
    }
  } finally {
    scratch.dispose();
    output.dispose();
  }
}

function invalid(detail: string): never {
  throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Invalid BSON ${detail}`);
}
