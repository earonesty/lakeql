import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";

/** Validate BSON wire framing without decoding strings or constructing values.
 * Explicit frames avoid the JS call-stack limit. Traversal storage is admitted
 * and released independently of future decoded output ownership. UTF-8 and value
 * representation checks belong to logical conversion, not this framing pass.
 */
export function validateBsonFraming(bytes: Uint8Array, parent: MemoryScope): void {
  const scope = parent.childScope();
  try {
    const view = scope.allocate(
      c.view,
      "BSON framing view",
      () => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
    ).value;
    const stack = scope.allocate(
      c.array,
      "BSON traversal stack",
      () =>
        [] as { end: number; resume: number; array: boolean; index: number; release: () => void }[],
    ).value;
    let offset = 0;
    const need = (length: number, end: number) => {
      if (!Number.isSafeInteger(length) || length < 0 || length > end - offset) invalid("extent");
    };
    const integer = (end: number) => {
      need(4, end);
      const result = view.getInt32(offset, true);
      offset += 4;
      return result;
    };
    const cstring = (end: number) => {
      while (offset < end && bytes[offset] !== 0) offset++;
      if (offset >= end) invalid("unterminated cstring");
      offset++;
    };
    const string = (end: number) => {
      const length = integer(end);
      if (length < 1) invalid("string length");
      need(length, end);
      if (bytes[offset + length - 1] !== 0) invalid("string terminator");
      offset += length;
    };
    const document = (limit: number, array: boolean, exactEnd?: number) => {
      const start = offset;
      const length = integer(limit);
      if (length < 5 || length > limit - start) invalid("document length");
      const end = start + length;
      if (exactEnd !== undefined && end !== exactEnd) invalid("document enclosing extent");
      const reservation = scope.reserve(
        c.object + 6 * c.property + c.arraySlot,
        "BSON traversal frame",
      );
      try {
        stack.push({ end, resume: end, array, index: 0, release: () => reservation.release() });
      } catch (error) {
        reservation.release();
        throw error;
      }
    };
    try {
      document(bytes.length, false, bytes.length);
      while (stack.length > 0) {
        const frame = stack[stack.length - 1];
        if (!frame) invalid("traversal state");
        if (offset === frame.end - 1) {
          if (bytes[offset] !== 0) invalid("document terminator");
          offset = frame.resume;
          stack.pop();
          frame.release();
          continue;
        }
        const end = frame.end - 1;
        need(1, end);
        const type = bytes[offset++];
        const key = offset;
        cstring(end);
        if (frame.array) {
          const keyEnd = offset - 1;
          if (key === keyEnd || (bytes[key] === 48 && keyEnd - key !== 1)) invalid("array index");
          let value = 0;
          for (let i = key; i < keyEnd; i++) {
            const digit = (bytes[i] ?? 0) - 48;
            if (digit < 0 || digit > 9) invalid("array index");
            value = value * 10 + digit;
          }
          if (!Number.isSafeInteger(value) || value !== frame.index++) invalid("array sequence");
        }
        switch (type) {
          case 1:
          case 9:
          case 17:
          case 18:
            need(8, end);
            offset += 8;
            break;
          case 16:
            need(4, end);
            offset += 4;
            break;
          case 19:
            need(16, end);
            offset += 16;
            break;
          case 7:
            need(12, end);
            offset += 12;
            break;
          case 8:
            need(1, end);
            if (bytes[offset] !== 0 && bytes[offset] !== 1) invalid("boolean");
            offset++;
            break;
          case 2:
          case 13:
          case 14:
            string(end);
            break;
          case 3:
          case 4:
            document(end, type === 4);
            break;
          case 5: {
            const length = integer(end);
            need(1, end);
            const subtype = bytes[offset++];
            need(length, end);
            if (subtype === 2) {
              if (length < 4) invalid("old binary length");
              const nested = integer(offset + length);
              if (nested !== length - 4) invalid("old binary extent");
              offset += nested;
            } else offset += length;
            break;
          }
          case 11:
            cstring(end);
            cstring(end);
            break;
          case 12:
            string(end);
            need(12, end);
            offset += 12;
            break;
          case 15: {
            const start = offset;
            const length = integer(end);
            if (length < 14 || length > end - start) invalid("code scope length");
            const scopeEnd = start + length;
            string(scopeEnd);
            document(scopeEnd, false, scopeEnd);
            break;
          }
          case 6:
          case 10:
          case 127:
          case 255:
            break;
          default:
            invalid("element type");
        }
      }
    } finally {
      for (const frame of stack) frame.release();
    }
  } finally {
    scope.dispose();
  }
}

function invalid(detail: string): never {
  throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Invalid BSON ${detail}`);
}
