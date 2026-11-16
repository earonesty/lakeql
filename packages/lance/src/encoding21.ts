// Storage 2.1 structural/compressive descriptions from lance-format/lance v8.0.0.
import { LakeqlError } from "lakeql";
import { ProtoReader } from "./proto.js";

export type Compression21 =
  | { kind: "flat" | "bitpack"; bits: number }
  | { kind: "out-of-line"; bits: number; values: Compression21 }
  | { kind: "variable"; offsets: Compression21 }
  | { kind: "fsst"; symbols: Uint8Array; values: Compression21 }
  | { kind: "rle"; values: Compression21; lengths: Compression21 }
  | { kind: "constant"; value: Uint8Array };

export type Layout21 = {
  kind: "structural";
  layout: "mini" | "null" | "zip";
  layers: number[];
  values?: Compression21;
  definitions?: Compression21;
  dictionary?: Compression21;
  dictionaryItems: number;
  buffers: number;
  items: number;
  bitsDef: number;
  bitsValue: number;
  bitsOffset: number;
};

export function parseLayout21(bytes: Uint8Array): Layout21 {
  const outer = new ProtoReader(bytes);
  if (outer.done) corrupt21("Empty structural layout");
  const key = outer.key();
  if (![1, 2, 3].includes(key.field)) unsupported21("Unsupported structural layout");
  const reader = new ProtoReader(outer.message(key.wire));
  if (!outer.done) corrupt21("Multiple structural layouts");
  const out: Layout21 = {
    kind: "structural",
    layout: key.field === 1 ? "mini" : key.field === 2 ? "null" : "zip",
    layers: [],
    buffers: 0,
    items: 0,
    dictionaryItems: 0,
    bitsDef: 0,
    bitsValue: 0,
    bitsOffset: 0,
  };
  while (!reader.done) {
    const { field, wire } = reader.key();
    if (out.layout === "mini") {
      if (field === 1) unsupported21("Repeated values require a repeated-value decoder");
      else if (field === 4) out.dictionary = parseCompression21(reader.message(wire));
      else if (field === 2) out.definitions = parseCompression21(reader.message(wire));
      else if (field === 3) out.values = parseCompression21(reader.message(wire));
      else if (field === 5) out.dictionaryItems = reader.safeInteger(wire, "dictionary count");
      else if (field === 6) out.layers.push(...reader.packedSafeIntegers(wire, "definition layer"));
      else if (field === 7) out.buffers = reader.safeInteger(wire, "mini-block buffer count");
      else if (field === 8 && reader.safeInteger(wire, "repetition depth") !== 0)
        unsupported21("Repeated values require a repeated-value decoder");
      else if (field === 9) out.items = reader.safeInteger(wire, "mini-block item count");
      else if (field === 10 && reader.varint(wire) !== 0n)
        unsupported21("Large mini-blocks require storage 2.2");
      else if (![5, 8, 10].includes(field)) reader.skip(wire);
    } else if (out.layout === "null") {
      if (field === 5) out.layers.push(...reader.packedSafeIntegers(wire, "definition layer"));
      else unsupported21("Unsupported constant structural layout");
    } else {
      if (field === 1 && reader.varint(wire) !== 0n)
        unsupported21("Repeated full-zip values are unsupported");
      else if (field === 2) out.bitsDef = reader.safeInteger(wire, "definition bits");
      else if (field === 3) out.bitsValue = reader.safeInteger(wire, "value bits");
      else if (field === 4) out.bitsOffset = reader.safeInteger(wire, "offset bits");
      else if (field === 5) out.items = reader.safeInteger(wire, "full-zip item count");
      else if (field === 6) reader.safeInteger(wire, "visible item count");
      else if (field === 7) out.values = parseCompression21(reader.message(wire));
      else if (field === 8) out.layers.push(...reader.packedSafeIntegers(wire, "definition layer"));
      else if (field !== 1) reader.skip(wire);
    }
  }
  if (out.layers.length === 0 || out.layers.some((layer) => layer !== 1 && layer !== 3))
    unsupported21("Structural projection supports primitive and struct layers without lists");
  if (out.layout === "mini" && (out.values === undefined || out.buffers < 0 || out.buffers > 8))
    corrupt21("Invalid mini-block description");
  return out;
}

function parseCompression21(bytes: Uint8Array, depth = 0): Compression21 {
  if (depth > 16) corrupt21("Compression nesting is too deep");
  const outer = new ProtoReader(bytes);
  if (outer.done) corrupt21("Empty compression description");
  const key = outer.key();
  const reader = new ProtoReader(outer.message(key.wire));
  if (!outer.done) corrupt21("Multiple compression descriptions");
  let bits = 0;
  let first: Compression21 | undefined;
  let second: Compression21 | undefined;
  let value: Uint8Array = new Uint8Array();
  while (!reader.done) {
    const { field, wire } = reader.key();
    if (key.field === 1 || key.field === 5) {
      if (field === 1) bits = reader.safeInteger(wire, "compression bit width");
      else unsupported21("Compressed flat buffers require a general decompressor");
    } else if (key.field === 4) {
      if (field === 1) bits = reader.safeInteger(wire, "uncompressed bit width");
      else if (field === 3) first = parseCompression21(reader.message(wire), depth + 1);
      else reader.skip(wire);
    } else if (key.field === 3 || key.field === 6) {
      if (field === 1) value = reader.bytesValue(wire);
      else if (key.field === 6 && field === 2)
        second = parseCompression21(reader.message(wire), depth + 1);
      else reader.skip(wire);
    } else if (key.field === 2 || key.field === 8) {
      if (field === 1) first = parseCompression21(reader.message(wire), depth + 1);
      else if (field === 2 && key.field === 8)
        second = parseCompression21(reader.message(wire), depth + 1);
      else unsupported21("Compressed variable buffers require a general decompressor");
    } else unsupported21("Unsupported storage 2.1 compression", { encodingField: key.field });
  }
  if (key.field === 1 || key.field === 5) {
    if (bits < 1 || bits > 64 || (key.field === 5 && ![8, 16, 32, 64].includes(bits)))
      corrupt21("Invalid numeric bit width");
    return { kind: key.field === 1 ? "flat" : "bitpack", bits };
  }
  if (key.field === 4 && first !== undefined && [8, 16, 32, 64].includes(bits))
    return { kind: "out-of-line", bits, values: first };
  if (key.field === 3) return { kind: "constant", value };
  if (key.field === 2 && first !== undefined) return { kind: "variable", offsets: first };
  if (key.field === 6 && second !== undefined)
    return { kind: "fsst", symbols: value, values: second };
  if (key.field === 8 && first !== undefined && second !== undefined)
    return { kind: "rle", values: first, lengths: second };
  return corrupt21("Incomplete compression description");
}

export function corrupt21(message: string): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message);
}
export function unsupported21(message: string, details = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}
