/**
 * Read-only decoder for `fst` crate (v3) maps, the token dictionary format used by Lance
 * inverted indexes. Nodes are addressed by absolute offsets within the serialized bytes.
 */
import { LakeqlError } from "lakeql";

// First 63 entries of fst's COMMON_INPUTS_INV table (index 1..63 in a node header).
const COMMON_INPUTS_INV = Uint8Array.of(
  116,
  101,
  47,
  111,
  97,
  115,
  114,
  105,
  112,
  99,
  110,
  119,
  46,
  104,
  108,
  109,
  45,
  100,
  117,
  48,
  49,
  50,
  103,
  61,
  58,
  98,
  102,
  51,
  121,
  53,
  38,
  95,
  52,
  118,
  57,
  54,
  55,
  56,
  107,
  37,
  63,
  120,
  67,
  68,
  65,
  83,
  70,
  73,
  66,
  69,
  106,
  80,
  84,
  122,
  82,
  78,
  77,
  43,
  76,
  79,
  113,
  72,
  71,
);

const EMPTY_ADDRESS = 0;
const TRANS_INDEX_THRESHOLD = 32;
const FST_VERSION = 3;

/** Byte-level automaton driven over FST keys. */
export interface FstAutomaton<S> {
  start(): S;
  /** False prunes the whole subtree below this state. */
  canMatch(state: S): boolean;
  isMatch(state: S): boolean;
  accept(state: S, byte: number): S;
}

interface NodeView {
  addr: number;
  isFinal: boolean;
  ntrans: number;
  finalOutput: number;
  input(i: number): number;
  output(i: number): number;
  transAddr(i: number): number;
  find(b: number): number;
}

function corrupt(message: string): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message);
}

function unpackUint(data: Uint8Array, offset: number, size: number): number {
  let value = 0;
  for (let i = 0; i < size; i += 1) {
    value += (data[offset + i] as number) * 2 ** (8 * i);
  }
  if (!Number.isSafeInteger(value)) corrupt("Lance FST value exceeds the safe integer range");
  return value;
}

export class FstMap {
  readonly length: number;
  private readonly rootAddr: number;
  private readonly end: number;

  constructor(private readonly data: Uint8Array) {
    if (data.length < 36) corrupt("Lance FST token set is truncated");
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const version = view.getBigUint64(0, true);
    if (version !== BigInt(FST_VERSION)) {
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
        "Unsupported Lance FST token set version",
        { version: version.toString() },
      );
    }
    this.end = data.length - 4;
    this.rootAddr = Number(view.getBigUint64(this.end - 8, true));
    this.length = Number(view.getBigUint64(this.end - 16, true));
    if (this.rootAddr !== EMPTY_ADDRESS && this.rootAddr + 21 !== data.length) {
      corrupt("Lance FST root address is inconsistent with its length");
    }
  }

  private node(addr: number): NodeView {
    const data = this.data;
    if (addr === EMPTY_ADDRESS) {
      return {
        addr,
        isFinal: true,
        ntrans: 0,
        finalOutput: 0,
        input: () => corrupt("FST node has no transitions"),
        output: () => 0,
        transAddr: () => corrupt("FST node has no transitions"),
        find: () => -1,
      };
    }
    if (addr < 16 || addr >= this.end) corrupt("Lance FST node address is out of bounds");
    const header = data[addr] as number;
    const kind = header >> 6;
    const commonIdx = header & 0x3f;
    if (kind === 3 || kind === 2) {
      // OneTransNext (3) and OneTrans (2) share the input encoding.
      const common = commonIdx === 0 ? undefined : COMMON_INPUTS_INV[commonIdx - 1];
      const inputLen = common === undefined ? 1 : 0;
      const input = common ?? (data[addr - 1] as number);
      if (kind === 3) {
        const end = addr - inputLen;
        return {
          addr,
          isFinal: false,
          ntrans: 1,
          finalOutput: 0,
          input: () => input,
          output: () => 0,
          transAddr: () => end - 1,
          find: (b) => (b === input ? 0 : -1),
        };
      }
      const sizes = data[addr - inputLen - 1] as number;
      const tsize = sizes >> 4;
      const osize = sizes & 0x0f;
      const end = addr - inputLen - 1 - tsize - osize;
      return {
        addr,
        isFinal: false,
        ntrans: 1,
        finalOutput: 0,
        input: () => input,
        output: () =>
          osize === 0 ? 0 : unpackUint(data, addr - inputLen - 1 - tsize - osize, osize),
        transAddr: () => {
          const delta = unpackUint(data, addr - inputLen - 1 - tsize, tsize);
          return delta === EMPTY_ADDRESS ? EMPTY_ADDRESS : end - delta;
        },
        find: (b) => (b === input ? 0 : -1),
      };
    }
    const isFinal = (header & 0x40) !== 0;
    const stateNtrans = header & 0x3f;
    const ntransLen = stateNtrans === 0 ? 1 : 0;
    const sizes = data[addr - ntransLen - 1] as number;
    const tsize = sizes >> 4;
    const osize = sizes & 0x0f;
    let ntrans = stateNtrans;
    if (stateNtrans === 0) {
      ntrans = data[addr - 1] as number;
      if (ntrans === 1) ntrans = 256;
    }
    const indexSize = ntrans > TRANS_INDEX_THRESHOLD ? 256 : 0;
    const totalTransSize = ntrans + ntrans * tsize + indexSize;
    const finalOsize = isFinal ? osize : 0;
    const end = addr - ntransLen - 1 - totalTransSize - ntrans * osize - finalOsize;
    const base = addr - ntransLen - 1;
    return {
      addr,
      isFinal,
      ntrans,
      finalOutput:
        osize === 0 || !isFinal
          ? 0
          : unpackUint(data, base - totalTransSize - ntrans * osize - osize, osize),
      input: (i) => data[base - indexSize - i - 1] as number,
      output: (i) =>
        osize === 0 ? 0 : unpackUint(data, base - totalTransSize - i * osize - osize, osize),
      transAddr: (i) => {
        const delta = unpackUint(data, base - indexSize - ntrans - i * tsize - tsize, tsize);
        return delta === EMPTY_ADDRESS ? EMPTY_ADDRESS : end - delta;
      },
      find: (b) => {
        if (ntrans > TRANS_INDEX_THRESHOLD) {
          const i = data[base - indexSize + b] as number;
          return i >= ntrans ? -1 : i;
        }
        const start = base - ntrans;
        for (let i = 0; i < ntrans; i += 1) {
          if (data[start + i] === b) return ntrans - i - 1;
        }
        return -1;
      },
    };
  }

  get(key: Uint8Array): number | undefined {
    let node = this.node(this.rootAddr);
    let output = 0;
    for (const byte of key) {
      const i = node.find(byte);
      if (i < 0) return undefined;
      output += node.output(i);
      node = this.node(node.transAddr(i));
    }
    return node.isFinal ? output + node.finalOutput : undefined;
  }

  /** Visit every key accepted by the automaton, in lexicographic order. */
  search<S>(
    automaton: FstAutomaton<S>,
    visit: (key: Uint8Array, value: number) => boolean | undefined,
  ): void {
    const key: number[] = [];
    const walk = (addr: number, state: S, output: number): boolean => {
      const node = this.node(addr);
      if (node.isFinal && automaton.isMatch(state)) {
        if (visit(Uint8Array.from(key), output + node.finalOutput) === false) return false;
      }
      for (let i = 0; i < node.ntrans; i += 1) {
        const byte = node.input(i);
        const next = automaton.accept(state, byte);
        if (!automaton.canMatch(next)) continue;
        key.push(byte);
        const keep = walk(node.transAddr(i), next, output + node.output(i));
        key.pop();
        if (!keep) return false;
      }
      return true;
    };
    walk(this.rootAddr, automaton.start(), 0);
  }

  entries(): [Uint8Array, number][] {
    const all: [Uint8Array, number][] = [];
    this.search(
      { start: () => 0, canMatch: () => true, isMatch: () => true, accept: () => 0 },
      (key, value) => {
        all.push([key, value]);
        return true;
      },
    );
    return all;
  }
}
