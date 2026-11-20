/**
 * Query-side text analysis for Lance inverted indexes. The analyzer is rebuilt from the
 * parameters persisted with the index and must reproduce the index-time pipeline exactly:
 *
 *   base tokenizer -> remove-long -> lowercase -> stem -> stop words -> ASCII folding
 *
 * Analyzer components this reader does not implement (other languages, n-gram, ICU, JSON,
 * dictionary segmenters) are rejected when the index is opened. Approximating them would
 * silently return different results than Lance.
 */
import { LakeqlError } from "lakeql";
import { stemEnglish } from "./porter2.js";

export interface LanceTokenizerParams {
  /** As persisted; `assertSupportedAnalyzer` rejects values this reader cannot reproduce. */
  lanceTokenizer: string;
  baseTokenizer: string;
  language: string;
  withPosition: boolean;
  maxTokenLength: number | undefined;
  lowerCase: boolean;
  stem: boolean;
  removeStopWords: boolean;
  customStopWords: readonly string[] | undefined;
  asciiFolding: boolean;
}

export interface QueryToken {
  text: string;
  /** Position in the analyzed query, counting tokens removed by filters. */
  position: number;
}

// Lance (tantivy) English stop words.
const ENGLISH_STOP_WORDS = [
  "a",
  "an",
  "and",
  "are",
  "as",
  "at",
  "be",
  "but",
  "by",
  "for",
  "if",
  "in",
  "into",
  "is",
  "it",
  "no",
  "not",
  "of",
  "on",
  "or",
  "such",
  "that",
  "the",
  "their",
  "then",
  "there",
  "these",
  "they",
  "this",
  "to",
  "was",
  "will",
  "with",
];

/** Parse the `params` JSON stored in an inverted index's `metadata.lance`. */
export function parseTokenizerParams(json: string): LanceTokenizerParams {
  let raw: Record<string, unknown>;
  try {
    raw = JSON.parse(json) as Record<string, unknown>;
  } catch (cause) {
    throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", "Lance inverted index params are not JSON", {
      cause: String(cause),
    });
  }
  const bool = (key: string, fallback: boolean): boolean =>
    typeof raw[key] === "boolean" ? (raw[key] as boolean) : fallback;
  const lanceTokenizer = raw.lance_tokenizer ?? "text";
  const baseTokenizer = raw.base_tokenizer ?? "simple";
  const language = raw.language ?? "English";
  const stem = bool("stem", true);
  const removeStopWords = bool("remove_stop_words", true);
  const customStopWords = Array.isArray(raw.custom_stop_words)
    ? (raw.custom_stop_words as unknown[]).filter((w): w is string => typeof w === "string")
    : undefined;
  const maxTokenLength =
    typeof raw.max_token_length === "number" ? (raw.max_token_length as number) : undefined;
  return {
    lanceTokenizer: String(lanceTokenizer),
    baseTokenizer: String(baseTokenizer),
    language: String(language),
    withPosition: bool("with_position", false),
    maxTokenLength,
    lowerCase: bool("lower_case", true),
    stem,
    removeStopWords,
    customStopWords,
    asciiFolding: bool("ascii_folding", true),
  };
}

/** Throw unless the persisted analyzer can be reproduced exactly by this reader. */
export function assertSupportedAnalyzer(params: LanceTokenizerParams): void {
  if (params.lanceTokenizer !== "text") {
    unsupported("Lance JSON inverted indexes are not supported", {
      lanceTokenizer: params.lanceTokenizer,
    });
  }
  if (!["simple", "whitespace", "raw"].includes(params.baseTokenizer)) {
    unsupported("Lance inverted-index base tokenizer is not supported", {
      baseTokenizer: params.baseTokenizer,
    });
  }
  if (
    params.language !== "English" &&
    (params.stem || (params.removeStopWords && params.customStopWords === undefined))
  ) {
    unsupported("Lance inverted-index language is not supported", { language: params.language });
  }
}

const ALNUM = /[\p{Alphabetic}\p{N}]/u;
const encoder = new TextEncoder();

function baseTokens(text: string, kind: string): string[] {
  if (kind === "raw") return [text];
  if (kind === "whitespace") {
    // Rust `char::is_ascii_whitespace`: space, \t, \n, \x0c, \r.
    return text.split(/[ \t\n\f\r]+/u).filter((token) => token.length > 0);
  }
  const tokens: string[] = [];
  let current = "";
  for (const char of text) {
    if (ALNUM.test(char)) current += char;
    else if (current.length > 0) {
      tokens.push(current);
      current = "";
    }
  }
  if (current.length > 0) tokens.push(current);
  return tokens;
}

export function analyze(text: string, params: LanceTokenizerParams): QueryToken[] {
  const stopWords = new Set(
    params.customStopWords ?? (params.removeStopWords ? ENGLISH_STOP_WORDS : []),
  );
  const out: QueryToken[] = [];
  for (const [position, base] of baseTokens(text, params.baseTokenizer).entries()) {
    if (
      params.maxTokenLength !== undefined &&
      encoder.encode(base).length >= params.maxTokenLength
    ) {
      continue;
    }
    let token = base;
    if (params.lowerCase) token = lowerCase(token);
    if (params.stem) token = stemEnglish(token);
    if (params.removeStopWords && stopWords.has(token)) continue;
    if (params.asciiFolding) token = foldToAscii(token);
    out.push({ text: token, position });
  }
  return out;
}

function lowerCase(text: string): string {
  let out = "";
  for (const char of text) out += char.toLowerCase();
  return out;
}

const FOLD: Record<string, string> = {
  ß: "ss",
  ẞ: "SS",
  Æ: "AE",
  æ: "ae",
  Œ: "OE",
  œ: "oe",
  Ø: "O",
  ø: "o",
  Ł: "L",
  ł: "l",
  Đ: "D",
  Ð: "D",
  đ: "d",
  ð: "d",
  Þ: "TH",
  þ: "th",
  Ħ: "H",
  ħ: "h",
  Ŧ: "T",
  ŧ: "t",
  Ŋ: "N",
  ŋ: "n",
  ı: "i",
  ĸ: "k",
  ſ: "s",
};

const COMBINING_MARK = /^\p{M}$/u;

export function foldToAscii(text: string): string {
  let out = "";
  for (const char of text) {
    const code = char.codePointAt(0) as number;
    if (code < 128) {
      out += char;
      continue;
    }
    const mapped = FOLD[char];
    if (mapped !== undefined) {
      out += mapped;
      continue;
    }
    let decomposedOut = "";
    for (const part of char.normalize("NFKD")) {
      if ((part.codePointAt(0) as number) < 128) decomposedOut += part;
      else if (COMBINING_MARK.test(part)) continue;
      else decomposedOut += FOLD[part] ?? "";
    }
    out += decomposedOut.length === 0 ? char : decomposedOut;
  }
  return out;
}

function unsupported(message: string, details: Record<string, unknown>): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}
