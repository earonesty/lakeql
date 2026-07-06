import {
  findTopLevelKeyword,
  isBoundary,
  keywordAt,
  skipBlockComment,
  skipLineComment,
} from "./sql-scan.js";

export interface SqlWindowPrepassFrame {
  text?: string;
  ignoreNulls?: boolean;
}

export interface SqlWindowPrepassResult {
  sql: string;
  frames: (SqlWindowPrepassFrame | undefined)[];
}

const FRAME_KEYWORDS = ["rows", "range", "groups"] as const;

export function extractWindowFrames(sql: string): SqlWindowPrepassResult {
  const named = inlineNamedWindows(sql);
  let out = "";
  const frames: (SqlWindowPrepassFrame | undefined)[] = [];
  let index = 0;
  while (index < named.length) {
    const over = findKeyword(named, "over", index);
    if (over === -1) {
      out += named.slice(index);
      break;
    }
    const beforeOver = stripWindowNullTreatment(named.slice(index, over));
    out += beforeOver.sql;
    const afterOver = skipWhitespace(named, over + "over".length);
    if (named[afterOver] !== "(") {
      out += named.slice(over, afterOver);
      index = afterOver;
      frames.push(
        beforeOver.ignoreNulls === undefined ? undefined : { ignoreNulls: beforeOver.ignoreNulls },
      );
      continue;
    }
    const close = matchingParen(named, afterOver);
    if (close === -1) {
      out += named.slice(over);
      break;
    }
    const content = named.slice(afterOver + 1, close);
    const stripped = stripFrame(content);
    frames.push(
      stripped.frame === undefined && beforeOver.ignoreNulls === undefined
        ? undefined
        : {
            ...(stripped.frame === undefined ? {} : { text: stripped.frame }),
            ...(beforeOver.ignoreNulls === undefined
              ? {}
              : { ignoreNulls: beforeOver.ignoreNulls }),
          },
    );
    out += `${named.slice(over, afterOver + 1)}${stripped.content})`;
    index = close + 1;
  }
  return { sql: out, frames };
}

function inlineNamedWindows(sql: string): string {
  const fromKeyword = findTopLevelKeyword(sql, "from", 0);
  const windowKeyword = findTopLevelKeyword(sql, "window", fromKeyword === -1 ? 0 : fromKeyword);
  if (windowKeyword === -1) return rewriteNamedWindowRefs(sql, new Map());
  const definitionsStart = skipWhitespace(sql, windowKeyword + "window".length);
  const definitionsEnd = findNamedWindowClauseEnd(sql, definitionsStart);
  const definitions = parseNamedWindowDefinitions(sql.slice(definitionsStart, definitionsEnd));
  const withoutClause = `${sql.slice(0, windowKeyword)} ${sql.slice(definitionsEnd)}`;
  return rewriteNamedWindowRefs(withoutClause, resolveNamedWindows(definitions));
}

function parseNamedWindowDefinitions(sql: string): Map<string, string> {
  const definitions = new Map<string, string>();
  let index = 0;
  while (index < sql.length) {
    index = skipWhitespace(sql, index);
    if (index >= sql.length) break;
    const name = readIdentifier(sql, index);
    if (name === undefined) break;
    index = skipWhitespace(sql, name.end);
    if (!keywordAt(sql, index, "as")) break;
    index = skipWhitespace(sql, index + "as".length);
    if (sql[index] !== "(") break;
    const close = matchingParen(sql, index);
    if (close === -1) break;
    definitions.set(name.name, sql.slice(index + 1, close).trim());
    index = skipWhitespace(sql, close + 1);
    if (sql[index] !== ",") break;
    index += 1;
  }
  return definitions;
}

function resolveNamedWindows(definitions: Map<string, string>): Map<string, string> {
  const resolved = new Map<string, string>();
  const resolve = (name: string, stack: Set<string>): string | undefined => {
    const cached = resolved.get(name);
    if (cached !== undefined) return cached;
    const definition = definitions.get(name);
    if (definition === undefined) return undefined;
    if (stack.has(name)) return definition;
    const base = readIdentifier(definition, skipWhitespace(definition, 0));
    if (base !== undefined) {
      const parent = resolve(base.name, new Set([...stack, name]));
      if (parent !== undefined) {
        const merged = `${parent} ${definition.slice(base.end).trim()}`.trim();
        resolved.set(name, merged);
        return merged;
      }
    }
    resolved.set(name, definition);
    return definition;
  };
  for (const name of definitions.keys()) resolve(name, new Set());
  return resolved;
}

function rewriteNamedWindowRefs(sql: string, definitions: Map<string, string>): string {
  if (definitions.size === 0) return sql;
  let out = "";
  let index = 0;
  while (index < sql.length) {
    const over = findKeyword(sql, "over", index);
    if (over === -1) {
      out += sql.slice(index);
      break;
    }
    out += sql.slice(index, over);
    const afterOver = skipWhitespace(sql, over + "over".length);
    if (sql[afterOver] === "(") {
      const close = matchingParen(sql, afterOver);
      if (close === -1) {
        out += sql.slice(over);
        break;
      }
      const content = sql.slice(afterOver + 1, close).trim();
      const name = readIdentifier(content, 0);
      if (name !== undefined && definitions.has(name.name)) {
        const rest = content.slice(name.end).trim();
        out += `over (${definitions.get(name.name)}${rest.length === 0 ? "" : ` ${rest}`})`;
      } else {
        out += sql.slice(over, close + 1);
      }
      index = close + 1;
      continue;
    }
    const name = readIdentifier(sql, afterOver);
    if (name !== undefined && definitions.has(name.name)) {
      out += `over (${definitions.get(name.name)})`;
      index = name.end;
      continue;
    }
    out += sql.slice(over, afterOver);
    index = afterOver;
  }
  return out;
}

function stripWindowNullTreatment(sql: string): { sql: string; ignoreNulls?: boolean } {
  const suffix = /(\s+)(ignore|respect)\s+nulls\s*$/iu.exec(sql);
  if (suffix !== null && suffix[2] !== undefined) {
    return {
      sql: sql.slice(0, suffix.index) + (suffix[1] ?? ""),
      ignoreNulls: suffix[2].toLowerCase() === "ignore",
    };
  }
  const close = previousNonWhitespace(sql, sql.length - 1);
  if (close === -1 || sql[close] !== ")") return { sql };
  const open = matchingOpenParen(sql, close);
  if (open === -1) return { sql };
  const content = sql.slice(open + 1, close);
  const stripped = stripTopLevelNullTreatment(content);
  if (stripped.ignoreNulls === undefined) return { sql };
  return {
    sql: `${sql.slice(0, open + 1)}${stripped.sql}${sql.slice(close)}`,
    ignoreNulls: stripped.ignoreNulls,
  };
}

function stripTopLevelNullTreatment(sql: string): { sql: string; ignoreNulls?: boolean } {
  let depth = 0;
  let quote: "'" | '"' | "`" | undefined;
  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];
    if (quote !== undefined) {
      if (char === quote) {
        if (next === quote) {
          index += 1;
          continue;
        }
        quote = undefined;
      }
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (char === "(") depth += 1;
    else if (char === ")") depth = Math.max(0, depth - 1);
    if (depth !== 0) continue;
    const match = /^(ignore|respect)\s+nulls\b/iu.exec(sql.slice(index));
    if (match === null || match[1] === undefined || !isBoundary(sql[index - 1])) continue;
    const start = trimLeftWhitespace(sql, index);
    const end = trimRightWhitespace(sql, index + match[0].length);
    return {
      sql: `${sql.slice(0, start)}${sql.slice(end)}`.replace(/\s+,/u, ","),
      ignoreNulls: match[1].toLowerCase() === "ignore",
    };
  }
  return { sql };
}

function stripFrame(content: string): { content: string; frame?: string } {
  const start = findFrameStart(content);
  if (start === -1) return { content };
  const frame = content.slice(start).trim();
  return { content: trimTrailingHorizontalWhitespace(content.slice(0, start)), frame };
}

function trimTrailingHorizontalWhitespace(value: string): string {
  return value.replace(/[ \t\f\v]+$/u, "");
}

function findFrameStart(content: string): number {
  let depth = 0;
  let quote: "'" | '"' | "`" | undefined;
  for (let index = 0; index < content.length; index += 1) {
    const char = content[index];
    const next = content[index + 1];
    if (quote !== undefined) {
      if (char === quote) {
        if (next === quote) {
          index += 1;
          continue;
        }
        quote = undefined;
      }
      continue;
    }
    if (char === "-" && next === "-") {
      index = skipLineComment(content, index + 2);
      continue;
    }
    if (char === "/" && next === "*") {
      index = skipBlockComment(content, index + 2);
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (char === "(") depth += 1;
    else if (char === ")") depth = Math.max(0, depth - 1);
    if (depth === 0 && FRAME_KEYWORDS.some((keyword) => keywordAt(content, index, keyword))) {
      return index;
    }
  }
  return -1;
}

function findKeyword(sql: string, keyword: string, start: number): number {
  let quote: "'" | '"' | "`" | undefined;
  for (let index = start; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];
    if (quote !== undefined) {
      if (char === quote) {
        if (next === quote) {
          index += 1;
          continue;
        }
        quote = undefined;
      }
      continue;
    }
    if (char === "-" && next === "-") {
      index = skipLineComment(sql, index + 2);
      continue;
    }
    if (char === "/" && next === "*") {
      index = skipBlockComment(sql, index + 2);
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (keywordAt(sql, index, keyword)) return index;
  }
  return -1;
}

function findNamedWindowClauseEnd(sql: string, start: number): number {
  const candidates = ["order", "limit", "offset", "fetch", "union", "qualify"];
  let end = sql.length;
  for (const keyword of candidates) {
    const index = findTopLevelKeyword(sql, keyword, start);
    if (index !== -1 && index < end) end = index;
  }
  return end;
}

function matchingParen(sql: string, open: number): number {
  let depth = 0;
  let quote: "'" | '"' | "`" | undefined;
  for (let index = open; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];
    if (quote !== undefined) {
      if (char === quote) {
        if (next === quote) {
          index += 1;
          continue;
        }
        quote = undefined;
      }
      continue;
    }
    if (char === "-" && next === "-") {
      index = skipLineComment(sql, index + 2);
      continue;
    }
    if (char === "/" && next === "*") {
      index = skipBlockComment(sql, index + 2);
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (char === "(") depth += 1;
    else if (char === ")") {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return -1;
}

function matchingOpenParen(sql: string, close: number): number {
  let depth = 0;
  let quote: "'" | '"' | "`" | undefined;
  for (let index = close; index >= 0; index -= 1) {
    const char = sql[index];
    const previous = sql[index - 1];
    if (quote !== undefined) {
      if (char === quote) quote = undefined;
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      if (previous === char) {
        index -= 1;
        continue;
      }
      quote = char;
      continue;
    }
    if (char === ")") depth += 1;
    else if (char === "(") {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return -1;
}

function skipWhitespace(sql: string, index: number): number {
  let cursor = index;
  while (/\s/u.test(sql[cursor] ?? "")) cursor += 1;
  return cursor;
}

function readIdentifier(sql: string, index: number): { name: string; end: number } | undefined {
  const char = sql[index];
  if (char === '"') {
    let cursor = index + 1;
    let name = "";
    while (cursor < sql.length) {
      const current = sql[cursor];
      if (current === '"' && sql[cursor + 1] === '"') {
        name += '"';
        cursor += 2;
        continue;
      }
      if (current === '"') return { name, end: cursor + 1 };
      name += current ?? "";
      cursor += 1;
    }
    return undefined;
  }
  const match = /^[A-Za-z_][A-Za-z0-9_]*/u.exec(sql.slice(index));
  if (match === null) return undefined;
  return { name: match[0], end: index + match[0].length };
}

function previousNonWhitespace(sql: string, index: number): number {
  let cursor = index;
  while (cursor >= 0 && /\s/u.test(sql[cursor] ?? "")) cursor -= 1;
  return cursor;
}

function trimLeftWhitespace(sql: string, index: number): number {
  let cursor = index;
  while (cursor > 0 && /\s/u.test(sql[cursor - 1] ?? "")) cursor -= 1;
  return cursor;
}

function trimRightWhitespace(sql: string, index: number): number {
  let cursor = index;
  while (cursor < sql.length && /\s/u.test(sql[cursor] ?? "")) cursor += 1;
  return cursor;
}
