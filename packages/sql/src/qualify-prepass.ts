export interface SqlQualifyPrepassResult {
  sql: string;
  qualify?: string;
}

const CLAUSE_KEYWORDS = ["order by", "limit", "offset"] as const;

export function extractTopLevelQualify(sql: string): SqlQualifyPrepassResult {
  const qualify = findTopLevelKeyword(sql, "qualify", 0);
  if (qualify === -1) return { sql };
  const bodyStart = qualify + "qualify".length;
  const bodyEnd = nextClauseStart(sql, bodyStart);
  const predicate = sql.slice(bodyStart, bodyEnd).trim();
  if (predicate.length === 0) return { sql };
  const rewritten = `${sql.slice(0, qualify)} ${sql.slice(bodyEnd)}`.trim();
  return { sql: rewritten, qualify: predicate };
}

function nextClauseStart(sql: string, start: number): number {
  let best = sql.length;
  for (const keyword of CLAUSE_KEYWORDS) {
    const index = findTopLevelKeyword(sql, keyword, start);
    if (index !== -1 && index < best) best = index;
  }
  return best;
}

function findTopLevelKeyword(sql: string, keyword: string, start: number): number {
  let depth = 0;
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
    if (char === "(") depth += 1;
    else if (char === ")") depth = Math.max(0, depth - 1);
    if (depth === 0 && keywordAt(sql, index, keyword)) return index;
  }
  return -1;
}

function keywordAt(sql: string, index: number, keyword: string): boolean {
  if (sql.slice(index, index + keyword.length).toLowerCase() !== keyword) return false;
  return isBoundary(sql[index - 1]) && isBoundary(sql[index + keyword.length]);
}

function isBoundary(char: string | undefined): boolean {
  return char === undefined || !/[A-Za-z0-9_]/u.test(char);
}

function skipLineComment(sql: string, index: number): number {
  const newline = sql.indexOf("\n", index);
  return newline === -1 ? sql.length : newline;
}

function skipBlockComment(sql: string, index: number): number {
  const end = sql.indexOf("*/", index);
  return end === -1 ? sql.length : end + 1;
}
