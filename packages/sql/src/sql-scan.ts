export function findTopLevelKeyword(sql: string, keyword: string, start: number): number {
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

export function keywordAt(sql: string, index: number, keyword: string): boolean {
  if (sql.slice(index, index + keyword.length).toLowerCase() !== keyword) return false;
  return isBoundary(sql[index - 1]) && isBoundary(sql[index + keyword.length]);
}

export function isBoundary(char: string | undefined): boolean {
  return char === undefined || !/[A-Za-z0-9_]/u.test(char);
}

export function skipLineComment(sql: string, index: number): number {
  const newline = sql.indexOf("\n", index);
  return newline === -1 ? sql.length : newline;
}

export function skipBlockComment(sql: string, index: number): number {
  const end = sql.indexOf("*/", index);
  return end === -1 ? sql.length : end + 1;
}
