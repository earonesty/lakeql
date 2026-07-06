import { findTopLevelKeyword } from "./sql-scan.js";

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
