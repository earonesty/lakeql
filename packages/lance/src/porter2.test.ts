import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { stemEnglish } from "./porter2.js";

describe("Snowball English stemmer", () => {
  it("matches the reference stemmer for a large vocabulary", async () => {
    const pairs = JSON.parse(
      await readFile(resolve(import.meta.dirname, "../fixtures/porter2-english.json"), "utf8"),
    ) as [string, string][];
    const mismatches = pairs
      .filter(([word, stem]) => stemEnglish(word) !== stem)
      .map(([word, stem]) => `${word}: expected ${stem}, got ${stemEnglish(word)}`);
    expect(mismatches.slice(0, 30)).toEqual([]);
    expect(pairs.length).toBeGreaterThan(10000);
  });
});
