/**
 * Snowball "english" (Porter2) stemmer. Lance applies `rust-stemmers`' English algorithm to
 * tokens at index time, so queries must stem identically.
 */

const VOWELS = new Set(["a", "e", "i", "o", "u", "y"]);
const DOUBLES = new Set(["bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt"]);
const LI_ENDING = new Set(["c", "d", "e", "g", "h", "k", "m", "n", "r", "t"]);

const EXCEPTIONS = new Map<string, string>(
  Object.entries({
    skis: "ski",
    skies: "sky",
    dying: "die",
    lying: "lie",
    tying: "tie",
    idly: "idl",
    gently: "gentl",
    ugly: "ugli",
    early: "earli",
    only: "onli",
    singly: "singl",
    sky: "sky",
    news: "news",
    howe: "howe",
    atlas: "atlas",
    cosmos: "cosmos",
    bias: "bias",
    andes: "andes",
  }),
);

const AFTER_1A_INVARIANT = new Set([
  "inning",
  "outing",
  "canning",
  "herring",
  "earring",
  "proceed",
  "exceed",
  "succeed",
]);

const isVowel = (chars: string[], i: number): boolean => VOWELS.has(chars[i] as string);

function regions(chars: string[]): { r1: number; r2: number } {
  const word = chars.join("");
  const findR = (from: number): number => {
    for (let i = from + 1; i < chars.length; i += 1) {
      if (!isVowel(chars, i) && isVowel(chars, i - 1)) return i + 1;
    }
    return chars.length;
  };
  let r1: number;
  if (word.startsWith("gener") || word.startsWith("arsen")) r1 = 5;
  else if (word.startsWith("commun")) r1 = 6;
  else r1 = findR(0);
  let r2 = chars.length;
  for (let i = r1 + 1; i < chars.length; i += 1) {
    if (!isVowel(chars, i) && isVowel(chars, i - 1)) {
      r2 = i + 1;
      break;
    }
  }
  if (r1 >= chars.length) r2 = chars.length;
  return { r1, r2 };
}

function endsShortSyllable(chars: string[], end: number): boolean {
  // `end` is the exclusive end of the prefix being examined.
  if (end === 2) {
    return isVowel(chars, 0) && !isVowel(chars, 1);
  }
  if (end < 3) return false;
  const a = end - 3;
  const b = end - 2;
  const c = end - 1;
  return (
    !isVowel(chars, a) &&
    isVowel(chars, b) &&
    !isVowel(chars, c) &&
    !["w", "x", "Y"].includes(chars[c] as string)
  );
}

function isShort(chars: string[], r1: number): boolean {
  return r1 >= chars.length && endsShortSyllable(chars, chars.length);
}

function hasVowel(chars: string[], start: number, end: number): boolean {
  for (let i = start; i < end; i += 1) if (isVowel(chars, i)) return true;
  return false;
}

export function stemEnglish(input: string): string {
  if (input.length <= 2) return input;
  const exception = EXCEPTIONS.get(input);
  if (exception !== undefined) return exception;
  let chars = [...input];
  if (chars[0] === "'") chars = chars.slice(1);
  // Snowball restores `y` only when it marked one, so a literal capital Y is left alone.
  let marked = false;
  if (chars[0] === "y") {
    chars[0] = "Y";
    marked = true;
  }
  for (let i = 1; i < chars.length; i += 1) {
    if (chars[i] === "y" && isVowel(chars, i - 1)) {
      chars[i] = "Y";
      marked = true;
    }
  }
  const { r1, r2 } = regions(chars);
  const word = () => chars.join("");
  const endsWith = (suffix: string): boolean => word().endsWith(suffix);
  const cut = (n: number): void => {
    chars = chars.slice(0, chars.length - n);
  };
  const replace = (n: number, text: string): void => {
    chars = [...chars.slice(0, chars.length - n), ...text];
  };
  const inR1 = (suffixLength: number): boolean => chars.length - suffixLength >= r1;
  const inR2 = (suffixLength: number): boolean => chars.length - suffixLength >= r2;

  // Step 0
  for (const suffix of ["'s'", "'s", "'"]) {
    if (endsWith(suffix)) {
      cut(suffix.length);
      break;
    }
  }

  // Step 1a
  if (endsWith("sses")) replace(4, "ss");
  else if (endsWith("ied") || endsWith("ies")) {
    if (chars.length - 3 > 1) replace(3, "i");
    else replace(3, "ie");
  } else if (endsWith("us") || endsWith("ss")) {
    // no change
  } else if (endsWith("s")) {
    if (hasVowel(chars, 0, chars.length - 2)) cut(1);
  }
  if (AFTER_1A_INVARIANT.has(word())) return word();

  // Step 1b
  const step1b = (): void => {
    if (endsWith("eedly")) {
      if (inR1(5)) replace(5, "ee");
      return;
    }
    if (endsWith("eed")) {
      if (inR1(3)) replace(3, "ee");
      return;
    }
    for (const suffix of ["ingly", "edly", "ing", "ed"]) {
      if (!endsWith(suffix)) continue;
      if (!hasVowel(chars, 0, chars.length - suffix.length)) return;
      cut(suffix.length);
      const w = word();
      if (w.endsWith("at") || w.endsWith("bl") || w.endsWith("iz")) chars.push("e");
      else if (DOUBLES.has(w.slice(-2))) cut(1);
      else if (isShort(chars, r1)) chars.push("e");
      return;
    }
  };
  step1b();

  // Step 1c
  if (
    chars.length > 2 &&
    (chars.at(-1) === "y" || chars.at(-1) === "Y") &&
    !isVowel(chars, chars.length - 2)
  ) {
    replace(1, "i");
  }

  // Step 2
  const step2: [string, string][] = [
    ["ization", "ize"],
    ["ational", "ate"],
    ["fulness", "ful"],
    ["ousness", "ous"],
    ["iveness", "ive"],
    ["tional", "tion"],
    ["biliti", "ble"],
    ["lessli", "less"],
    ["entli", "ent"],
    ["ation", "ate"],
    ["alism", "al"],
    ["aliti", "al"],
    ["ousli", "ous"],
    ["iviti", "ive"],
    ["fulli", "ful"],
    ["enci", "ence"],
    ["anci", "ance"],
    ["abli", "able"],
    ["izer", "ize"],
    ["ator", "ate"],
    ["alli", "al"],
    ["bli", "ble"],
    ["ogi", "og"],
    ["li", ""],
  ];
  for (const [suffix, replacement] of step2) {
    if (!endsWith(suffix)) continue;
    if (!inR1(suffix.length)) break;
    if (suffix === "ogi") {
      if (chars.at(-4) === "l") replace(3, "og");
    } else if (suffix === "li") {
      if (LI_ENDING.has(chars.at(-3) as string)) cut(2);
    } else replace(suffix.length, replacement);
    break;
  }

  // Step 3
  const step3: [string, string][] = [
    ["ational", "ate"],
    ["tional", "tion"],
    ["alize", "al"],
    ["icate", "ic"],
    ["iciti", "ic"],
    ["ative", ""],
    ["ical", "ic"],
    ["ness", ""],
    ["ful", ""],
  ];
  for (const [suffix, replacement] of step3) {
    if (!endsWith(suffix)) continue;
    if (!inR1(suffix.length)) break;
    if (suffix === "ative") {
      if (inR2(suffix.length)) cut(suffix.length);
    } else replace(suffix.length, replacement);
    break;
  }

  // Step 4
  const step4 = [
    "ement",
    "ance",
    "ence",
    "able",
    "ible",
    "ment",
    "ant",
    "ent",
    "ism",
    "ate",
    "iti",
    "ous",
    "ive",
    "ize",
    "ion",
    "al",
    "er",
    "ic",
  ];
  for (const suffix of step4) {
    if (!endsWith(suffix)) continue;
    if (!inR2(suffix.length)) break;
    if (suffix === "ion") {
      const before = chars.at(-4);
      if (before === "s" || before === "t") cut(3);
    } else cut(suffix.length);
    break;
  }

  // Step 5
  if (chars.at(-1) === "e") {
    if (inR2(1) || (inR1(1) && !endsShortSyllable(chars, chars.length - 1))) cut(1);
  } else if (chars.at(-1) === "l" && inR2(1) && chars.at(-2) === "l") {
    cut(1);
  }
  void r1;
  void r2;
  return marked ? word().replaceAll("Y", "y") : word();
}
