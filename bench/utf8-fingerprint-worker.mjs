import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import ts from "typescript";
import { createWorkerHeapRuntime } from "./worker-heap-runtime.mjs";

const baseline = process.env.LAKEQL_COMPARE_BASELINE ?? "3bf0ba1";
const oldSource = execFileSync(
  "git",
  ["show", `${baseline}:packages/core/src/utf8-fingerprint.ts`],
  { encoding: "utf8" },
);
const newSource = await readFile(
  new URL("../packages/core/src/utf8-fingerprint.ts", import.meta.url),
  "utf8",
);
const compile = (source) =>
  ts
    .transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
    })
    .outputText.replace("export function fnv1a64Utf8", "function fnv1a64Utf8");
const inputs = [
  ["ascii", "0123456789abcdef"],
  ["unicode", "é雪😀"],
  ["mixed", "ascii-prefix-雪😀"],
  ["surrogates", "\ud800x\udc00"],
].flatMap(([kind, token]) => [1, 64, 4096].map((repeats) => ({ kind, token, repeats })));
const script = `const prior = (() => { ${compile(oldSource)}; return fnv1a64Utf8; })();
const current = (() => { ${compile(newSource)}; return fnv1a64Utf8; })();
const inputs = ${JSON.stringify(inputs)};
export default { fetch(request) {
  const url = new URL(request.url);
  const input = inputs[Number(url.searchParams.get("case"))];
  const text = input.token.repeat(input.repeats);
  const iterations = Math.max(1, Math.floor(2000000 / text.length));
  const hash = url.searchParams.get("variant") === "prior" ? prior : current;
  let digest;
  for (let i = 0; i < iterations; i++) digest = hash(text);
  return Response.json({ digest: digest.toString(16), iterations });
} };`;
const runtime = await createWorkerHeapRuntime(script, Uint8Array.of(1));
const cases = [];
try {
  for (const [index, input] of inputs.entries()) {
    const text = input.token.repeat(input.repeats);
    let expected = 0xcbf29ce484222325n;
    for (const byte of new TextEncoder().encode(text))
      expected = BigInt.asUintN(64, (expected ^ BigInt(byte)) * 0x100000001b3n);
    const samples = { prior: [], current: [] };
    for (let round = 0; round < 10; round++) {
      for (const variant of round % 2 ? ["current", "prior"] : ["prior", "current"]) {
        const start = performance.now();
        const response = await runtime.dispatchFetch(
          `http://worker/?case=${index}&variant=${variant}`,
        );
        assert.equal(response.status, 200);
        const result = await response.json();
        const elapsed = performance.now() - start;
        assert.equal(result.digest, expected.toString(16));
        if (round >= 4) samples[variant].push(elapsed / result.iterations);
      }
    }
    const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
    cases.push({
      kind: input.kind,
      codeUnits: text.length,
      samplesMs: samples,
      priorMedianMs: median(samples.prior),
      currentMedianMs: median(samples.current),
    });
  }
} finally {
  await runtime.dispose();
}
console.log(
  JSON.stringify({
    baseline,
    sourceSha256: createHash("sha256").update(newSource).digest("hex"),
    runtime: "native workerd",
    cases,
    scope:
      "Host wall clock includes dispatch/response overhead; alternating variants in one warmed isolate, four warmup rounds and six measured rounds. Worker timing APIs are not used. Independent TextEncoder/BigInt oracle checks every result. Kernel only, no cloud IO or total heap claim.",
  }),
);
