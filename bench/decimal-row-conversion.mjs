import assert from "node:assert/strict";
import { convertDecimalValues, normalizeDecimalRows } from "../packages/parquet/dist/decimal.js";

const element = {
  name: "amount",
  type: "INT64",
  converted_type: "DECIMAL",
  precision: 15,
  scale: 4,
};
const metadata = {
  schema: [{ name: "root", num_children: 2 }, { name: "id", type: "INT32" }, element],
};
for (const count of [10000, 100000]) {
  const rows = Array.from({ length: count }, (_, id) =>
    Object.freeze({
      id,
      amount: id % 7 === 0 ? null : BigInt(id - Math.floor(count / 2)) * 17n,
    }),
  );
  const expected = rows.map(({ id, amount }) => ({
    id,
    amount: amount === null ? null : Number(`${amount}e-4`),
  }));
  for (const mode of ["singleton-arrays", "shared-scalar"]) {
    const times = [];
    for (let repetition = 0; repetition < 10; repetition++) {
      const start = performance.now();
      const result =
        mode === "singleton-arrays"
          ? rows.map((row) => ({
              ...row,
              amount: convertDecimalValues([row.amount], element, "amount")[0],
            }))
          : normalizeDecimalRows(metadata, ["id", "amount"], rows);
      const elapsed = performance.now() - start;
      assert.deepEqual(result, expected);
      if (repetition >= 3) times.push(elapsed);
    }
    times.sort((a, b) => a - b);
    console.log(
      JSON.stringify({
        rows: count,
        mode,
        medianMs: times[3],
        samples: 7,
        scope: "decimal row normalization only; output parity checked outside timed region",
      }),
    );
  }
}
