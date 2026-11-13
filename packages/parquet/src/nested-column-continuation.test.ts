import type { ColumnMetaData, PageHeader } from "hyparquet";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { ByteWriter } from "hyparquet-writer/src/bytewriter.js";
import { writePageHeader } from "hyparquet-writer/src/datapage.js";
import { MemoryBudget, MemoryScope, memoryStore, vectorLength, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedColumnPages } from "./nested-column-pages.js";
import { readNestedColumnRows } from "./nested-column-rows.js";
import { windowNestedFragments } from "./nested-fragment-window.js";
import { joinNestedFragments, joinNestedRowFragments } from "./nested-row-fragments.js";
import { asyncBufferFromStore } from "./store-buffer.js";

function appendPage(
  writer: Parameters<typeof writePageHeader>[0],
  reps: number[],
  defs: number[],
  values: number[],
) {
  const body = new ByteWriter();
  body.appendUint32(reps.length * 2);
  for (const rep of reps) body.appendBytes(Uint8Array.of(2, rep));
  body.appendUint32(defs.length * 2);
  for (const def of defs) body.appendBytes(Uint8Array.of(2, def));
  for (const value of values) body.appendInt32(value);
  writePageHeader(writer, {
    type: "DATA_PAGE",
    compressed_page_size: body.offset,
    uncompressed_page_size: body.offset,
    data_page_header: {
      num_values: reps.length,
      encoding: "PLAIN",
      definition_level_encoding: "RLE",
      repetition_level_encoding: "RLE",
    },
  } as PageHeader);
  writer.appendBytes(body.getBytes());
}

for (const malformed of [false, true])
  it(`carries a V1 repeated row across an empty page (malformed=${malformed})`, async () => {
    const writer = new ByteWriter();
    appendPage(writer, [0, 1], [2, 1], [7]);
    appendPage(writer, [], [], []);
    appendPage(writer, [1, 0], malformed ? [0, 2] : [2, 2], malformed ? [11] : [9, 11]);
    const store = memoryStore();
    await store.put("column.bin", writer.getBytes());
    const file = await asyncBufferFromStore(store, "column.bin");
    const path = getSchemaPath(
      [
        { name: "root", num_children: 1 },
        { name: "items", repetition_type: "REPEATED", num_children: 1 },
        { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
      ],
      ["items", "value"],
    );
    const element = path.at(-1)?.element;
    if (!element) throw Error("Missing leaf");
    const decoder: ColumnDecoder = {
      type: "INT32",
      codec: "UNCOMPRESSED",
      element,
      schemaPath: path,
      pathInSchema: ["items", "value"],
      parsers: DEFAULT_PARSERS,
    };
    const column: ColumnMetaData = {
      type: "INT32",
      codec: "UNCOMPRESSED",
      path_in_schema: ["items", "value"],
      encodings: ["PLAIN", "RLE"],
      num_values: 4n,
      data_page_offset: 0n,
      total_compressed_size: BigInt(writer.offset),
      total_uncompressed_size: BigInt(writer.offset),
    };
    const budget = new MemoryBudget(65536);
    const scope = new MemoryScope(budget);
    const iterator = readNestedColumnPages(file, column, decoder, scope);
    const first = await iterator.next();
    if (first.done) throw Error("Missing first page");
    const retained = first.value.retain();
    try {
      expect([0, 1].map((i) => vectorValue(retained.value.slots, i))).toEqual([7, null]);
      const empty = await iterator.next();
      if (empty.done) throw Error("Missing empty page");
      expect(vectorLength(empty.value.value.slots)).toBe(0);
      expect(empty.value.value.nextDefinition).toBe(1);
      if (malformed) await expect(iterator.next()).rejects.toThrow(/transition/);
      else {
        const last = await iterator.next();
        if (last.done) throw Error("Missing final page");
        expect(last.value.value.rows.continuesPrevious).toBe(true);
        expect([0, 1].map((i) => vectorValue(last.value.value.slots, i))).toEqual([9, 11]);
        expect((await iterator.next()).done).toBe(true);
      }
      scope.dispose();
      expect([0, 1].map((i) => vectorValue(retained.value.slots, i))).toEqual([7, null]);
    } finally {
      await iterator.return(undefined);
      scope.dispose();
      retained.release();
    }
    expect(budget.usedBytes).toBe(0);

    const rowScope = new MemoryScope(new MemoryBudget(65536));
    const rows = readNestedColumnRows(
      readNestedColumnPages(file, column, decoder, rowScope),
      rowScope,
    );
    if (malformed) {
      await expect(rows.next()).rejects.toThrow(/transition/);
      rowScope.dispose();
      expect(rowScope.budget.usedBytes).toBe(0);
    } else {
      const firstRow = await rows.next();
      if (firstRow.done) throw Error("Missing sealed row");
      const pinned = firstRow.value.retain();
      try {
        expect(pinned.value.row).toBe(0);
        expect(pinned.value.fragments).toHaveLength(2);
        const joinedScope = new MemoryScope(new MemoryBudget(65536));
        const joined = joinNestedRowFragments(pinned.value, path, joinedScope);
        expect(Array.from(joined.levels.repetition ?? [])).toEqual([0, 1, 1]);
        expect(Array.from(joined.levels.definition ?? [])).toEqual([2, 1, 2]);
        expect([0, 1].map((i) => vectorValue(joined.values, i))).toEqual([7, 9]);
        const joinedPeak = joinedScope.budget.peakBytes;
        joinedScope.dispose();
        const exactJoin = new MemoryScope(new MemoryBudget(joinedPeak));
        joinNestedRowFragments(pinned.value, path, exactJoin);
        exactJoin.dispose();
        const tightJoin = new MemoryScope(new MemoryBudget(joinedPeak - 1));
        expect(() => joinNestedRowFragments(pinned.value, path, tightJoin)).toThrow(/budget/);
        expect(tightJoin.budget.usedBytes).toBe(0);
        tightJoin.dispose();

        expect(
          pinned.value.fragments.flatMap((fragment) =>
            Array.from({ length: fragment.valueEnd - fragment.valueStart }, (_, i) =>
              vectorValue(fragment.page.values, fragment.valueStart + i),
            ),
          ),
        ).toEqual([7, 9]);
        expect(
          pinned.value.fragments.flatMap((fragment) =>
            Array.from(
              fragment.page.physical.levels.definition?.subarray(
                fragment.eventStart,
                fragment.eventEnd,
              ) ?? [],
            ),
          ),
        ).toEqual([2, 1, 2]);
        const finalRow = await rows.next();
        if (finalRow.done) throw Error("Missing final sealed row");
        expect(finalRow.value.value.row).toBe(1);
        expect(finalRow.value.value.fragments).toHaveLength(1);
        const fragments = [...pinned.value.fragments, ...finalRow.value.value.fragments];
        for (const [start, values, repetition] of [
          [0, [7, 9], [0, 1, 1]],
          [1, [11], [0]],
        ] as const) {
          const selection = new MemoryScope(new MemoryBudget(65536));
          try {
            const selected = windowNestedFragments(fragments, start, 1, selection);
            const joined = joinNestedFragments(selected, 1, path, selection);
            expect(Array.from(joined.levels.repetition ?? [])).toEqual(repetition);
            expect(
              Array.from({ length: vectorLength(joined.values) }, (_, i) =>
                vectorValue(joined.values, i),
              ),
            ).toEqual(values);
          } finally {
            selection.dispose();
          }
          expect(selection.budget.usedBytes).toBe(0);
        }
        const combinedScope = new MemoryScope(new MemoryBudget(65536));
        const combined = joinNestedFragments(fragments, 2, path, combinedScope);
        expect(combined.levels.rowsStarted).toBe(2);
        expect(Array.from(combined.levels.repetition ?? [])).toEqual([0, 1, 1, 0]);
        expect(Array.from(combined.levels.definition ?? [])).toEqual([2, 1, 2, 2]);
        expect([0, 1, 2].map((i) => vectorValue(combined.values, i))).toEqual([7, 9, 11]);
        const combinedPeak = combinedScope.budget.peakBytes;
        combinedScope.dispose();
        const exactCombined = new MemoryScope(new MemoryBudget(combinedPeak));
        joinNestedFragments(fragments, 2, path, exactCombined);
        exactCombined.dispose();
        const deniedCombined = new MemoryScope(new MemoryBudget(combinedPeak - 1));
        expect(() => joinNestedFragments(fragments, 2, path, deniedCombined)).toThrow(/budget/);
        expect(deniedCombined.budget.usedBytes).toBe(0);
        deniedCombined.dispose();
        const invalidCombined = new MemoryScope(new MemoryBudget(0));
        for (const count of [0, 1, 3, NaN])
          expect(() => joinNestedFragments(fragments, count, path, invalidCombined)).toThrow(
            /sealed/,
          );
        expect(() => joinNestedFragments(fragments.slice(1), 1, path, invalidCombined)).toThrow(
          /sealed/,
        );
        expect(invalidCombined.budget.usedBytes).toBe(0);
        invalidCombined.dispose();
        const firstFragment = fragments[0],
          middle = fragments[1],
          lastFragment = fragments[2];
        if (!firstFragment || !middle || !lastFragment) throw Error("Missing batch fragments");
        const mergedScope = new MemoryScope(new MemoryBudget(65536));
        joinNestedFragments(
          [
            firstFragment,
            { ...middle, eventEnd: lastFragment.eventEnd, valueEnd: lastFragment.valueEnd },
          ],
          2,
          path,
          mergedScope,
        );
        // Adjacent fragments on the second page require only one payload route.
        expect(mergedScope.budget.peakBytes).toBe(combinedPeak);
        mergedScope.dispose();

        expect((await rows.next()).done).toBe(true);
        rowScope.dispose();
        const fragment = pinned.value.fragments[0];
        if (!fragment) throw Error("Missing retained fragment");
        expect(vectorValue(fragment.page.values, fragment.valueStart)).toBe(7);
      } finally {
        await rows.return(undefined);
        rowScope.dispose();
        pinned.release();
      }
      expect(rowScope.budget.usedBytes).toBe(0);
      for (const blockSize of [2, 3]) {
        const blockScope = new MemoryScope(new MemoryBudget(65536));
        let blocks = 0;
        try {
          for await (const block of readNestedColumnRows(
            readNestedColumnPages(file, column, decoder, blockScope),
            blockScope,
            blockSize,
          )) {
            blocks++;
            expect(block.value.row).toBe(0);
            expect(block.value.rowCount).toBe(2);
            expect(block.value.fragments).toHaveLength(2);
            const joined = joinNestedRowFragments(block.value, path, blockScope);
            expect(joined.levels.rowsStarted).toBe(2);
            expect([0, 1, 2].map((i) => vectorValue(joined.values, i))).toEqual([7, 9, 11]);
          }
          expect(blocks).toBe(1);
        } finally {
          blockScope.dispose();
        }
        expect(blockScope.budget.usedBytes).toBe(0);
      }
      const consume = async (limit: number) => {
        const bounded = new MemoryScope(new MemoryBudget(limit));
        let seen = 0;
        try {
          for await (const row of readNestedColumnRows(
            readNestedColumnPages(file, column, decoder, bounded),
            bounded,
          )) {
            expect(row.value.row).toBe(seen++);
          }
          expect(seen).toBe(2);
          return bounded.budget.peakBytes;
        } finally {
          bounded.dispose();
          expect(bounded.budget.usedBytes).toBe(0);
        }
      };
      const peak = await consume(65536);
      expect(await consume(peak)).toBe(peak);
      await expect(consume(peak - 1)).rejects.toThrow(/budget/);
    }
  });
