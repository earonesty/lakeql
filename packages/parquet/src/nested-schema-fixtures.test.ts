import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import {
  batchFromStructVector,
  MemoryBudget,
  MemoryScope,
  memoryStore,
  vectorLength,
  vectorValue,
} from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedColumnPages } from "./nested-column-pages.js";
import { readNestedColumnRows } from "./nested-column-rows.js";
import { readNestedProjectedBatches } from "./nested-projected-batches.js";
import { readAlignedNestedRows } from "./nested-row-alignment.js";
import { readNestedRowGroupBatches } from "./nested-row-group-read.js";
import { readNestedSchemaBatches } from "./nested-schema-batches.js";
import { compileNestedProjection } from "./nested-schema-plan.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { asyncBufferFromStore } from "./store-buffer.js";

for (const [name, expected] of [
  [
    "nested_lists.snappy.parquet",
    [
      {
        a: [
          [["a", "b"], ["c"]],
          [null, ["d"]],
        ],
        b: 1,
      },
      {
        a: [
          [
            ["a", "b"],
            ["c", "d"],
          ],
          [null, ["e"]],
        ],
        b: 1,
      },
      {
        a: [
          [["a", "b"], ["c", "d"], ["e"]],
          [null, ["f"]],
        ],
        b: 1,
      },
    ],
  ],
  [
    "list_columns.parquet",
    [
      { int64_list: [1n, 2n, 3n], utf8_list: ["abc", "efg", "hij"] },
      { int64_list: [null, 1n], utf8_list: null },
      { int64_list: [4n], utf8_list: ["efg", null, "hij", "xyz"] },
    ],
  ],
  ["null_list.parquet", [{ emptylist: [] }]],
  [
    "old_list_structure.parquet",
    [
      {
        a: [
          [1, 2],
          [3, 4],
        ],
      },
    ],
  ],
  [
    "map_no_value.parquet",
    [
      {
        my_map: { "1": null, "2": null, "3": null },
        my_map_no_v: { "1": null, "2": null, "3": null },
        my_list: [1, 2, 3],
      },
      {
        my_map: { "4": null, "5": null, "6": null },
        my_map_no_v: { "4": null, "5": null, "6": null },
        my_list: [4, 5, 6],
      },
      {
        my_map: { "7": null, "8": null, "9": null },
        my_map_no_v: { "7": null, "8": null, "9": null },
        my_list: [7, 8, 9],
      },
    ],
  ],
  [
    "nested_structs.rust.parquet",
    [
      {
        roll_num: {
          min: 190406409000602n,
          max: 190407175004000n,
          mean: 190406671229999n,
          count: 495n,
          sum: 94251302258849568n,
          variance: 0n,
        },
      },
    ],
  ],
] as const) {
  const fixture = new URL(
    `../../../fixtures/external/parquet-testing/data/${name}`,
    import.meta.url,
  );
  it.skipIf(!existsSync(fixture))(`executes streamed compiled schema for ${name}`, async () => {
    const bytes = readFileSync(fixture);
    const metadata = parquetMetadata(
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
    );
    const store = memoryStore();
    await store.put(name, bytes);
    const file = await asyncBufferFromStore(store, name);
    const requested = Object.keys(expected[0]);
    const columns = (metadata.row_groups[0]?.columns ?? [])
      .filter((chunk) => requested.includes(chunk.meta_data?.path_in_schema[0] ?? ""))
      .map((chunk) => {
        if (!chunk.meta_data) throw Error("Missing column");
        return chunk.meta_data;
      });
    const paths = columns.map((column) => getSchemaPath(metadata.schema, column.path_in_schema));
    const root = paths[0]?.[0];
    if (!root) throw Error("Missing root");
    const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
    try {
      const plan = compileNestedProjection(root, paths, requested, scope);
      const sources = () =>
        columns.map((column, i) => {
          const schemaPath = paths[i];
          const element = schemaPath?.at(-1)?.element;
          if (!schemaPath || !element) throw Error("Missing leaf");
          return readNestedColumnRows(
            readNestedColumnPages(
              file,
              column,
              {
                schemaPath,
                element,
                type: column.type,
                codec: column.codec,
                pathInSchema: column.path_in_schema,
                parsers: lakeqlParquetParsers,
              },
              scope,
            ),
            scope,
          );
        });
      let row = 0;
      for await (const owner of readNestedSchemaBatches(
        readAlignedNestedRows(sources(), scope, 2),
        plan,
        paths,
        scope,
        { rowStart: 0, rowEnd: expected.length, batchSize: 1 },
      )) {
        expect(vectorLength(owner.value)).toBe(1);
        expect(vectorValue(owner.value, 0)).toEqual(expected[row++]);
      }
      expect(row).toBe(expected.length);
      const baseline = scope.budget.usedBytes;
      for (const reader of [
        readNestedRowGroupBatches(file, metadata, scope, {
          columns: requested,
          batchSize: 2,
          maxConcurrentPageReads: 2,
        }),
        readOwnedParquetVectorBatches(file, metadata, scope.budget, {
          columns: requested,
          batchSize: 2,
          maxConcurrentPageReads: 2,
        }),
      ]) {
        let adapterPosition = 0;
        for await (const owner of reader) {
          try {
            expect(owner.value.rowOffset).toBe(adapterPosition);
            const batch = owner.value.batch;
            for (let i = 0; i < batch.rowCount; i++) {
              const reference = expected[adapterPosition++] as Record<string, unknown>;
              for (const name of requested) {
                const field = batch.columns[name];
                if (!field) throw Error("Missing public field");
                expect(vectorValue(field, i)).toEqual(reference[name]);
              }
            }
          } finally {
            owner.release();
          }
        }
        expect(adapterPosition).toBe(expected.length);
        expect(scope.budget.usedBytes).toBe(baseline);
      }

      const descriptors = columns.map((metadata, i) => {
        const schemaPath = paths[i];
        const element = schemaPath?.at(-1)?.element;
        if (!schemaPath || !element) throw Error("Missing descriptor");
        return {
          metadata,
          decoder: {
            schemaPath,
            element,
            type: metadata.type,
            codec: metadata.codec,
            pathInSchema: metadata.path_in_schema,
            parsers: lakeqlParquetParsers,
          },
        };
      });
      const controller = new AbortController();
      const cancellation = new MemoryScope(scope.budget, [controller.signal]);
      let activeReads = 0,
        startedReads = 0;
      const targetReads = Math.min(2, descriptors.length);
      const delayed = {
        ...file,
        sliceOwned: (_start: number, _end: number, readScope: MemoryScope): Promise<never> =>
          new Promise((_resolve, reject) => {
            activeReads++;
            startedReads++;
            const abort = () => {
              readScope.signal.removeEventListener("abort", abort);
              activeReads--;
              reject(Error("in-flight read aborted"));
            };
            readScope.signal.addEventListener("abort", abort, { once: true });
            if (readScope.signal.aborted) abort();
            if (startedReads === targetReads) controller.abort();
          }),
      };
      await expect(
        readNestedProjectedBatches(delayed, root, descriptors, requested, cancellation, {
          rowStart: 0,
          rowEnd: expected.length,
          batchSize: 2,
          maxConcurrentReads: 2,
        }).next(),
      ).rejects.toThrow(/aborted|disposed/i);
      expect(startedReads).toBe(targetReads);
      expect(activeReads).toBe(0);
      cancellation.dispose();
      expect(scope.budget.usedBytes).toBe(baseline);

      const firstDescriptor = descriptors[0];
      if (!firstDescriptor) throw Error("Missing first descriptor");
      const unsupported = {
        metadata: { ...firstDescriptor.metadata, codec: "LZO" as const },
        decoder: { ...firstDescriptor.decoder, codec: "LZO" as const },
      };
      let unexpectedReads = 0;
      const unreadable = {
        ...file,
        sliceOwned: () => {
          unexpectedReads++;
          throw Error("Unexpected read before preflight");
        },
      };
      const rejected = readNestedProjectedBatches(
        unreadable,
        root,
        [unsupported, ...descriptors.slice(1)],
        requested,
        scope,
        { rowStart: 0, rowEnd: expected.length, batchSize: 2 },
      );
      await expect(rejected.next()).rejects.toThrow(/Unsupported nested projected column/);
      expect(unexpectedReads).toBe(0);
      expect(scope.budget.usedBytes).toBe(baseline);
      const ignored = {
        ...unsupported,
        metadata: { ...unsupported.metadata, path_in_schema: ["unselected_unsupported_field"] },
      };
      let projectedPosition = 0;
      for await (const batch of readNestedProjectedBatches(
        file,
        root,
        [...descriptors, ignored],
        requested,
        scope,
        { rowStart: 0, rowEnd: expected.length, batchSize: 2, maxConcurrentReads: 2 },
      )) {
        for (let i = 0; i < vectorLength(batch.value); i++)
          expect(vectorValue(batch.value, i)).toEqual(expected[projectedPosition++]);
        const conversion = scope.childScope();
        try {
          const publicBatch = batchFromStructVector(batch.value, conversion);
          expect(publicBatch.rowCount).toBe(vectorLength(batch.value));
          expect(Object.keys(publicBatch.columns)).toEqual(requested);
        } finally {
          conversion.dispose();
        }
      }
      expect(projectedPosition).toBe(expected.length);
      expect(scope.budget.usedBytes).toBe(baseline);

      for (const batchSize of [1, 2, 4]) {
        let position = 0;
        const batches = readNestedSchemaBatches(
          readAlignedNestedRows(sources(), scope, 2),
          plan,
          paths,
          scope,
          { rowStart: 0, rowEnd: expected.length, batchSize },
        );
        for await (const batch of batches) {
          const length = vectorLength(batch.value);
          expect(length).toBe(Math.min(batchSize, expected.length - position));
          for (let i = 0; i < length; i++)
            expect(vectorValue(batch.value, i)).toEqual(expected[position++]);
        }
        expect(position).toBe(expected.length);
        expect(scope.budget.usedBytes).toBe(baseline);
      }
      for (const field of root.children.filter((field) => requested.includes(field.element.name))) {
        const projectionScope = scope.childScope();
        try {
          const name = field.element.name;
          const indexes = paths.flatMap((path, i) => (path[1]?.element.name === name ? [i] : []));
          const selectedPaths = indexes.map((i) => {
            const path = paths[i];
            if (!path) throw Error("Missing selected path");
            return path;
          });
          const allSources = sources();
          const selectedSources = indexes.map((i) => {
            const source = allSources[i];
            if (!source) throw Error("Missing selected source");
            return source;
          });
          const projection = compileNestedProjection(root, selectedPaths, [name], projectionScope);
          let position = 0;
          for await (const batch of readNestedSchemaBatches(
            readAlignedNestedRows(selectedSources, projectionScope, 2),
            projection,
            selectedPaths,
            projectionScope,
            { rowStart: 0, rowEnd: expected.length, batchSize: 2 },
          )) {
            for (let i = 0; i < vectorLength(batch.value); i++) {
              const reference = expected[position++] as Record<string, unknown>;
              expect(vectorValue(batch.value, i)).toEqual({ [name]: reference[name] });
            }
          }
          expect(position).toBe(expected.length);
        } finally {
          projectionScope.dispose();
        }
        expect(scope.budget.usedBytes).toBe(baseline);
      }
      for (const [start, end] of [
        [0, 0],
        [0, 1],
        [1, expected.length],
      ]) {
        let position = start;
        for await (const batch of readNestedSchemaBatches(
          readAlignedNestedRows(sources(), scope, 2),
          plan,
          paths,
          scope,
          { rowStart: start, rowEnd: end, batchSize: 2 },
        )) {
          for (let i = 0; i < vectorLength(batch.value); i++)
            expect(vectorValue(batch.value, i)).toEqual(expected[position++]);
        }
        expect(position).toBe(end);
        expect(scope.budget.usedBytes).toBe(baseline);
      }
      const early = readNestedSchemaBatches(
        readAlignedNestedRows(sources(), scope, 2),
        plan,
        paths,
        scope,
        { rowStart: 0, rowEnd: expected.length, batchSize: 1 },
      );
      const first = await early.next();
      if (first.done) throw Error("Missing first row");
      const retained = first.value.retain();
      await early.return(undefined);
      expect(vectorValue(retained.value, 0)).toEqual(expected[0]);
      retained.release();
      expect(scope.budget.usedBytes).toBe(baseline);

      async function* interruptedRows() {
        for await (const row of readAlignedNestedRows(sources(), scope, 2)) {
          yield row;
          throw Error("fixture source interrupted");
        }
      }
      const interrupted = readNestedSchemaBatches(interruptedRows(), plan, paths, scope, {
        rowStart: 0,
        rowEnd: expected.length + 1,
        batchSize: 2,
      });
      await expect(interrupted.next()).rejects.toThrow("fixture source interrupted");
      expect(scope.budget.usedBytes).toBe(baseline);

      const denied = new MemoryScope(scope.budget.child(0));
      const failed = readNestedSchemaBatches(
        readAlignedNestedRows(sources(), scope, 2),
        plan,
        paths,
        denied,
        { rowStart: 0, rowEnd: expected.length, batchSize: 1 },
      );
      await expect(failed.next()).rejects.toThrow(/budget/);
      expect(denied.budget.usedBytes).toBe(0);
      expect(scope.budget.usedBytes).toBe(baseline);
      denied.dispose();

      const mismatched = readNestedSchemaBatches(
        readAlignedNestedRows(sources(), scope, 2),
        plan,
        [],
        scope,
        { rowStart: 0, rowEnd: expected.length, batchSize: 1 },
      );
      await expect(mismatched.next()).rejects.toThrow(/mismatch/);
      expect(scope.budget.usedBytes).toBe(baseline);

      const independent = readNestedSchemaBatches(
        readAlignedNestedRows(sources(), scope, 2),
        plan,
        paths,
        scope,
        { rowStart: 0, rowEnd: expected.length, batchSize: 1 },
      );
      const output = await independent.next();
      if (output.done) throw Error("Missing retained row");
      const pinned = output.value.retain();
      try {
        await independent.return(undefined);
        scope.dispose();
        expect(vectorValue(pinned.value, 0)).toEqual(expected[0]);
      } finally {
        pinned.release();
      }
    } finally {
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  });
}
