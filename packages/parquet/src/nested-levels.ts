import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import { visitLevelRuns } from "./level-runs.js";

type Levels = Uint8Array | Uint32Array;
export interface NestedLevelOptions {
  count: number;
  maxRepetition: number;
  maxDefinition: number;
  /** Only the first page of a column must start a new row. */
  startsColumn: boolean;
  /** V2 lengths; omit for V1 length-prefixed level streams. */
  repetitionBytes?: number;
  definitionBytes?: number;
}

/** Decode only level streams; payload begins at valueOffset and is never read. */
export function readNestedLevels(
  bytes: Uint8Array,
  options: NestedLevelOptions,
  scope: MemoryScope,
) {
  const { count, maxRepetition, maxDefinition } = options;
  const invalid = () =>
    new LakeqlError("LAKEQL_VALIDATION_ERROR", "Malformed nested Parquet levels");
  for (const value of [count, maxRepetition, maxDefinition])
    if (!Number.isSafeInteger(value) || value < 0 || value > 0xffffffff) throw invalid();
  if (maxRepetition > maxDefinition) throw invalid();
  const construction = scope.childScope();
  try {
    const allocate = (maximum: number): Levels | undefined =>
      maximum === 0
        ? undefined
        : construction.allocate(
            count * (maximum <= 255 ? 1 : 4) + c.view,
            "nested level storage",
            () => (maximum <= 255 ? new Uint8Array(count) : new Uint32Array(count)),
          ).value;
    const repetition = allocate(maxRepetition);
    const definition = allocate(maxDefinition);
    const reader = {
      view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
      offset: 0,
    };
    let rowsStarted = maxRepetition === 0 ? count : 0;
    let presentCount = maxDefinition === 0 ? count : 0;
    const decode = (
      maximum: number,
      output: Levels | undefined,
      extent: number | undefined,
      repeated: boolean,
    ) => {
      if (maximum === 0 && (extent === undefined || extent === 0)) return;
      visitLevelRuns(
        reader,
        maximum === 0 ? 0 : 32 - Math.clz32(maximum),
        count,
        (value, start, run) => {
          if (value > maximum || (repeated && options.startsColumn && start === 0 && value !== 0))
            throw invalid();
          output?.fill(value, start, start + run);
          if (repeated && maximum > 0 && value === 0) rowsStarted += run;
          if (!repeated && maximum > 0 && value === maximum) presentCount += run;
        },
        extent,
        true,
      );
    };
    decode(maxRepetition, repetition, options.repetitionBytes, true);
    decode(maxDefinition, definition, options.definitionBytes, false);
    const result = construction.allocate(
      c.object + 5 * c.property,
      "nested level descriptor",
      () => ({
        repetition,
        definition,
        rowsStarted,
        presentCount,
        valueOffset: reader.offset,
      }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}
