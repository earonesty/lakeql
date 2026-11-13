import type { ColumnDecoder } from "hyparquet/src/types.js";
import { LakeqlError, type MemoryScope, type OwnedMemory, type Vector } from "lakeql-core";
import {
  type PreparedPageWindow,
  readPreparedPageBody,
  validatePreparedPageBody,
} from "./page-window.js";
import type { StoreAsyncBuffer } from "./types.js";
import { planFlatNumericVector, planFlatNumericVectorHeader } from "./vector-batches.js";

export interface NumericPageGroupEntry {
  page: PreparedPageWindow;
  decoder: ColumnDecoder;
}

/** Whole-group admission for prepared independent numeric pages. Retained peer
 * inputs stay charged separately. Unsupported capabilities decline before I/O.
 * execute transfers vectors into the caller scope; executeOwned returns independent
 * per-page owners so alignment can retire exhausted peers separately.
 */
export function planNumericPageGroup(
  entries: readonly NumericPageGroupEntry[],
  file: StoreAsyncBuffer,
) {
  if (file.ownedReadAllocationBytes === undefined) return undefined;
  const transportBytes = entries.map(
    ({ page }) => file.ownedReadAllocationBytes?.(page.bodyOffset, page.bodyEnd) ?? 0,
  );
  const plans = entries.map(({ page, decoder }) =>
    planFlatNumericVectorHeader(page.header, decoder),
  );
  if (plans.some((plan) => plan === undefined)) return undefined;
  const extents = entries.map(({ page }, i) => {
    const plan = plans[i];
    if (plan === undefined) throw new Error("Missing numeric group plan");
    const transport = transportBytes[i];
    if (
      transport === undefined ||
      !Number.isSafeInteger(transport) ||
      transport < page.bodyEnd - page.bodyOffset
    )
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid transport allocation extent");
    const extent = transport + plan.bytes;
    if (!Number.isSafeInteger(extent) || extent < 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid numeric group extent");
    return extent;
  });
  const bytes = extents.reduce((total, extent) => total + extent, 0);
  if (!Number.isSafeInteger(bytes))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Numeric group extent exceeds safe capacity");
  async function executeOwned(
    file: StoreAsyncBuffer,
    scope: MemoryScope,
    maxConcurrentReads = 4,
    decode: (work: () => Vector) => Vector = (work) => work(),
  ): Promise<OwnedMemory<Vector>[]> {
    if (!Number.isSafeInteger(maxConcurrentReads) || maxConcurrentReads < 1)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid numeric group concurrency");
    if (file.sliceOwned === undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Numeric group requires owned transport");
    for (const [i, { page }] of entries.entries()) {
      validatePreparedPageBody(file, page);
      const actual = file.ownedReadAllocationBytes?.(page.bodyOffset, page.bodyEnd);
      if (actual === undefined || actual !== transportBytes[i])
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Prepared transport capability changed");
    }
    const group = scope.childStage(bytes, "prepared numeric page group");
    const children: MemoryScope[] = [];
    try {
      // Partition every extent before dispatch so a later admission failure
      // cannot strand earlier transport work.
      for (const extent of extents) children.push(group.childStage(extent, "numeric group page"));
      const vectors: Vector[] = new Array(entries.length);
      let next = 0;
      let failed = false;
      let failure: unknown;
      await Promise.allSettled(
        Array.from({ length: Math.min(maxConcurrentReads, entries.length) }, async () => {
          while (!failed && next < entries.length) {
            const i = next++;
            try {
              const entry = entries[i];
              const child = children[i];
              const estimate = plans[i];
              if (entry === undefined || child === undefined || estimate === undefined)
                throw new Error("Missing numeric group stage");
              const { page, decoder } = entry;
              const transport = child.childStage(transportBytes[i] ?? 0, "numeric page transport");
              try {
                const input = await readPreparedPageBody(file, page, transport);
                const precise = planFlatNumericVector(input, page.header, decoder);
                if (precise !== undefined && precise.bytes > estimate.bytes)
                  throw new LakeqlError(
                    "LAKEQL_VALIDATION_ERROR",
                    "Numeric group exceeds prepared extent",
                  );
                const vector = decode(() =>
                  precise === undefined ? estimate.execute(input, child) : precise.execute(child),
                );
                if (precise?.borrowsInput ?? estimate.mayBorrowInput)
                  child.adopt(transport.retainAllocations(vector));
                vectors[i] = vector;
              } finally {
                transport.dispose();
              }
            } catch (error) {
              if (!failed) {
                failure = error;
                failed = true;
              }
              group.dispose();
              throw error;
            }
          }
        }),
      );
      if (failed) throw failure;
      const owners: OwnedMemory<Vector>[] = [];
      try {
        for (const [i, vector] of vectors.entries()) {
          const child = children[i];
          if (child === undefined) throw new Error("Missing numeric group result");
          owners.push(child.retainAllocations(vector));
        }
        return owners;
      } catch (error) {
        for (const owner of owners) owner.release();
        throw error;
      }
    } finally {
      // allSettled above keeps each in-flight allocation charged through failures.
      group.dispose();
    }
  }
  async function measureOwned(file: StoreAsyncBuffer, scope: MemoryScope) {
    const inputs: OwnedMemory<Uint8Array>[] = [];
    const measured: NonNullable<ReturnType<typeof planFlatNumericVector>>[] = [];
    let closed = false;
    const dispose = () => {
      closed = true;
      for (const input of inputs) input.release();
    };
    try {
      for (const [i, entry] of entries.entries()) {
        validatePreparedPageBody(file, entry.page);
        const extent = file.ownedReadAllocationBytes?.(entry.page.bodyOffset, entry.page.bodyEnd);
        if (file.sliceOwned === undefined || extent === undefined || extent !== transportBytes[i])
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Prepared transport capability changed");
        const transport = scope.childStage(extent, "measured numeric input");
        try {
          const input = await readPreparedPageBody(file, entry.page, transport);
          const estimate = plans[i];
          if (estimate === undefined) throw new Error("Missing numeric header estimate");
          const precise = planFlatNumericVector(input, entry.page.header, entry.decoder);
          if (precise !== undefined && precise.bytes > estimate.bytes)
            throw new LakeqlError(
              "LAKEQL_VALIDATION_ERROR",
              "Measured numeric extent exceeds header",
            );
          measured.push(
            precise ?? {
              bytes: estimate.bytes,
              borrowsInput: estimate.mayBorrowInput,
              execute: (stage) => estimate.execute(input, stage),
            },
          );
          inputs.push(transport.retainAllocations(input));
        } finally {
          transport.dispose();
        }
      }
      const measuredBytes = measured.reduce((total, plan) => total + plan.bytes, 0);
      if (!Number.isSafeInteger(measuredBytes))
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Measured numeric group exceeds safe capacity",
        );
      return {
        bytes: measuredBytes,
        dispose,
        async executeOwned(
          _file: StoreAsyncBuffer,
          target: MemoryScope,
          _maxConcurrentReads?: number,
          decode: (work: () => Vector) => Vector = (work) => work(),
        ) {
          if (closed || scope.disposed)
            throw new LakeqlError("LAKEQL_ABORTED", "Measured numeric inputs disposed");
          if (target.budget !== scope.budget)
            throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Measured numeric job budget changed");
          const group = target.childStage(measuredBytes, "measured numeric decode group");
          const owners: OwnedMemory<Vector>[] = [];
          try {
            for (const [i, plan] of measured.entries()) {
              const child = group.childStage(plan.bytes, "measured numeric page");
              try {
                const vector = decode(() => plan.execute(child));
                const input = inputs[i];
                if (plan.borrowsInput && input !== undefined) child.adopt(input.retain());
                owners.push(child.retainAllocations(vector));
              } finally {
                child.dispose();
              }
            }
            return owners;
          } catch (error) {
            for (const owner of owners) owner.release();
            throw error;
          } finally {
            group.dispose();
          }
        },
      };
    } catch (error) {
      dispose();
      throw error;
    }
  }
  return {
    bytes,
    measureOwned,
    executeOwned,
    async execute(
      file: StoreAsyncBuffer,
      scope: MemoryScope,
      maxConcurrentReads = 4,
    ): Promise<Vector[]> {
      const owners = await executeOwned(file, scope, maxConcurrentReads);
      try {
        return owners.map((owner) => scope.adopt(owner).value);
      } catch (error) {
        for (const owner of owners) owner.release();
        throw error;
      }
    },
  };
}
