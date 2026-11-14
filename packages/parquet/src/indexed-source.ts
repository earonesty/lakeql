import { LakeqlError, type MemoryBudget, type MemoryScope, throwIfAborted } from "lakeql-core";
import { footerDirectoryBytes } from "./footer-directory.js";
import { readParquetFooterEnvelope } from "./incremental-metadata.js";
import { indexedLeafBytes, openIndexedParquetEnvelope } from "./indexed-footer.js";
import { readParquetFooterBuffer } from "./metadata-cache.js";
import { ParquetReadScope } from "./read-scope.js";
import type { StoreAsyncBuffer } from "./types.js";

/** Owns footer/directory backing and raw compact-tree admission until disposal.
 * Converted metadata JS still needs separate admission.
 * Borrowed reads require source lifetime; acquired groups independently pin footer backing.
 */
export async function acquireIndexedParquetSource(
  file: StoreAsyncBuffer,
  budget: MemoryBudget | MemoryScope,
  signals: readonly AbortSignal[] = [],
) {
  for (const signal of signals) throwIfAborted(signal);
  const stage = new ParquetReadScope(file, budget, signals);
  try {
    let acquisition = stage.allocations.childScope();
    let footer = await readParquetFooterBuffer(stage.file, acquisition);
    // readParquetFooterBuffer has already validated the trailer and extent.
    const footerBytes = new DataView(footer).getUint32(footer.byteLength - 8, true) + 8;
    let envelope: ReturnType<typeof readParquetFooterEnvelope> | undefined;
    const narrow = async () => {
      envelope = undefined;
      acquisition.dispose();
      footer = new ArrayBuffer(0);
      acquisition = stage.allocations.childScope();
      footer = await readParquetFooterBuffer(stage.file, acquisition, footerBytes);
    };
    try {
      envelope = readParquetFooterEnvelope(footer, undefined, acquisition);
    } catch (error) {
      if (
        !(error instanceof LakeqlError) ||
        error.code !== "LAKEQL_BUDGET_EXCEEDED" ||
        footer.byteLength <= footerBytes
      )
        throw error;
      await narrow();
      envelope = readParquetFooterEnvelope(footer, undefined, acquisition);
    }
    const directoryBytes = footerDirectoryBytes(envelope.rowGroupCount);
    // Retain a larger first read when the directory fits. Under pressure, discard
    // its page prefix before fetching exact backing; never overlap both buffers.
    if (
      directoryBytes + indexedLeafBytes(envelope.metadata) >
        stage.allocations.availableAllocationBytes &&
      footer.byteLength > footerBytes
    ) {
      await narrow();
      envelope = readParquetFooterEnvelope(footer, undefined, acquisition);
    }
    return indexedSourceHandle(
      stage,
      acquisition,
      openIndexedParquetEnvelope(envelope, stage.allocations),
    );
  } catch (error) {
    stage.dispose();
    throw error;
  }
}

/** Keep the public handle outside acquisition scratch's closure context. Closing
 * either the source or its parent detaches all state; acquired groups own pins.
 */
function indexedSourceHandle(
  stage: ParquetReadScope,
  acquisition: MemoryScope,
  source: ReturnType<typeof openIndexedParquetEnvelope>,
) {
  let active:
    | {
        stage: ParquetReadScope;
        acquisition: MemoryScope;
        source: ReturnType<typeof openIndexedParquetEnvelope>;
      }
    | undefined = { stage, acquisition, source };
  const current = () => {
    if (active === undefined || active.stage.allocations.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Indexed footer source is closed");
    return active;
  };
  const dispose = () => {
    const previous = active;
    active = undefined;
    if (previous === undefined) return;
    previous.stage.allocations.signal.removeEventListener("abort", dispose);
    previous.stage.dispose();
  };
  stage.allocations.signal.addEventListener("abort", dispose, { once: true });
  if (stage.allocations.disposed) dispose();
  return {
    get metadata() {
      return current().source.metadata;
    },
    get rowGroupCount() {
      return current().source.rowGroupCount;
    },
    readRowGroup(index: number) {
      return current().source.readRowGroup(index);
    },
    acquireRowGroup(index: number) {
      const state = current();
      const groupScope = state.stage.allocations.childScope();
      try {
        const group = state.source.readRowGroup(index, groupScope);
        // Pin footer views and raw group independently of source disposal.
        // Directory backing is not needed by the completed group.
        groupScope.adopt(state.acquisition.retainTreeAllocations(undefined));
        return groupScope.retainAllocations(group);
      } finally {
        groupScope.dispose();
      }
    },
    dispose,
  };
}
