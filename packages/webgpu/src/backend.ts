import {
  type BackendAssessment,
  type BackendExecutionContext,
  type BackendPlanningContext,
  type BackendRejection,
  type CompiledPhysicalFragment,
  estimateBatchBytes,
  LakeqlError,
  type PhysicalCapabilities,
  type PhysicalExecutionBackend,
  type PhysicalFragment,
  type PhysicalFragmentInput,
  type PhysicalFragmentResult,
  selectedRowCount,
  throwIfAborted,
  type Vector,
  validatePhysicalFragment,
} from "lakeql-core";
import {
  type CompiledWebGpuPredicate,
  compileWebGpuPredicate,
  type WebGpuPredicateColumn,
} from "./predicate.js";
import {
  WebGpuDeviceManager,
  type WebGpuDeviceOptions,
  type WebGpuRuntimeProvider,
} from "./runtime.js";

export interface WebGpuBackendOptions extends WebGpuDeviceOptions {
  id?: string;
  maxInputRows?: number;
  maxStorageBufferBytes?: number;
  workgroupSize?: number;
  cost?: Partial<WebGpuCostModel>;
}

export interface WebGpuCostModel {
  fixedMs: number;
  uploadBytesPerMs: number;
  computeRowsPerMs: number;
  readbackBytesPerMs: number;
  coldCompileMs: number;
}

interface WebGpuCompiledState {
  readonly predicate: CompiledWebGpuPredicate;
}

const DEFAULT_COST: WebGpuCostModel = {
  fixedMs: 0.12,
  uploadBytesPerMs: 2_000_000,
  computeRowsPerMs: 10_000_000,
  readbackBytesPerMs: 1_000_000,
  coldCompileMs: 0.08,
};

const SUPPORTED_SHAPES = ["bool", "f32", "i32", "u32", "u8"] as const;

export class WebGpuPhysicalBackend implements PhysicalExecutionBackend {
  readonly id: string;
  readonly #devices: WebGpuDeviceManager;
  readonly #maxInputRows: number;
  readonly #maxStorageBufferBytes: number;
  readonly #workgroupSize: number;
  readonly #cost: WebGpuCostModel;
  readonly #pipelines = new Map<string, Promise<GPUComputePipeline>>();

  constructor(provider: WebGpuRuntimeProvider, options: WebGpuBackendOptions = {}) {
    this.id = options.id ?? "webgpu";
    this.#maxInputRows = options.maxInputRows ?? 16_777_216;
    this.#maxStorageBufferBytes = options.maxStorageBufferBytes ?? 128 * 1024 * 1024;
    this.#workgroupSize = options.workgroupSize ?? 256;
    if (!positiveInteger(this.#maxInputRows) || !positiveInteger(this.#maxStorageBufferBytes)) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "WebGPU row and storage limits must be positive integers",
      );
    }
    if (this.#workgroupSize !== 256) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "WebGPU workgroup size must match the compiled 256-lane kernel",
      );
    }
    this.#cost = { ...DEFAULT_COST, ...options.cost };
    for (const [name, value] of Object.entries(this.#cost)) {
      if (!Number.isFinite(value) || value <= 0) {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          `WebGPU cost parameter ${name} must be finite and positive`,
        );
      }
    }
    this.#devices = new WebGpuDeviceManager(provider, {
      ...(options.adapter === undefined ? {} : { adapter: options.adapter }),
      ...(options.device === undefined ? {} : { device: options.device }),
    });
    this.#devices.onInvalidated(() => this.#pipelines.clear());
  }

  capabilities(): PhysicalCapabilities {
    return {
      backendKind: "accelerator",
      operators: [
        {
          kind: "select",
          inputShapes: SUPPORTED_SHAPES,
          outputShapes: ["u8"],
          supportsNulls: true,
          supportsDictionary: false,
          notes: ["Three-valued predicates produce a CPU selection mask"],
        },
      ],
      fusedSequences: [],
      vectorShapes: SUPPORTED_SHAPES,
      semantics: {
        nulls: "lakeql",
        numeric: "lakeql",
        ordering: "none",
        aggregateState: "lakeql-snapshot",
      },
      limits: {
        maxInputRows: this.#maxInputRows,
        maxInputBytes: this.#maxStorageBufferBytes,
        maxOutputBytes: this.#maxStorageBufferBytes,
        maxDispatches: 1,
      },
      supportsResidentInput: false,
      supportsResidentOutput: false,
    };
  }

  assess(fragment: PhysicalFragment, context: BackendPlanningContext): BackendAssessment {
    const compilation = compileWebGpuPredicate(fragment);
    const reasons: BackendRejection[] = [...validatePhysicalFragment(fragment)];
    if (!compilation.supported) {
      reasons.push({ code: "operator", message: compilation.reason });
    } else {
      const physical = physicalTransferBytes(fragment, compilation.compiled);
      if (fragment.input.rowCount > this.#maxInputRows) {
        reasons.push({
          code: "limit",
          message: "Fragment exceeds the configured WebGPU row limit",
          details: { actual: fragment.input.rowCount, limit: this.#maxInputRows },
        });
      }
      if (physical.largestBufferBytes > this.#maxStorageBufferBytes) {
        reasons.push({
          code: "limit",
          message: "Fragment exceeds the configured WebGPU storage-buffer limit",
          details: {
            actual: physical.largestBufferBytes,
            limit: this.#maxStorageBufferBytes,
          },
        });
      }
      addBudgetReason(
        reasons,
        "accelerator memory bytes",
        context.budget?.maxAcceleratorMemoryBytes,
        physical.deviceBytes,
      );
      addBudgetReason(
        reasons,
        "accelerator upload bytes",
        context.budget?.maxAcceleratorUploadBytes,
        physical.uploadBytes,
      );
      addBudgetReason(
        reasons,
        "accelerator readback bytes",
        context.budget?.maxAcceleratorReadbackBytes,
        physical.outputBytes,
      );
    }
    const compiledResident =
      compilation.supported && this.#pipelines.has(this.#pipelineKey(compilation.compiled));
    const cost = webGpuCost(
      fragment,
      compilation.supported ? compilation.compiled : undefined,
      compiledResident,
      this.#cost,
    );
    return {
      supported: reasons.length === 0,
      reasons,
      cost,
      inputResident: false,
      compiledResident,
    };
  }

  async compile(fragment: PhysicalFragment): Promise<CompiledPhysicalFragment> {
    const assessment = this.assess(fragment, {});
    if (!assessment.supported) {
      throw new LakeqlError(
        "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
        `Backend ${this.id} cannot compile physical fragment ${fragment.id}`,
        { backendId: this.id, fragmentId: fragment.id, reasons: assessment.reasons },
      );
    }
    const compilation = compileWebGpuPredicate(fragment);
    if (!compilation.supported) {
      throw new LakeqlError("LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED", compilation.reason);
    }
    return {
      backendId: this.id,
      fragment,
      backendState: { predicate: compilation.compiled } satisfies WebGpuCompiledState,
    };
  }

  async execute(
    compiled: CompiledPhysicalFragment,
    input: PhysicalFragmentInput,
    context: BackendExecutionContext = {},
  ): Promise<PhysicalFragmentResult> {
    if (compiled.backendId !== this.id || !isCompiledState(compiled.backendState)) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Compiled WebGPU fragment does not belong to this backend",
      );
    }
    if (input.kind !== "batch") {
      throw new LakeqlError(
        "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
        "WebGPU selection requires a decoded batch input",
      );
    }
    validateInput(compiled.fragment, input);
    const signal = context.budget?.signal;
    const now = context.now ?? (() => Date.now());
    const startedAt = now();
    const predicate = compiled.backendState.predicate;
    const transfer = physicalTransferBytes(compiled.fragment, predicate);
    enforceRuntimeBudget(compiled.fragment.id, context, transfer);
    const selection = await this.#devices.scoped(
      this.id,
      async (lease) => {
        const deviceLimit = Math.min(
          lease.device.limits.maxBufferSize,
          lease.device.limits.maxStorageBufferBindingSize,
        );
        if (transfer.largestBufferBytes > deviceLimit) {
          throw new LakeqlError(
            "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
            "Fragment exceeds the acquired WebGPU device buffer limit",
            {
              fragmentId: compiled.fragment.id,
              actual: transfer.largestBufferBytes,
              limit: deviceLimit,
            },
          );
        }
        const buffers: GPUBuffer[] = [];
        try {
          const values = uploadValues(input.batch.columns, predicate.columns, input.batch.rowCount);
          const valueBuffer = createUploadBuffer(
            lease.device,
            lease.runtime.constants.bufferUsage,
            values,
            `lakeql:${compiled.fragment.id}:values`,
          );
          const validity = uploadValidity(
            input.batch.columns,
            predicate.columns,
            input.batch.rowCount,
          );
          const validityBuffer = createUploadBuffer(
            lease.device,
            lease.runtime.constants.bufferUsage,
            validity,
            `lakeql:${compiled.fragment.id}:validity`,
          );
          buffers.push(valueBuffer, validityBuffer);
          const entries: GPUBindGroupEntry[] = [
            { binding: 0, resource: { buffer: valueBuffer } },
            { binding: 1, resource: { buffer: validityBuffer } },
          ];

          const outputBytes = alignedBufferBytes(input.batch.rowCount * 4);
          const output = lease.device.createBuffer({
            label: `lakeql:${compiled.fragment.id}:selection`,
            size: outputBytes,
            usage:
              lease.runtime.constants.bufferUsage.STORAGE |
              lease.runtime.constants.bufferUsage.COPY_SRC,
          });
          const readback = lease.device.createBuffer({
            label: `lakeql:${compiled.fragment.id}:readback`,
            size: outputBytes,
            usage:
              lease.runtime.constants.bufferUsage.MAP_READ |
              lease.runtime.constants.bufferUsage.COPY_DST,
          });
          buffers.push(output, readback);
          entries.push({ binding: predicate.outputBinding, resource: { buffer: output } });

          const pipeline = await this.#pipeline(lease.device, lease.generation, predicate);
          throwIfAborted(signal);
          const bindGroup = lease.device.createBindGroup({
            layout: pipeline.getBindGroupLayout(0),
            entries,
          });
          const encoder = lease.device.createCommandEncoder({
            label: `lakeql:${compiled.fragment.id}`,
          });
          if (input.batch.rowCount > 0) {
            const pass = encoder.beginComputePass();
            pass.setPipeline(pipeline);
            pass.setBindGroup(0, bindGroup);
            pass.dispatchWorkgroups(Math.ceil(input.batch.rowCount / this.#workgroupSize));
            pass.end();
          }
          encoder.copyBufferToBuffer(output, 0, readback, 0, outputBytes);
          lease.device.queue.submit([encoder.finish()]);
          await lease.device.queue.onSubmittedWorkDone();
          throwIfAborted(signal);
          await readback.mapAsync(lease.runtime.constants.mapMode.READ, 0, outputBytes);
          try {
            const words = new Uint32Array(readback.getMappedRange(0, outputBytes));
            const result = new Uint8Array(input.batch.rowCount);
            for (let index = 0; index < result.length; index += 1) {
              result[index] = words[index] === 1 ? 1 : 0;
            }
            return result;
          } finally {
            readback.unmap();
          }
        } finally {
          for (const buffer of buffers) buffer.destroy();
        }
      },
      signal,
    );
    const elapsedMs = now() - startedAt;
    enforceElapsedBudget(compiled.fragment.id, context, elapsedMs, now);
    const selectedRows = selectedRowCount(selection.length, selection);
    if (
      context.enforceOutputRows !== false &&
      context.budget?.maxOutputRows !== undefined &&
      selectedRows > context.budget.maxOutputRows
    ) {
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        `Query exceeded output rows budget (${selectedRows} > ${context.budget.maxOutputRows})`,
        {
          resource: "output rows",
          actual: selectedRows,
          limit: context.budget.maxOutputRows,
          fragmentId: compiled.fragment.id,
        },
      );
    }
    return {
      output: { kind: "selection", selection },
      metrics: {
        backendId: this.id,
        elapsedMs,
        inputRows: input.batch.rowCount,
        selectedRows,
        outputRows: selectedRows,
        inputBytes: estimateBatchBytes(input.batch),
        uploadBytes: transfer.uploadBytes,
        readbackBytes: transfer.outputBytes,
        dispatches: input.batch.rowCount === 0 ? 0 : 1,
        replayed: false,
      },
    };
  }

  close(): void {
    this.#pipelines.clear();
    this.#devices.close();
  }

  async #pipeline(
    device: GPUDevice,
    generation: number,
    predicate: CompiledWebGpuPredicate,
  ): Promise<GPUComputePipeline> {
    const key = this.#pipelineKey(predicate, generation);
    let pipeline = this.#pipelines.get(key);
    if (pipeline === undefined) {
      pipeline = device.createComputePipelineAsync({
        label: "lakeql:selection",
        layout: "auto",
        compute: {
          module: device.createShaderModule({ label: "lakeql:selection", code: predicate.wgsl }),
          entryPoint: "main",
        },
      });
      this.#pipelines.set(key, pipeline);
      pipeline.catch(() => this.#pipelines.delete(key));
    }
    return pipeline;
  }

  #pipelineKey(predicate: CompiledWebGpuPredicate, generation = this.#devices.generation): string {
    return `${generation}\u0000${predicate.cacheKey}`;
  }
}

interface PhysicalTransfer {
  readonly uploadBytes: number;
  readonly outputBytes: number;
  readonly deviceBytes: number;
  readonly largestBufferBytes: number;
}

function physicalTransferBytes(
  fragment: PhysicalFragment,
  predicate: CompiledWebGpuPredicate,
): PhysicalTransfer {
  const bufferBytes = alignedBufferBytes(fragment.input.rowCount * 4);
  const outputBytes = bufferBytes;
  const packedInputBytes = alignedBufferBytes(
    predicate.columns.length * fragment.input.rowCount * 4,
  );
  const uploadBytes = packedInputBytes * 2;
  return {
    uploadBytes,
    outputBytes,
    deviceBytes: uploadBytes + outputBytes * 2,
    largestBufferBytes: Math.max(packedInputBytes, outputBytes),
  };
}

function webGpuCost(
  fragment: PhysicalFragment,
  predicate: CompiledWebGpuPredicate | undefined,
  compiledResident: boolean,
  model: WebGpuCostModel,
) {
  const transfer =
    predicate === undefined
      ? { uploadBytes: fragment.estimates.inputBytes, outputBytes: fragment.estimates.outputBytes }
      : physicalTransferBytes(fragment, predicate);
  const uploadMs = transfer.uploadBytes / model.uploadBytesPerMs;
  const computeMs = fragment.input.rowCount / model.computeRowsPerMs;
  const readbackMs = transfer.outputBytes / model.readbackBytesPerMs;
  const compileMs = compiledResident ? 0 : model.coldCompileMs;
  const synchronizationMs = model.fixedMs;
  return {
    totalMs: uploadMs + computeMs + readbackMs + compileMs + synchronizationMs,
    inputConversionMs: 0,
    uploadMs,
    compileMs,
    computeMs,
    synchronizationMs,
    readbackMs,
    outputConversionMs: fragment.input.rowCount / 20_000_000,
  };
}

function createUploadBuffer(
  device: GPUDevice,
  usage: WebGpuRuntimeBufferUsage,
  data: ArrayBufferView,
  label: string,
): GPUBuffer {
  const size = alignedBufferBytes(data.byteLength);
  const buffer = device.createBuffer({
    label,
    size,
    usage: usage.STORAGE | usage.COPY_DST,
  });
  if (data.byteLength > 0) {
    device.queue.writeBuffer(buffer, 0, data.buffer, data.byteOffset, data.byteLength);
  }
  return buffer;
}

type WebGpuRuntimeBufferUsage = {
  readonly COPY_DST: GPUBufferUsageFlags;
  readonly STORAGE: GPUBufferUsageFlags;
};

function uploadValues(
  vectors: Record<string, Vector>,
  columns: readonly WebGpuPredicateColumn[],
  rowCount: number,
): Uint32Array {
  const output = new Uint32Array(columns.length * rowCount);
  for (const column of columns) {
    const vector = vectors[column.name];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${column.name}`);
    }
    if (vector.type !== column.shape) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", `Column ${column.name} changed physical shape`);
    }
    const offset = column.ordinal * rowCount;
    switch (vector.type) {
      case "f32":
      case "i32":
      case "u32":
        output.set(
          new Uint32Array(
            vector.values.buffer,
            vector.values.byteOffset,
            vector.values.byteLength / 4,
          ),
          offset,
        );
        break;
      case "bool":
      case "u8":
        output.set(vector.values, offset);
        break;
      default:
        throw new LakeqlError(
          "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
          `Column ${column.name} cannot be uploaded to WebGPU`,
        );
    }
  }
  return output;
}

function uploadValidity(
  vectors: Record<string, Vector>,
  columns: readonly WebGpuPredicateColumn[],
  rowCount: number,
): Uint32Array {
  const output = new Uint32Array(columns.length * rowCount);
  for (const column of columns) {
    const vector = vectors[column.name];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${column.name}`);
    }
    const target = output.subarray(column.ordinal * rowCount, (column.ordinal + 1) * rowCount);
    if ("valid" in vector && vector.valid !== undefined) target.set(vector.valid);
    else target.fill(1);
  }
  return output;
}

function validateInput(
  fragment: PhysicalFragment,
  input: Extract<PhysicalFragmentInput, { kind: "batch" }>,
): void {
  if (fragment.input.rowCount !== input.batch.rowCount) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Physical input row count changed after planning", {
      expected: fragment.input.rowCount,
      actual: input.batch.rowCount,
    });
  }
  if (
    fragment.input.sourceIdentity !== undefined &&
    fragment.input.sourceIdentity !== input.sourceIdentity
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Physical input source identity changed");
  }
}

function enforceRuntimeBudget(
  fragmentId: string,
  context: BackendExecutionContext,
  transfer: PhysicalTransfer,
): void {
  const checks = [
    {
      resource: "accelerator memory bytes",
      limit: context.budget?.maxAcceleratorMemoryBytes,
      actual: transfer.deviceBytes,
    },
    {
      resource: "accelerator upload bytes",
      limit: context.budget?.maxAcceleratorUploadBytes,
      actual: transfer.uploadBytes,
    },
    {
      resource: "accelerator readback bytes",
      limit: context.budget?.maxAcceleratorReadbackBytes,
      actual: transfer.outputBytes,
    },
    {
      resource: "accelerator dispatches",
      limit: context.budget?.maxAcceleratorDispatches,
      actual: 1,
    },
  ];
  for (const check of checks) {
    if (check.limit !== undefined && check.actual > check.limit) {
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        `Fragment exceeded ${check.resource} budget (${check.actual} > ${check.limit})`,
        { resource: check.resource, actual: check.actual, limit: check.limit, fragmentId },
      );
    }
  }
}

function enforceElapsedBudget(
  fragmentId: string,
  context: BackendExecutionContext,
  elapsedMs: number,
  now: () => number,
): void {
  const actual = context.queryStartedAt === undefined ? elapsedMs : now() - context.queryStartedAt;
  if (context.budget?.maxElapsedMs !== undefined && actual > context.budget.maxElapsedMs) {
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      `Query exceeded elapsed milliseconds budget (${actual} > ${context.budget.maxElapsedMs})`,
      {
        resource: "elapsed milliseconds",
        actual,
        limit: context.budget.maxElapsedMs,
        fragmentId,
      },
    );
  }
}

function addBudgetReason(
  reasons: BackendRejection[],
  resource: string,
  limit: number | undefined,
  actual: number,
): void {
  if (limit !== undefined && actual > limit) {
    reasons.push({
      code: "budget",
      message: `Fragment exceeds ${resource} budget`,
      details: { resource, actual, limit },
    });
  }
}

function alignedBufferBytes(bytes: number): number {
  return Math.max(4, Math.ceil(bytes / 4) * 4);
}

function positiveInteger(value: number): boolean {
  return Number.isInteger(value) && value > 0;
}

function isCompiledState(value: unknown): value is WebGpuCompiledState {
  return (
    typeof value === "object" &&
    value !== null &&
    "predicate" in value &&
    typeof value.predicate === "object" &&
    value.predicate !== null
  );
}
