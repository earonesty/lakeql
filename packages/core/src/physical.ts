import {
  type Batch,
  predicateSelection,
  type Selection,
  selectedRowCount,
  selectedRowIndices,
  type Vector,
} from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Expr } from "./expr.js";
import type { AggregateSpec, OrderByTerm, QueryBudget } from "./query.js";
import { throwIfAborted } from "./store.js";
import {
  createVectorAggregateStates,
  restoreVectorAggregateStates,
  snapshotVectorAggregateStates,
  updateVectorAggregateStates,
  type VectorAggregateStateSnapshots,
} from "./vector-aggregate.js";
import {
  createVectorGroupByState,
  restoreVectorGroupByState,
  snapshotVectorGroupByState,
  updateVectorGroupByState,
  type VectorGroupByStateSnapshot,
} from "./vector-group-by.js";
import { type VectorProjectionSpec, vectorProjectBatch } from "./vector-project.js";
import { gatherBatch, vectorOrderByBatch, vectorTopKBatch } from "./vector-sort.js";

export type PhysicalVectorShape = Vector["type"];

export interface PhysicalColumnShape {
  shape: PhysicalVectorShape;
  nullable: boolean;
  dictionaryValueShape?: PhysicalColumnShape;
}

interface PhysicalInputBase {
  rowCount: number;
  columns: Record<string, PhysicalColumnShape>;
  sourceIdentity?: string;
}

export interface PhysicalBatchInput extends PhysicalInputBase {
  kind: "batch";
}

export interface PhysicalResidentInput extends PhysicalInputBase {
  kind: "resident";
  backendId: string;
  deviceGeneration: number;
  representation: string;
  cacheKey: string;
}

export type PhysicalInput = PhysicalBatchInput | PhysicalResidentInput;

export interface PhysicalSelect {
  kind: "select";
  predicate: Expr;
}

export interface PhysicalProject {
  kind: "project";
  select?: readonly string[];
  projections?: VectorProjectionSpec;
}

export interface PhysicalReduce {
  kind: "reduce";
  aggregates: AggregateSpec;
}

export interface PhysicalGroupedReduce {
  kind: "grouped-reduce";
  keys: readonly string[];
  aggregates: AggregateSpec;
  maxGroups?: number;
}

export interface PhysicalOrder {
  kind: "order";
  orderBy: readonly OrderByTerm[];
}

export interface PhysicalTopK {
  kind: "top-k";
  orderBy: readonly OrderByTerm[];
  limit: number;
  offset?: number;
}

export type PhysicalOperator =
  | PhysicalSelect
  | PhysicalProject
  | PhysicalReduce
  | PhysicalGroupedReduce
  | PhysicalOrder
  | PhysicalTopK;

export type PhysicalOperatorKind = PhysicalOperator["kind"];

export type PhysicalOutput =
  | { kind: "batch" }
  | { kind: "selection" }
  | { kind: "indices" }
  | { kind: "aggregate-snapshot" }
  | { kind: "grouped-aggregate-snapshot" }
  | { kind: "resident"; representation: string };

export interface PhysicalEstimates {
  rowCount: number;
  inputBytes: number;
  outputBytes: number;
  selectedRowCount?: number;
  groupCount?: number;
  retainedStateBytes?: number;
  dispatchCount?: number;
}

export interface PhysicalFragment {
  id: string;
  input: PhysicalInput;
  operators: readonly PhysicalOperator[];
  output: PhysicalOutput;
  estimates: PhysicalEstimates;
}

export interface PhysicalOperatorCapability {
  kind: PhysicalOperatorKind;
  inputShapes: readonly PhysicalVectorShape[] | "any";
  outputShapes: readonly PhysicalVectorShape[] | "any";
  supportsNulls: boolean;
  supportsDictionary: boolean;
  notes?: readonly string[];
}

export interface PhysicalSemanticCapabilities {
  nulls: "lakeql";
  numeric: "lakeql";
  ordering: "stable" | "unstable" | "none";
  aggregateState: "lakeql-snapshot" | "backend-private";
}

export interface PhysicalBackendLimits {
  maxInputRows?: number;
  maxInputBytes?: number;
  maxOutputBytes?: number;
  maxRetainedStateBytes?: number;
  maxDispatches?: number;
}

export interface PhysicalCapabilities {
  backendKind: "cpu" | "accelerator";
  operators: readonly PhysicalOperatorCapability[];
  fusedSequences: readonly (readonly PhysicalOperatorKind[])[];
  vectorShapes: readonly PhysicalVectorShape[];
  semantics: PhysicalSemanticCapabilities;
  limits: PhysicalBackendLimits;
  supportsResidentInput: boolean;
  supportsResidentOutput: boolean;
}

export type BackendRejectionCode =
  | "operator"
  | "sequence"
  | "input-shape"
  | "output"
  | "semantics"
  | "limit"
  | "budget"
  | "policy"
  | "unavailable";

export interface BackendRejection {
  code: BackendRejectionCode;
  message: string;
  operatorIndex?: number;
  details?: Record<string, unknown>;
}

export interface PhysicalCostEstimate {
  totalMs: number;
  inputConversionMs: number;
  uploadMs: number;
  compileMs: number;
  computeMs: number;
  synchronizationMs: number;
  readbackMs: number;
  outputConversionMs: number;
}

export interface BackendAssessment {
  supported: boolean;
  reasons: readonly BackendRejection[];
  cost: PhysicalCostEstimate;
  inputResident: boolean;
  compiledResident: boolean;
}

export type AcceleratorPolicy = "auto" | "disabled" | "required";

export interface BackendPlanningContext {
  policy?: AcceleratorPolicy;
  budget?: QueryBudget;
  inputResidencyKeys?: ReadonlySet<string>;
  compiledFragmentIds?: ReadonlySet<string>;
}

export interface BackendExecutionContext {
  budget?: QueryBudget;
  now?: () => number;
  queryStartedAt?: number;
  enforceOutputRows?: boolean;
  priorOutput?: Extract<
    PhysicalOutputValue,
    { kind: "aggregate-snapshot" | "grouped-aggregate-snapshot" }
  >;
}

export interface CompiledPhysicalFragment {
  backendId: string;
  fragment: PhysicalFragment;
  backendState?: unknown;
}

export type PhysicalFragmentInput =
  | { kind: "batch"; batch: Batch; sourceIdentity?: string }
  | {
      kind: "resident";
      backendId: string;
      deviceGeneration: number;
      representation: string;
      cacheKey: string;
      rowCount: number;
      handle: unknown;
    };

export type PhysicalOutputValue =
  | { kind: "batch"; batch: Batch }
  | { kind: "selection"; selection: Selection }
  | { kind: "indices"; indices: Uint32Array }
  | { kind: "aggregate-snapshot"; snapshot: VectorAggregateStateSnapshots }
  | { kind: "grouped-aggregate-snapshot"; snapshot: VectorGroupByStateSnapshot }
  | { kind: "resident"; representation: string; handle: unknown };

export interface PhysicalExecutionMetrics {
  backendId: string;
  elapsedMs: number;
  inputRows: number;
  selectedRows?: number;
  outputRows?: number;
  inputBytes: number;
  uploadBytes: number;
  readbackBytes: number;
  dispatches: number;
  replayed: boolean;
}

export interface PhysicalFragmentResult {
  output: PhysicalOutputValue;
  metrics: PhysicalExecutionMetrics;
}

export interface PhysicalExecutionBackend {
  readonly id: string;
  capabilities(): PhysicalCapabilities;
  assess(fragment: PhysicalFragment, context: BackendPlanningContext): BackendAssessment;
  compile(fragment: PhysicalFragment): Promise<CompiledPhysicalFragment>;
  execute(
    compiled: CompiledPhysicalFragment,
    input: PhysicalFragmentInput,
    context: BackendExecutionContext,
  ): Promise<PhysicalFragmentResult>;
}

export interface PhysicalPlacementCandidate {
  backendId: string;
  backendKind: PhysicalCapabilities["backendKind"];
  assessment: BackendAssessment;
  selected: boolean;
}

export interface PlannedPhysicalFragment {
  fragment: PhysicalFragment;
  backendId: string;
  candidates: readonly PhysicalPlacementCandidate[];
}

export class PhysicalBackendExecutionError extends LakeqlError {
  readonly backendId: string;
  readonly replayable: boolean;

  constructor(
    backendId: string,
    message: string,
    options: { replayable: boolean; cause?: unknown; details?: Record<string, unknown> },
  ) {
    super("LAKEQL_PHYSICAL_BACKEND_FAILURE", message, {
      backendId,
      replayable: options.replayable,
      ...options.details,
      cause: errorMessage(options.cause),
    });
    this.name = "PhysicalBackendExecutionError";
    this.backendId = backendId;
    this.replayable = options.replayable;
  }
}

const CPU_VECTOR_SHAPES: readonly PhysicalVectorShape[] = [
  "null",
  "f32",
  "f64",
  "i32",
  "u32",
  "u8",
  "i64",
  "timestamp",
  "bool",
  "utf8",
  "binary",
  "dict",
  "list",
  "struct",
  "map",
];

const CPU_OPERATOR_KINDS: readonly PhysicalOperatorKind[] = [
  "select",
  "project",
  "reduce",
  "grouped-reduce",
  "order",
  "top-k",
];

const CPU_CAPABILITIES: PhysicalCapabilities = {
  backendKind: "cpu",
  operators: CPU_OPERATOR_KINDS.map((kind) => ({
    kind,
    inputShapes: "any",
    outputShapes: "any",
    supportsNulls: true,
    supportsDictionary: true,
  })),
  fusedSequences: [
    ["select", "project", "reduce"],
    ["select", "project", "grouped-reduce"],
    ["select", "order", "project"],
    ["select", "top-k", "project"],
  ],
  vectorShapes: CPU_VECTOR_SHAPES,
  semantics: {
    nulls: "lakeql",
    numeric: "lakeql",
    ordering: "stable",
    aggregateState: "lakeql-snapshot",
  },
  limits: {},
  supportsResidentInput: false,
  supportsResidentOutput: false,
};

export class CpuPhysicalBackend implements PhysicalExecutionBackend {
  readonly id: string;

  constructor(id = "cpu") {
    this.id = id;
  }

  capabilities(): PhysicalCapabilities {
    return CPU_CAPABILITIES;
  }

  assess(fragment: PhysicalFragment, context: BackendPlanningContext): BackendAssessment {
    const reasons = [...validatePhysicalFragment(fragment), ...validateCpuPlacement(fragment)];
    return {
      supported: reasons.length === 0,
      reasons,
      cost: cpuCost(fragment),
      inputResident: false,
      compiledResident: context.compiledFragmentIds?.has(fragment.id) ?? false,
    };
  }

  async compile(fragment: PhysicalFragment): Promise<CompiledPhysicalFragment> {
    const reasons = [...validatePhysicalFragment(fragment), ...validateCpuPlacement(fragment)];
    if (reasons.length > 0) throw unsupportedBackend(this.id, fragment, reasons);
    return { backendId: this.id, fragment };
  }

  async execute(
    compiled: CompiledPhysicalFragment,
    input: PhysicalFragmentInput,
    context: BackendExecutionContext,
  ): Promise<PhysicalFragmentResult> {
    if (compiled.backendId !== this.id) {
      throw new PhysicalBackendExecutionError(
        this.id,
        `Compiled fragment belongs to backend ${compiled.backendId}`,
        { replayable: false, details: { compiledBackendId: compiled.backendId } },
      );
    }
    if (input.kind !== "batch") {
      throw new PhysicalBackendExecutionError(this.id, "CPU backend requires a batch input", {
        replayable: false,
        details: { inputKind: input.kind },
      });
    }
    const now = context.now ?? (() => Date.now());
    const startedAt = now();
    const budget = context.budget;
    const aggregateOptions = budget === undefined ? {} : { budget };
    throwIfAborted(budget?.signal);
    validateRuntimeInput(compiled.fragment.input, input);

    let batch = input.batch;
    let selection: Selection | undefined;
    let matchedRows = batch.rowCount;
    let terminal:
      | Extract<PhysicalOutputValue, { kind: "aggregate-snapshot" | "grouped-aggregate-snapshot" }>
      | undefined;

    for (const operator of compiled.fragment.operators) {
      throwIfAborted(budget?.signal);
      if (terminal !== undefined) {
        throw new PhysicalBackendExecutionError(
          this.id,
          `Operator ${operator.kind} follows a terminal reduction`,
          { replayable: false, details: { fragmentId: compiled.fragment.id } },
        );
      }
      switch (operator.kind) {
        case "select": {
          const next = predicateSelection(batch, operator.predicate);
          selection = selection === undefined ? next : intersectSelections(selection, next);
          matchedRows = selectedRowCount(batch.rowCount, selection);
          break;
        }
        case "project":
          batch = vectorProjectBatch(batch, operator.select, operator.projections);
          break;
        case "reduce": {
          if (
            context.priorOutput !== undefined &&
            context.priorOutput.kind !== "aggregate-snapshot"
          ) {
            throw priorOutputMismatch("aggregate-snapshot", context.priorOutput.kind);
          }
          const states =
            context.priorOutput?.kind === "aggregate-snapshot"
              ? restoreVectorAggregateStates(context.priorOutput.snapshot, aggregateOptions)
              : createVectorAggregateStates(operator.aggregates, aggregateOptions);
          updateVectorAggregateStates(
            states,
            operator.aggregates,
            batch,
            selection,
            aggregateOptions,
          );
          terminal = {
            kind: "aggregate-snapshot",
            snapshot: snapshotVectorAggregateStates(states),
          };
          break;
        }
        case "grouped-reduce": {
          if (
            context.priorOutput !== undefined &&
            context.priorOutput.kind !== "grouped-aggregate-snapshot"
          ) {
            throw priorOutputMismatch("grouped-aggregate-snapshot", context.priorOutput.kind);
          }
          const groupOptions = {
            ...aggregateOptions,
            ...(operator.maxGroups === undefined ? {} : { maxGroups: operator.maxGroups }),
          };
          const state =
            context.priorOutput?.kind === "grouped-aggregate-snapshot"
              ? restoreVectorGroupByState(
                  operator.keys,
                  operator.aggregates,
                  context.priorOutput.snapshot,
                  groupOptions,
                )
              : createVectorGroupByState(operator.keys, operator.aggregates);
          updateVectorGroupByState(state, batch, selection, {
            ...groupOptions,
          });
          terminal = {
            kind: "grouped-aggregate-snapshot",
            snapshot: snapshotVectorGroupByState(state),
          };
          break;
        }
        case "order":
          enforceBufferedRowsBudget(
            selectedRowCount(batch.rowCount, selection),
            budget,
            compiled.fragment.id,
          );
          batch = vectorOrderByBatch(batch, operator.orderBy, selection);
          selection = undefined;
          break;
        case "top-k":
          enforceBufferedRowsBudget(
            Math.min(
              selectedRowCount(batch.rowCount, selection),
              operator.limit + (operator.offset ?? 0),
            ),
            budget,
            compiled.fragment.id,
          );
          batch = vectorTopKBatch(
            batch,
            operator.orderBy,
            {
              limit: operator.limit,
              ...(operator.offset === undefined ? {} : { offset: operator.offset }),
            },
            selection,
          );
          selection = undefined;
          break;
      }
    }

    const output = cpuOutput(compiled.fragment.output, batch, selection, terminal);
    const finishedAt = now();
    const elapsedMs = finishedAt - startedAt;
    const budgetElapsedMs = finishedAt - (context.queryStartedAt ?? startedAt);
    const outputRows = physicalOutputRows(output);
    if (
      context.enforceOutputRows !== false &&
      outputRows !== undefined &&
      budget?.maxOutputRows !== undefined &&
      outputRows > budget.maxOutputRows
    ) {
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        `Query exceeded output rows budget (${outputRows} > ${budget.maxOutputRows})`,
        {
          resource: "output rows",
          limit: budget.maxOutputRows,
          actual: outputRows,
          fragmentId: compiled.fragment.id,
        },
      );
    }
    if (budget?.maxElapsedMs !== undefined && budgetElapsedMs > budget.maxElapsedMs) {
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        `Query exceeded elapsed milliseconds budget (${budgetElapsedMs} > ${budget.maxElapsedMs})`,
        {
          resource: "elapsed milliseconds",
          limit: budget.maxElapsedMs,
          actual: budgetElapsedMs,
          fragmentId: compiled.fragment.id,
        },
      );
    }
    return {
      output,
      metrics: {
        backendId: this.id,
        elapsedMs,
        inputRows: input.batch.rowCount,
        selectedRows: matchedRows,
        ...(outputRows === undefined ? {} : { outputRows }),
        inputBytes: estimateBatchBytes(input.batch),
        uploadBytes: 0,
        readbackBytes: 0,
        dispatches: 0,
        replayed: false,
      },
    };
  }
}

export function physicalInputFromBatch(
  batch: Batch,
  options: { sourceIdentity?: string } = {},
): PhysicalBatchInput {
  const columns: Record<string, PhysicalColumnShape> = {};
  for (const [name, vector] of Object.entries(batch.columns)) {
    columns[name] = physicalColumnShape(vector);
  }
  return {
    kind: "batch",
    rowCount: batch.rowCount,
    columns,
    ...(options.sourceIdentity === undefined ? {} : { sourceIdentity: options.sourceIdentity }),
  };
}

export function estimateBatchBytes(batch: Batch): number {
  let bytes = 0;
  for (const vector of Object.values(batch.columns)) bytes += estimateVectorBytes(vector);
  return bytes;
}

export function planPhysicalFragment(
  fragment: PhysicalFragment,
  backends: readonly PhysicalExecutionBackend[],
  context: BackendPlanningContext = {},
): PlannedPhysicalFragment {
  if (backends.length === 0) {
    throw new LakeqlError(
      "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
      "No physical execution backends are installed",
      { fragmentId: fragment.id },
    );
  }
  const backendIds = new Set<string>();
  for (const backend of backends) {
    if (backend.id.trim().length === 0 || backendIds.has(backend.id)) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Physical backend ids must be unique non-empty strings",
        { backendId: backend.id, fragmentId: fragment.id },
      );
    }
    backendIds.add(backend.id);
  }
  const policy = context.policy ?? "auto";
  const contractReasons = validatePhysicalFragment(fragment);
  const assessed = backends.map((backend) => {
    const capabilities = backend.capabilities();
    const policyReasons = placementPolicyReasons(policy, capabilities.backendKind);
    const assessment = backend.assess(fragment, context);
    const budgetReasons =
      capabilities.backendKind === "accelerator"
        ? acceleratorBudgetReasons(fragment, context.budget, assessment.inputResident)
        : [];
    const costReasons = invalidCostReasons(assessment.cost);
    const reasons = uniqueRejections([
      ...contractReasons,
      ...assessment.reasons,
      ...policyReasons,
      ...budgetReasons,
      ...costReasons,
    ]);
    return {
      backend,
      backendKind: capabilities.backendKind,
      assessment: {
        ...assessment,
        supported: assessment.supported && reasons.length === 0,
        reasons,
      },
    };
  });
  const supported = assessed
    .filter((candidate) => candidate.assessment.supported)
    .sort(
      (left, right) =>
        left.assessment.cost.totalMs - right.assessment.cost.totalMs ||
        left.backend.id.localeCompare(right.backend.id),
    );
  const selected = supported[0];
  if (selected === undefined) {
    throw new LakeqlError(
      "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
      `No backend can execute physical fragment ${fragment.id}`,
      {
        fragmentId: fragment.id,
        policy,
        candidates: assessed.map((candidate) => ({
          backendId: candidate.backend.id,
          reasons: candidate.assessment.reasons,
        })),
      },
    );
  }
  return {
    fragment,
    backendId: selected.backend.id,
    candidates: assessed.map((candidate) => ({
      backendId: candidate.backend.id,
      backendKind: candidate.backendKind,
      assessment: candidate.assessment,
      selected: candidate.backend.id === selected.backend.id,
    })),
  };
}

export async function executePlannedPhysicalFragment(
  plan: PlannedPhysicalFragment,
  backends: readonly PhysicalExecutionBackend[],
  input: PhysicalFragmentInput,
  context: BackendExecutionContext = {},
  options: { replayOnCpu?: boolean } = {},
): Promise<PhysicalFragmentResult> {
  const selected = backends.find((backend) => backend.id === plan.backendId);
  if (selected === undefined) {
    throw new LakeqlError(
      "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
      `Planned backend ${plan.backendId} is not installed`,
      { fragmentId: plan.fragment.id, backendId: plan.backendId },
    );
  }
  try {
    const compiled = await selected.compile(plan.fragment);
    return await selected.execute(compiled, input, context);
  } catch (error) {
    if (
      options.replayOnCpu !== true ||
      selected.capabilities().backendKind !== "accelerator" ||
      !(error instanceof PhysicalBackendExecutionError) ||
      !error.replayable
    ) {
      throw error;
    }
    const cpu = backends.find((backend) => backend.capabilities().backendKind === "cpu");
    if (cpu === undefined) throw error;
    const compiled = await cpu.compile(plan.fragment);
    const replay = await cpu.execute(compiled, input, context);
    return { ...replay, metrics: { ...replay.metrics, replayed: true } };
  }
}

export function validatePhysicalFragment(fragment: PhysicalFragment): BackendRejection[] {
  const reasons: BackendRejection[] = [];
  if (fragment.id.trim().length === 0) {
    reasons.push({ code: "limit", message: "Physical fragment id must be non-empty" });
  }
  if (!nonNegativeInteger(fragment.input.rowCount)) {
    reasons.push({
      code: "limit",
      message: "Physical input row count must be a non-negative integer",
      details: { rowCount: fragment.input.rowCount },
    });
  }
  if (fragment.estimates.rowCount !== fragment.input.rowCount) {
    reasons.push({
      code: "limit",
      message: "Physical row estimate must equal the input row count",
      details: {
        inputRowCount: fragment.input.rowCount,
        estimatedRowCount: fragment.estimates.rowCount,
      },
    });
  }
  for (const [name, value] of Object.entries(fragment.estimates)) {
    if (value !== undefined && (!Number.isFinite(value) || value < 0)) {
      reasons.push({
        code: "limit",
        message: `Physical estimate ${name} must be finite and non-negative`,
        details: { estimate: name, value },
      });
    }
  }
  let terminal = false;
  fragment.operators.forEach((operator, operatorIndex) => {
    if (terminal) {
      reasons.push({
        code: "sequence",
        message: `Operator ${operator.kind} follows a terminal reduction`,
        operatorIndex,
      });
    }
    if (operator.kind === "reduce" || operator.kind === "grouped-reduce") terminal = true;
    if (
      operator.kind === "top-k" &&
      (!nonNegativeInteger(operator.limit) ||
        (operator.offset !== undefined && !nonNegativeInteger(operator.offset)))
    ) {
      reasons.push({
        code: "limit",
        message: "Physical top-k limit and offset must be non-negative integers",
        operatorIndex,
        details: { limit: operator.limit, offset: operator.offset },
      });
    }
    if (
      operator.kind === "grouped-reduce" &&
      operator.maxGroups !== undefined &&
      !nonNegativeInteger(operator.maxGroups)
    ) {
      reasons.push({
        code: "limit",
        message: "Physical grouped-reduce maxGroups must be a non-negative integer",
        operatorIndex,
        details: { maxGroups: operator.maxGroups },
      });
    }
  });
  const last = fragment.operators.at(-1);
  if (fragment.output.kind === "aggregate-snapshot" && last?.kind !== "reduce") {
    reasons.push({
      code: "output",
      message: "Aggregate snapshot output requires a terminal reduce operator",
    });
  }
  if (fragment.output.kind === "grouped-aggregate-snapshot" && last?.kind !== "grouped-reduce") {
    reasons.push({
      code: "output",
      message: "Grouped aggregate snapshot output requires a terminal grouped-reduce operator",
    });
  }
  if (
    (last?.kind === "reduce" && fragment.output.kind !== "aggregate-snapshot") ||
    (last?.kind === "grouped-reduce" && fragment.output.kind !== "grouped-aggregate-snapshot")
  ) {
    reasons.push({
      code: "output",
      message: `Terminal ${last.kind} does not match ${fragment.output.kind} output`,
    });
  }
  return reasons;
}

function validateCpuPlacement(fragment: PhysicalFragment): BackendRejection[] {
  const reasons: BackendRejection[] = [];
  if (fragment.input.kind !== "batch") {
    reasons.push({ code: "input-shape", message: "CPU backend requires decoded batch input" });
  }
  if (fragment.output.kind === "resident") {
    reasons.push({ code: "output", message: "CPU backend cannot produce resident output" });
  }
  return reasons;
}

function unsupportedBackend(
  backendId: string,
  fragment: PhysicalFragment,
  reasons: readonly BackendRejection[],
): LakeqlError {
  return new LakeqlError(
    "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
    `Backend ${backendId} cannot compile physical fragment ${fragment.id}`,
    { backendId, fragmentId: fragment.id, reasons },
  );
}

function validateRuntimeInput(
  expected: PhysicalInput,
  actual: Extract<PhysicalFragmentInput, { kind: "batch" }>,
): void {
  if (expected.rowCount !== actual.batch.rowCount) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Physical input row count changed after planning", {
      expected: expected.rowCount,
      actual: actual.batch.rowCount,
    });
  }
  for (const [name, expectedShape] of Object.entries(expected.columns)) {
    const vector = actual.batch.columns[name];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Physical input is missing column ${name}`, {
        column: name,
      });
    }
    const actualShape = physicalColumnShape(vector);
    if (!sameColumnShape(expectedShape, actualShape)) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        `Physical input shape changed for column ${name}`,
        {
          column: name,
          expected: expectedShape,
          actual: actualShape,
        },
      );
    }
  }
  if (expected.sourceIdentity !== undefined && expected.sourceIdentity !== actual.sourceIdentity) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Physical input source identity changed", {
      expected: expected.sourceIdentity,
      actual: actual.sourceIdentity,
    });
  }
}

function physicalColumnShape(vector: Vector): PhysicalColumnShape {
  return {
    shape: vector.type,
    nullable: "valid" in vector && vector.valid !== undefined,
    ...(vector.type === "dict"
      ? { dictionaryValueShape: physicalColumnShape(vector.dictionary) }
      : {}),
  };
}

function sameColumnShape(left: PhysicalColumnShape, right: PhysicalColumnShape): boolean {
  if (left.shape !== right.shape || left.nullable !== right.nullable) return false;
  if (left.dictionaryValueShape === undefined || right.dictionaryValueShape === undefined) {
    return left.dictionaryValueShape === right.dictionaryValueShape;
  }
  return sameColumnShape(left.dictionaryValueShape, right.dictionaryValueShape);
}

function estimateVectorBytes(vector: Vector): number {
  let bytes = "valid" in vector && vector.valid !== undefined ? vector.valid.byteLength : 0;
  switch (vector.type) {
    case "null":
      return bytes;
    case "f32":
    case "f64":
    case "i32":
    case "u32":
    case "u8":
    case "i64":
    case "timestamp":
    case "bool":
      return bytes + vector.values.byteLength;
    case "utf8":
      for (const value of vector.values) bytes += value.length * 2;
      return bytes;
    case "binary":
      for (const value of vector.values) bytes += value.byteLength;
      return bytes;
    case "dict":
      return bytes + vector.indices.byteLength + estimateVectorBytes(vector.dictionary);
    case "list":
      return bytes + vector.offsets.byteLength + estimateVectorBytes(vector.child);
    case "struct":
      for (const field of Object.values(vector.fields)) bytes += estimateVectorBytes(field);
      return bytes;
    case "map":
      return (
        bytes +
        vector.offsets.byteLength +
        estimateVectorBytes(vector.keys) +
        estimateVectorBytes(vector.values)
      );
  }
}

function intersectSelections(left: Selection, right: Selection): Selection {
  if (left.length !== right.length) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Physical selections have different lengths", {
      left: left.length,
      right: right.length,
    });
  }
  const selection = new Uint8Array(left.length);
  for (let index = 0; index < left.length; index += 1) {
    selection[index] = left[index] === 1 && right[index] === 1 ? 1 : 0;
  }
  return selection;
}

function cpuOutput(
  expected: PhysicalOutput,
  batch: Batch,
  selection: Selection | undefined,
  terminal:
    | Extract<PhysicalOutputValue, { kind: "aggregate-snapshot" | "grouped-aggregate-snapshot" }>
    | undefined,
): PhysicalOutputValue {
  if (terminal !== undefined) {
    if (terminal.kind !== expected.kind) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Physical terminal output does not match plan", {
        expected: expected.kind,
        actual: terminal.kind,
      });
    }
    return terminal;
  }
  switch (expected.kind) {
    case "batch": {
      const indices =
        selection === undefined
          ? undefined
          : Array.from(selectedRowIndices(batch.rowCount, selection));
      return {
        kind: "batch",
        batch: indices === undefined ? batch : gatherBatch(batch, indices),
      };
    }
    case "selection":
      return {
        kind: "selection",
        selection: selection ?? new Uint8Array(batch.rowCount).fill(1),
      };
    case "indices": {
      const indices = Array.from(selectedRowIndices(batch.rowCount, selection));
      if (indices.some((index) => index > 0xffff_ffff)) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Physical index output exceeds u32", {
          rowCount: batch.rowCount,
        });
      }
      return { kind: "indices", indices: Uint32Array.from(indices) };
    }
    case "aggregate-snapshot":
    case "grouped-aggregate-snapshot":
      throw new LakeqlError("LAKEQL_TYPE_ERROR", `Physical output ${expected.kind} is missing`);
    case "resident":
      throw new LakeqlError(
        "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
        "CPU backend cannot produce resident output",
      );
  }
}

function physicalOutputRows(output: PhysicalOutputValue): number | undefined {
  switch (output.kind) {
    case "batch":
      return output.batch.rowCount;
    case "selection":
      return output.selection.reduce((sum, selected) => sum + (selected === 1 ? 1 : 0), 0);
    case "indices":
      return output.indices.length;
    case "aggregate-snapshot":
      return 1;
    case "grouped-aggregate-snapshot":
      return output.snapshot.groups.length;
    case "resident":
      return undefined;
  }
}

function cpuCost(fragment: PhysicalFragment): PhysicalCostEstimate {
  const computeMs =
    (fragment.estimates.rowCount * Math.max(1, fragment.operators.length)) / 2_000_000;
  return {
    totalMs: computeMs,
    inputConversionMs: 0,
    uploadMs: 0,
    compileMs: 0,
    computeMs,
    synchronizationMs: 0,
    readbackMs: 0,
    outputConversionMs: 0,
  };
}

function placementPolicyReasons(
  policy: AcceleratorPolicy,
  backendKind: PhysicalCapabilities["backendKind"],
): BackendRejection[] {
  if (policy === "disabled" && backendKind === "accelerator") {
    return [{ code: "policy", message: "Accelerators are disabled by policy" }];
  }
  if (policy === "required" && backendKind === "cpu") {
    return [{ code: "policy", message: "An accelerator is required by policy" }];
  }
  return [];
}

function acceleratorBudgetReasons(
  fragment: PhysicalFragment,
  budget: QueryBudget | undefined,
  inputResident: boolean,
): BackendRejection[] {
  if (budget === undefined) return [];
  const checks: Array<{
    resource: string;
    limit: number | undefined;
    actual: number;
  }> = [
    {
      resource: "accelerator memory bytes",
      limit: budget.maxAcceleratorMemoryBytes,
      actual:
        fragment.estimates.inputBytes +
        fragment.estimates.outputBytes +
        (fragment.estimates.retainedStateBytes ?? 0),
    },
    {
      resource: "accelerator upload bytes",
      limit: budget.maxAcceleratorUploadBytes,
      actual: inputResident ? 0 : fragment.estimates.inputBytes,
    },
    {
      resource: "accelerator readback bytes",
      limit: budget.maxAcceleratorReadbackBytes,
      actual: fragment.estimates.outputBytes,
    },
    {
      resource: "accelerator dispatches",
      limit: budget.maxAcceleratorDispatches,
      actual: fragment.estimates.dispatchCount ?? fragment.operators.length,
    },
  ];
  return checks
    .filter((check) => check.limit !== undefined && check.actual > check.limit)
    .map((check) => ({
      code: "budget" as const,
      message: `Fragment exceeds ${check.resource} budget`,
      details: { resource: check.resource, limit: check.limit, actual: check.actual },
    }));
}

function enforceBufferedRowsBudget(
  rows: number,
  budget: QueryBudget | undefined,
  fragmentId: string,
): void {
  if (budget?.maxBufferedRows === undefined || rows <= budget.maxBufferedRows) return;
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Query exceeded buffered rows budget (${rows} > ${budget.maxBufferedRows})`,
    {
      resource: "buffered rows",
      limit: budget.maxBufferedRows,
      actual: rows,
      fragmentId,
    },
  );
}

function uniqueRejections(reasons: readonly BackendRejection[]): BackendRejection[] {
  const seen = new Set<string>();
  return reasons.filter((reason) => {
    const key = `${reason.code}\u0000${reason.operatorIndex ?? ""}\u0000${reason.message}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function nonNegativeInteger(value: number): boolean {
  return Number.isInteger(value) && value >= 0;
}

function invalidCostReasons(cost: PhysicalCostEstimate): BackendRejection[] {
  for (const [component, value] of Object.entries(cost)) {
    if (!Number.isFinite(value) || value < 0) {
      return [
        {
          code: "limit",
          message: `Physical backend cost ${component} must be finite and non-negative`,
          details: { component, value },
        },
      ];
    }
  }
  return [];
}

function errorMessage(error: unknown): string | undefined {
  if (error === undefined) return undefined;
  return error instanceof Error ? error.message : String(error);
}

function priorOutputMismatch(expected: string, actual: string): LakeqlError {
  return new LakeqlError(
    "LAKEQL_TYPE_ERROR",
    `Prior physical output ${actual} cannot seed ${expected}`,
    { expected, actual },
  );
}
