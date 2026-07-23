# WebGPU-Accelerated Execution

Status: implemented foundation and continuing roadmap

Research date: 2026-07-23

## Implementation Status

The production foundation described here is implemented in `lakeql-core` and
the optional `lakeql-webgpu` package:

- accelerator-neutral physical fragments, structural capabilities, cost-based
  placement, accelerator budgets, explain statistics, and bounded CPU replay;
- physical lowering for projected scans, aggregate and grouped-aggregate work,
  top-k, and exact-vector candidate scoring;
- type-preserving `bool`, `u8`, `i32`, `u32`, `u64`, and `f32` vectors, with
  `u64` retained on CPU unless a backend advertises an exact representation;
- injected browser/Worker-compatible WebGPU runtime ownership with Dawn used
  only for development tests and benchmarks;
- nullable selection, fused count/min/max, and bounded one-key grouped
  count/min/max kernels;
- format-neutral exact `f32` dot, squared-L2, and cosine-distance scoring fused
  with stable top-k up to 32 candidates and paired `u32` row-ID words;
- a bounded reference-counted LRU cache for immutable exact-vector candidate
  blocks, tied to source identity and device generation.

Every advertised kernel has Dawn differential tests against the CPU physical
backend. The package also has a workerd packaging test that does not acquire a
device or load a native runtime.

The current explicit limits are product contracts, not silent fallbacks:

- grouped WebGPU reduction accepts one non-dictionary scalar key, at most 32
  groups, and at most 16 count/min/max aggregates;
- exact-vector top-k accepts `f32` candidate blocks and `k <= 32`;
- general resident columns, dictionary grouped reduction, sum/average, compact
  or render-resident output, product quantization, joins, windows, and sorting
  kernels remain on the roadmap;
- unsupported fragments are rejected during assessment and remain eligible for
  CPU placement unless the caller selected the `required` policy.

### Dawn development benchmark snapshot

These measurements were recorded on the development host with Dawn's Node
adapter. They validate kernel behavior and the cost shape; they are not browser
or hardware acceleration claims.

| Fragment | Rows / shape | CPU warm median | WebGPU warm median | WebGPU transfer |
| --- | ---: | ---: | ---: | ---: |
| selection + count/min/max | 1,000,000 rows | 46.7 ms | 30.9 ms | 16.0 MB upload, 35.2 KB readback |
| selection + 16-group count/min/max | 1,000,000 rows | 77.1 ms | 65.2 ms | 24.0 MB upload, 574.5 KB readback |
| resident dot + top-16 | 100,000 × 128 | 33.9 ms | 54.0 ms | 536 B query upload, 125.1 KB readback |

The resident vector lane separately paid about 31.9 ms to upload and retain
52.4 MB; the equivalent non-resident cold query took about 138.8 ms. The
resident Dawn lane remains slower than CPU on this host, which is why automatic
placement retains the end-to-end cost decision rather than assuming WebGPU is
faster.

## Decision

Add accelerator-aware physical planning to `lakeql-core` and implement WebGPU as
an optional browser execution backend in a separate `lakeql-webgpu` package.

GPU support must be expressed as capabilities of generic physical operators over
specific vector shapes. The SQL, JSON query, Parquet, Iceberg, and Lance APIs must
not contain WebGPU-specific query branches. The CPU implementation remains a
complete backend for the same physical operator contract.

The planner may place a bounded physical fragment on an accelerator only when:

- every operator and input vector shape in the fragment is supported;
- the backend preserves LakeQL's declared null, numeric, ordering, and aggregate
  semantics;
- the fragment can execute within query and device resource budgets;
- an explainable cost decision predicts an end-to-end benefit after upload,
  dispatch, synchronization, and result transfer;
- a bounded CPU replay remains available if the device is lost before the
  fragment commits its partial result.

This design makes WebGPU one backend rather than the definition of GPU support.
The same physical contract can support native WebGPU, CUDA, Metal, Vulkan, or
other accelerator implementations without changing query APIs or format
readers.

## Motivation

LakeQL already has most of the execution boundaries needed by an accelerator:

- Parquet readers produce columnar `Batch` values.
- Vector predicates produce selection masks without materializing rows.
- Projection, aggregate, group-by, join, sort, top-k, and window implementations
  already operate on vectors.
- Columnar query paths request large batches rather than issuing work for each
  small result batch.
- Parquet task manifests split input into bounded row-group work units.
- Aggregate states have deterministic snapshots that can cross a JSON work-unit
  boundary and merge in task order.
- Query budgets and statistics already describe I/O, decoded rows, buffering,
  memory, elapsed time, and concurrency.

The useful acceleration boundary is therefore not "run this JavaScript function
on a GPU." It is a fused physical fragment such as:

```text
decoded columns
    -> residual filter
    -> computed projection
    -> group key encoding
    -> partial aggregate
    -> compact partial-state readback
```

The cost of moving decoded data to a GPU is paid once for the fragment, and the
result crossing back to the CPU is much smaller than the input. Repeated
interactive queries can avoid the upload entirely when source columns remain in
a bounded device-resident cache.

NVIDIA Sirius demonstrates the same broad architecture at a different hardware
and software scale. Sirius reuses DuckDB scanning and optimization, converts
decoded data to a columnar GPU representation, caches hot tables in GPU memory,
and executes relational operators through cuDF. Its ClickBench results are hot
runs on NVIDIA server hardware, so they are evidence for the architecture rather
than a performance prediction for browser WebGPU:

- <https://developer.nvidia.com/blog/nvidia-gpu-accelerated-sirius-achieves-record-setting-clickbench-record/>
- <https://www.nvidia.com/en-us/on-demand/session/gtc26-s81870/>

## Product Boundary

`lakeql-core` owns:

- backend-neutral physical operator and fragment types;
- vector-shape and semantic capability negotiation;
- CPU physical execution;
- backend selection and cost planning;
- accelerator budgets, statistics, and explain output;
- bounded replay and deterministic partial-state merge contracts.

`lakeql-webgpu` owns:

- browser adapter and device discovery;
- WGSL module generation or selection;
- GPU buffer layouts and aligned transfers;
- pipeline, bind-group, staging-buffer, and device-resident column caches;
- WebGPU implementations of supported physical operators;
- device loss, validation error, and resource cleanup handling;
- WebGPU timing and calibration where the browser exposes reliable facilities.

Format packages own:

- preserving useful physical vector shapes while decoding;
- producing the same backend-neutral `Batch` contract;
- format-specific pruning, range reads, decompression, and metadata caches.

Applications own:

- whether accelerators are allowed, disabled, or required;
- device-memory and transfer budgets;
- whether decoded columns may remain resident between queries;
- user-facing behavior for high-power use or unavailable devices.

WebGPU is normally exposed in secure browser Window and Worker contexts.
Cloudflare also exposes an experimental compute-only WebGPU subset inside
Durable Objects during local Wrangler development. As of the research date,
Durable Objects that rely on WebGPU cannot be deployed to Cloudflare, and
ordinary Workers do not expose it. Production Cloudflare queries therefore
remain CPU-backed or delegate acceleration to the requesting browser.

Cloudflare runtime reference:
<https://developers.cloudflare.com/durable-objects/api/webgpu/>

`lakeql-webgpu` must accept a runtime-supplied GPU/adapter provider rather than
hard-code `window`, `document`, canvas, or browser lifecycle assumptions. Its
browser entry point obtains `navigator.gpu`; a local Durable Object entry point
may obtain the runtime's `navigator.gpu`. Node, Deno, Bun, and production
workerd paths continue using the CPU backend unless the caller installs a
compatible accelerator provider.

Cloudflare's local Durable Object API validates that the backend contract is not
intrinsically browser-bound. A deployable Durable Object implementation is a
possible future adapter, not a current package or deployment commitment. Its
runtime, residency, sharding, and cost design should be made from the production
Cloudflare contract when that work is undertaken.

## Physical Execution Model

### Operator IR

Introduce a compact physical operator IR describing columnar data flow, not SQL
syntax:

```ts
type PhysicalOperator =
  | PhysicalSelect
  | PhysicalProject
  | PhysicalReduce
  | PhysicalGroupedReduce
  | PhysicalHashJoin
  | PhysicalOrder
  | PhysicalTopK
  | PhysicalWindow
  | PhysicalVectorDistance;

interface PhysicalFragment {
  input: PhysicalInput;
  operators: PhysicalOperator[];
  output: PhysicalOutput;
  estimates: PhysicalEstimates;
}
```

Operators declare their required inputs, output shapes, retained state,
order-sensitivity, merge behavior, and resource estimates. `PhysicalInput` can
refer to decoded batches, a work-unit row range, or a backend-resident dataset
slice. `PhysicalOutput` distinguishes a materialized batch, selection, index
vector, aggregate snapshot, grouped aggregate snapshot, top-k candidates, or a
backend-resident renderable buffer.

The existing CPU vector functions become the CPU implementation of these
operators. Query behavior must remain unchanged when every fragment is assigned
to the CPU backend.

### Backend Contract

The backend contract must negotiate complete fragments rather than advertise a
single boolean such as `supportsGpu`:

```ts
interface PhysicalExecutionBackend {
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
```

`PhysicalCapabilities` describes:

- operator kinds and legal fused sequences;
- accepted input, key, selection, and output vector shapes;
- null representation and dictionary support;
- numeric overflow, rounding, NaN, signed-zero, and timestamp behavior;
- stable ordering and tie-breaking guarantees;
- aggregate state format and merge compatibility;
- maximum input, retained-state, group, join-build, and output shapes;
- device limits that affect dispatch or storage bindings;
- whether results can remain backend-resident for another fragment or renderer.

Capabilities are structural and semantic. A backend cannot claim support for
`reduce(sum)` without identifying the accepted input shape and accumulator
semantics.

### Fragment Planning and Fusion

The planner builds maximal legal fragments, then compares complete placement
alternatives. It must avoid alternating between CPU and GPU for adjacent
operators when transfer boundaries erase the benefit.

Useful fused sequences include:

```text
select -> project -> reduce
select -> dictionary-key group -> grouped reduce
vector distance -> top-k
select -> compact/gather -> backend-resident render output
```

Fragment boundaries are also recovery boundaries. A fragment consumes immutable
decoded input or a replayable work unit and publishes output only after successful
completion. Device loss before publication discards the incomplete GPU state and
replays that fragment on the CPU. It does not restart completed work units or
change fan-in order.

### Cost Model

Backend selection must compare end-to-end cost:

```text
decode
+ input conversion
+ upload
+ pipeline compilation or cache lookup
+ dispatch and compute
+ synchronization
+ readback
+ downstream conversion
```

Inputs to the estimate include:

- decoded row count and byte width;
- projected column shapes and null density;
- predicate selectivity estimates when available;
- expected group or candidate cardinality;
- whether source buffers and compiled pipelines are resident;
- expected output size;
- device limits and calibration class;
- downstream placement, including direct rendering.

The default decision is conservative when estimates or device measurements are
missing. Applications can select an execution policy:

```ts
type AcceleratorPolicy = "auto" | "disabled" | "required";
```

`required` is an explicit operational/testing policy: planning fails with a typed
error if the requested query cannot be placed on an installed accelerator. It
must not weaken semantic checks.

`explain()` reports every candidate fragment, selected backend, expected transfer
bytes, residency status, rejected capabilities, cost inputs, and the reason for
the final placement.

## Vector Shapes and SQL Semantics

LakeQL currently normalizes JavaScript numbers into `Float64Array` and integer or
timestamp values into `BigInt64Array`. WGSL provides `f32`, optionally `f16`,
`i32`, and `u32`; ordinary 64-bit integer and `f64` arithmetic are not portable
WebGPU capabilities.

Silently converting `f64` to `f32` is not acceptable. Add physical vector shapes
that preserve source types where doing so improves execution without changing
logical values:

```text
f32
i32
u32
bool
dictionary indices (u32)
validity bitmap or validity bytes
i64/u64/timestamp word pairs where an exact kernel supports them
```

Parquet FLOAT, INT32, unsigned logical integers, dictionary indices, Lance
embeddings, and similar data can then reach a backend without first widening to
`f64` or `i64`. Logical materialization still returns the public scalar values
expected by LakeQL.

The WebGPU capability matrix begins with shapes that map directly and expands
only through conformance-proven semantics:

| Vector shape | Candidate operations | Required semantic proof |
| --- | --- | --- |
| `bool` | predicates, selection, count | null propagation and three-valued predicate behavior |
| `i32` / `u32` | comparison, arithmetic, group keys, min/max, bounded partial reduction | overflow and accumulator behavior |
| `f32` | comparison, projection, distance, min/max, declared reductions | NaN, signed zero, rounding, and reduction-order behavior |
| dictionary `u32` indices | equality, membership, group keys, histograms | dictionary identity, nulls, and cross-batch dictionary handling |
| paired 32-bit words | exact equality and ordered comparison for selected `i64`, `u64`, and timestamps | signed ordering, units, nulls, and boundary values |
| `f64` | CPU unless a backend declares a conforming implementation | full LakeQL numeric semantics |
| raw UTF-8 / binary / nested | CPU until a backend declares a conforming layout and operators | encoding, collation, nulls, bounds, and output representation |

Partial counters can be segmented so no GPU-local counter overflows, then merged
through the existing CPU aggregate snapshot path. Other reductions must use an
accumulator representation whose numerical contract matches the query. A fast
but different answer is not an accelerated implementation of the same query.

## WebGPU Data and Resource Model

### Buffer Layout

Each physical vector maps to one or more aligned storage buffers:

- values;
- validity;
- offsets for variable-width or nested shapes when supported;
- dictionary indices and dictionary values;
- selections or compacted indices;
- operator state and output counters.

Layouts must account for the adapter's actual storage binding, buffer size,
binding count, workgroup storage, workgroup dimension, and alignment limits.
Large fragments are tiled without changing operator semantics or work-unit
boundaries.

### Residency

A device-resident column cache is the central mechanism for repeated interactive
queries. Cache identity includes:

```text
device generation
source snapshot or object path
etag/size identity
row range
column identity
physical vector shape
null and dictionary representation
```

The cache is device-local and runtime-local; GPU handles never enter bookmarks,
task manifests, or JSON partials. Entries use explicit reference tracking and
bounded least-recently-used eviction. A source identity change, device loss, or
representation change invalidates the corresponding entries.

Decoded-column cache entries and device cache entries may share immutable source
identity metadata, but neither cache may assume the other contains an entry.

### Budgets

Extend `QueryBudget` with backend-neutral accelerator controls:

```ts
interface QueryBudget {
  maxAcceleratorMemoryBytes?: number;
  maxAcceleratorUploadBytes?: number;
  maxAcceleratorReadbackBytes?: number;
  maxAcceleratorDispatches?: number;
}
```

Query budget accounting covers live query input, retained state, staging
buffers, and outputs. Resident entries are admitted against the backend's
separate caller-provided `maxResidentBytes` capacity; a query that leases an
existing entry is charged only for its incremental query buffers. The backend
must tile, evict, choose the CPU placement, or fail with a typed budget error;
it must not rely on an out-of-memory device failure as normal flow control.

### Lifecycle

The WebGPU backend must:

- initialize asynchronously without blocking CPU planning;
- run in a dedicated browser worker when the application supplies one, or in a
  compute-only runtime when an explicit adapter provider is installed;
- cache compilation results by fragment and device capability fingerprint;
- surface shader compilation and validation errors with fragment context;
- stop dispatching promptly after cancellation;
- destroy unreferenced buffers deterministically;
- treat device loss as a new device generation and invalidate resident state;
- replay only bounded unpublished fragments on CPU;
- avoid background-tab performance promises because browsers may throttle work.

### Development Adapters

Use the Dawn `webgpu` Node package as the primary kernel-development adapter. It
implements compute WebGPU directly in Node and can be injected through the same
GPU-provider boundary as a browser's `navigator.gpu`. Keep it a development/test
dependency; it must not enter `lakeql-webgpu` runtime dependencies or application
bundles.

Node adapter reference: <https://github.com/dawn-gpu/node-webgpu>

The injected runtime includes `gpu` plus the WebGPU runtime constants used by the
backend, such as buffer usage, map mode, shader stage, and color-write masks. The
library must not install Node/Dawn globals by mutating `globalThis`; the browser
adapter and Node adapter construct the same explicit runtime-provider value.

The test layers have distinct purposes:

```text
Vitest + Dawn Node adapter
    -> rapid WGSL compile, dispatch, readback, limits, and CPU differential tests

headless and interactive browsers
    -> web-platform integration, Worker behavior, device diversity, and timing

local Wrangler Durable Object
    -> workerd API-subset and packaging conformance
```

Node/Dawn tests explicitly destroy devices and buffers and release the object
returned by `create()`; retaining it can keep the Node event loop alive. A
software adapter is sufficient for correctness CI. Hardware performance claims
still require named browser, adapter, driver, and machine measurements because a
native Dawn timing does not include browser scheduling or security overhead.

## Operator Delivery Order

The work proceeds in architectural layers. Each layer leaves a complete,
generally useful contract rather than adding query-specific dispatch.

### 1. Physical Operator Foundation — implemented

- Define the physical operator, fragment, capability, assessment, and backend
  contracts in `lakeql-core`.
- Implement the current vector engine as `CpuPhysicalBackend`.
- Lower existing aggregate, grouped aggregate, project, top-k, and supported
  predicate paths to physical fragments.
- Preserve work-unit snapshots, task-order fan-in, budgets, and output behavior.
- Add physical fragments and backend decisions to `explain()`.
- Prove CPU parity through existing conformance, reference, Worker, and browser
  suites before enabling another backend.

### 2. Type-Preserving Columnar Data — implemented for portable scalar shapes

- Add `f32`, `i32`, `u32`, and packed `u8` physical vector shapes.
- Preserve these shapes from Parquet and Lance decoders where the source type is
  known.
- Define shape conversion operators explicitly; never hide narrowing in a
  backend adapter.
- Extend vector projection, gathering, concatenation, selection, and
  materialization across the new shapes.
- Differentially test boundary values, nulls, dictionaries, and mixed batches.
- Represent 64-bit row identifiers as paired `u32` words at accelerator
  boundaries. The representation and comparison are exact; a backend may not
  narrow row identifiers to JavaScript numbers or single `u32` values.

### 3. WebGPU Runtime and Fused Relational Kernels — partially implemented

- Add the `lakeql-webgpu` package with adapter discovery, resource ownership,
  compilation caches, error scopes, timing hooks, and device-loss handling.
- Implement conformance-proven selection over `bool`, `i32`, `u32`, and `f32`.
- Implement fused selection plus count/min/max and supported sum/average partials.
- Implement dictionary-index equality, membership, histograms, and grouped
  partial aggregates.
- Return existing aggregate snapshot formats so CPU and WebGPU work units merge
  identically.
- Tile inputs according to negotiated device limits and accelerator budgets.

### 4. Placement, Residency, and Interactive Execution — partially implemented

- Implement end-to-end CPU versus WebGPU cost assessment.
- Add device-resident source-column and compiled-fragment caches.
- Reuse resident columns across parameter changes without binding query results
  to stale object identities.
- Support parameter buffers so repeated predicates do not require recompilation.
- Add backend-resident output suitable for direct WebGPU rendering.
- Report cold, warm, transfer, compute, readback, cache, and fallback metrics.

### 5. Vector Distance and Top-K — exact-vector foundation implemented

- Express exact-vector and quantized-code scoring as generic physical operators,
  not Lance-specific calls:

  ```text
  PhysicalExactVectorScore(f32 vectors, query, metric)
  PhysicalProductQuantizedScore(u8 codes, codebook, query, quantizer)
  PhysicalBoundedTopK(scores, row-id words, k)
  ```

- Define a format-neutral `PhysicalVectorCandidateBlock` as structure-of-arrays
  storage. It carries candidate count, encoding, row-ID low/high words, and
  either contiguous `f32` vectors or packed `u8` codes. PQ configuration and
  codebooks are immutable referenced inputs rather than repeated per row.
- Refactor Lance and Parquet readers to yield bounded candidate blocks without
  constructing a `Map`, row objects, boxed vectors, or scored candidate objects.
  The existing CPU implementation consumes the same blocks before WebGPU is
  enabled.
- Decode Lance V3 IVF-PQ centroids, partition metadata, quantizer metadata,
  codebooks, row IDs, and `__pq_code` buffers. IVF-FLAT remains the exact
  conformance/reference encoding, not the target million-vector serving layout.
- Build one query-dependent asymmetric-distance lookup table from the PQ
  codebook, then score packed codes without reconstructing full vectors.
  Quantizer transforms, residual rules, metric definitions, and codebook layout
  follow the stored Lance metadata and official implementation semantics.
- Fuse scoring with tiled bounded top-k. Each tile returns only retained
  `(distance, row-id-low, row-id-high)` tuples; a deterministic bounded merge
  combines tiles. Full score arrays never cross back to the CPU.
- Define CPU and WebGPU accumulation, NaN, zero-vector, ordering, and tie-breaking
  semantics explicitly. Differential tests include adversarial near-ties and
  row IDs that require both 32-bit words.
- Accept an optional aligned selection mask so scalar predicates are applied
  before candidate scoring. The planner must not implement filtered search by
  taking an unfiltered top-k and discarding rows afterward.
- Keep snapshot resolution, IVF partition planning, object-store range
  selection, candidate block decoding, exact-vector retrieval, and final row
  materialization backend-neutral.
- Permit both streamed IVF-PQ and resident PQ-code placement. Cold or
  memory-constrained queries range-read selected partitions; an interactive
  session may retain all compact codes and row IDs on the device and scan the
  million-item corpus repeatedly. Placement is selected from transfer, memory,
  residency, filter, and query-frequency costs.
- Treat exact refinement as another candidate-block score. The cost planner may
  keep a small rerank on CPU or place a larger rerank on WebGPU; the public result
  labels whether scores are approximate or refined.

### 6. Broader Operators

Add operators according to measured end-to-end payoff and semantic coverage:

- compact/gather and render interop;
- multi-column and higher-cardinality grouped reduction;
- radix sort and top-k;
- bounded hash joins with explicit build/probe shape capabilities;
- selected window partitions and segmented scans;
- paired-word timestamp and integer operations;
- string or nested kernels only after layouts and semantics are independently
  useful across query classes.

## Benchmark and Acceptance Plan

Kernel timing alone cannot justify planner placement. Browser benchmarks report:

```text
planning
object-store fetch
Parquet/Lance decode
CPU-to-GPU conversion
upload
compile
dispatch/compute
readback
fan-in/materialization
end-to-end elapsed time
peak CPU and accelerator memory
```

Every benchmark has:

- CPU columnar execution as the primary baseline;
- cold execution with no compiled pipeline or resident data;
- warm pipeline execution without resident source columns;
- warm resident execution;
- increasing row and byte sizes to locate crossover behavior;
- selectivity and group-cardinality sweeps;
- integrated and discrete GPU classes across supported browsers;
- local Wrangler Durable Object execution with the `experimental` and `webgpu`
  compatibility flags;
- correctness validation against the CPU result;
- device-loss and resource-budget variants.

The Node/Dawn lane is a kernel benchmark and correctness tool, not an end-to-end
browser benchmark. Published acceleration claims come from the browser lanes.

The existing 10M-row work-unit fixture supplies aggregate and grouped-aggregate
lanes. Add browser execution rather than changing its portable work-unit
boundary. Add a Lance lane covering centroid scoring, candidate scoring, and
top-k. Do not claim acceleration until end-to-end browser results show a
repeatable win on named hardware and browser versions.

Release acceptance requires:

- differential conformance for every advertised operator/shape capability;
- no silent precision loss or unsupported semantic fallback inside a fragment;
- deterministic results across tiling and batch boundaries;
- stable CPU replay after device loss;
- hard enforcement of upload, readback, dispatch, and memory budgets;
- explain output that accounts for each placement;
- no WebGPU code or browser globals in `lakeql-core`;
- bundle measurement proving non-WebGPU applications do not include the backend.

## Demonstration Plan

The demonstration suite must show query acceleration, not merely GPU graphics.
Each experience exposes a restrained telemetry panel with planned files and row
groups, decoded and GPU-evaluated rows, transfer bytes, residency hits, fragment
placement, query time, and frame time. A CPU comparison runs the same physical
fragment and verifies the result.

### Gaia: Query the Milky Way

Build a cinematic 3D star map from a partitioned Gaia catalog in Parquet.
Object-store and row-group pruning select spatial partitions; WebGPU evaluates
temperature, luminosity, distance, velocity, and selection predicates; selected
buffers feed the renderer without CPU materialization.

The defining interaction links a user-drawn 3D volume to a live
color-magnitude diagram. Brushing either view filters the other. This
demonstrates fused selection, projection, histogram aggregation, device
residency, and direct render output.

### City Pulse: One Billion Trips

Render taxi, bicycle, or transit movement as animated nighttime flows. Time
scrubbing and map-drawn regions recompute origin/destination cells, trip counts,
duration and revenue summaries, and hourly distributions.

A representative physical fragment is:

```text
time and origin selection
    -> destination dictionary/H3 key
    -> count and average partials
    -> compact flow and histogram buffers
```

The UI includes a visible CPU-versus-WebGPU race over the same fragment, followed
by a correctness match. This demonstrates dictionary grouping, parameterized hot
queries, aggregate partials, and the interaction between lake pruning and GPU
execution.

### The Semantic Museum

Arrange millions of artwork or historical-image embeddings as a navigable
constellation backed by a Lance V3 IVF-PQ serving index regenerated from
canonical embedding Parquet. Text, image, and blended-concept queries retrieve
nearby works while scalar filters constrain period, medium, country, and
collection.

The cold accelerated path performs IVF candidate planning on the CPU, range-reads
packed PQ codes for selected partitions, evaluates asymmetric distance and top-k
on WebGPU, optionally refines retained candidates against exact vectors, then
materializes only selected records. The warm path may keep all compact codes and
row identifiers device-resident and scan the entire million-item collection on
each query-vector change. Moving through the constellation therefore demonstrates
both object-store-aware ANN and genuinely continuous resident GPU search.

Scalar predicates produce a selection mask aligned with the serving index before
distance evaluation. Result semantics do not depend on post-filtering an
unfiltered nearest-neighbor result.

#### Embedding Model Decision

Use MobileCLIP2-S0 as the single image/text embedding model for the demonstration.
Its browser-oriented size and latency are part of the product requirement:

- run the image tower offline once while building the collection;
- export and quantize only the text tower for browser inference;
- cache the text tower through the site's normal durable asset cache;
- normalize its 512-dimensional image and text outputs before indexing or search;
- record the exact checkpoint, tokenizer, preprocessing, normalization, and export
  hashes in the dataset manifest;
- keep image embeddings in Lance and compact product-quantization codes in the
  browser-searchable index;
- use LakeQL scalar predicates over museum, period, medium, creator, place, and
  rights metadata instead of adding a second metadata embedding model.

Do not run a general model bakeoff. Validate MobileCLIP2-S0 on a small,
representative set of evocative and factual museum queries, then spend evaluation
effort on the complete browser experience: model download and cache behavior,
query latency, vector-search correctness, result quality, and index transfer
size. Confirm that the Apple model terms permit the intended public deployment
before redistributing the exported text tower.

The dataset manifest carries a model identity because embeddings and queries must
always use the same aligned towers. That is a data-compatibility contract, not a
commitment to shipping multiple model implementations.

#### Semantic Museum Data Releases

Use the Smithsonian Open Access collection as the backbone of the first complete
museum release. Its public S3 bucket is updated weekly and exposes sharded
newline-JSON metadata without credentials. Records already carry stable record
IDs, content hashes, update timestamps, collection metadata, rights, and image
delivery URLs. The image delivery service can return a bounded derivative, so
embedding workers must request a MobileCLIP-sized image rather than download
archival TIFFs.

The durable archive is the derived data product, not a mirror of source
originals. Store source and display URLs with every record. Keep originals at the
institution, and copy an image only if the application later needs a controlled
display derivative or a source disappears. This preserves provenance and makes a
million-image build practical.

Publish immutable, content-addressed releases under an R2 prefix:

```text
semantic-museum/
  releases/<release-id>/
    manifest.json
    metadata/source=<source>/unit=<unit>/part-*.parquet
    embeddings/model=<model-id>/bucket=<hash-bucket>/part-*.parquet
    indexes/model=<model-id>/museum.lance/...
    state/source=<source>/part-*.parquet
    failures/stage=<stage>/part-*.parquet
  current.json
```

`current.json` is replaced only after all release validation succeeds. Consumers
therefore see either the prior complete release or the new complete release,
never a partially compacted dataset.

The canonical metadata schema includes:

- stable item and media IDs;
- source, collection/unit, record URL, image URL, and display URL;
- title, description, creator, date/range, medium, object type, subjects, and
  places;
- record rights, media rights, attribution, and source update timestamp;
- image identity, dimensions, and source hash where available;
- embedding model identity, preprocessing identity, and embedding status.

Store embeddings separately as `item_id`, `media_id`, `model_id`, and a
fixed-size 512-element float vector. Metadata-only changes then do not rewrite
embedding objects, and model changes do not rewrite metadata. Partition by
source/unit and a stable ID hash bucket, not high-cardinality fields such as
artist or year. Compact small ingestion shards into roughly 128–512 MiB Parquet
objects with useful row groups; the exact target is chosen from measured browser
range-read behavior. Avoid an R2 object per record.

The Lance dataset is a reproducible serving derivative of the embedding Parquet,
not the only copy of the vectors. It contains normalized exact vectors, scalar
filter columns, and an IVF-PQ index. The manifest records the Parquet inputs,
Lance version, index training sample hash, metric, partitions, subvector count,
and every build parameter.

#### Affordable Embedding Pipeline

Build work is a manifest of immutable hash buckets. A worker can be interrupted
after any completed bucket without invalidating the release:

1. Stream Smithsonian metadata shards, select image media with acceptable rights,
   normalize records, and write canonical metadata and build-state Parquet.
2. Compare stable media identity and model identity with the prior build state.
   Queue only new, changed, failed, or newly eligible images.
3. On a GPU worker, asynchronously fetch bounded 256-pixel derivatives, decode
   and batch them, run the MobileCLIP2-S0 image tower, L2-normalize the 512
   floats, and upload completed embedding shards directly to R2.
4. Record download, decode, inference, validation, and rights failures as rows
   with retry classification. Missing images are data, not a build-wide error.
5. Compact completed shards, build the Lance IVF-PQ derivative, run deterministic
   quality and integrity checks, and atomically publish `current.json`.

The executable release harness lives in
[`tools/semantic-museum`](../tools/semantic-museum/README.md). It resolves and
freezes the Smithsonian source indexes, performs order-independent stable
bottom-k selection across the complete corpus, plans immutable hash buckets,
enforces global and per-bucket byte budgets, and writes typed receipts and
failures. Workers use the exact MobileCLIP2-S0 checkpoint and preprocessing
identity from the plan. Completed bucket objects are checksum-verified in R2
before their receipt is uploaded as the commit marker; replacement workers
restore and validate those objects before resuming.

Release finalization writes canonical metadata and fixed-size-vector Parquet,
accounts for every planned record, and produces a content manifest. Publication
verifies every local and remote object before replacing `current.json`. The same
commands run bounded single-bucket laptop fixtures and distributed million-image
builds; fixture selection uses an explicit prefix policy while full releases use
bottom-k selection to avoid collection-order bias.

RunPod and Vast storage must not become the archive. Workers use ephemeral local
space as a bounded decode/cache area and stream completed shards to R2. This
avoids paying GPU-provider persistent-disk rates between runs and makes
interruptible Vast instances safe. Choose an instance by measured dollars per
successfully embedded image, including download and decode, rather than by GPU
name.

Before the million-image build, run the same production pipeline over a
representative 10,000-image manifest on two inexpensive candidates. Measure
successful images per second, retry rate, bytes fetched, GPU utilization, and
output bytes. At an hourly price `p` and sustained end-to-end throughput `r`,
the million-image compute cost is:

```text
cost = 1,000,000 / r / 3,600 * p
```

Using current RunPod Community Cloud prices as examples:

| End-to-end rate | RTX A5000 at $0.27/hour | A40 at $0.44/hour |
| ---: | ---: | ---: |
| 10 images/second | $7.50 | $12.22 |
| 25 images/second | $3.00 | $4.89 |
| 50 images/second | $1.50 | $2.44 |
| 100 images/second | $0.75 | $1.22 |

These are scenario calculations, not a throughput claim. Network fetch and image
decode may dominate MobileCLIP inference. Use bounded concurrent fetching,
backpressure, and a decoded-batch queue so that measurement covers the complete
pipeline. Prefer a cheaper interruptible Vast listing when its measured
dollars-per-image wins and checkpoint recovery has been exercised.

For one million items, normalized float32 embeddings occupy 2.048 GB before
container overhead; 64-byte PQ codes occupy 64 MB. Compressed metadata,
embedding Parquet, a Lance serving copy, manifests, and the browser text model
should ordinarily fit in roughly 5–10 GB. Under Cloudflare R2 Standard pricing,
the first 10 GB-month, one million Class A operations, and ten million Class B
operations are free. Beyond that, storage is $0.015/GB-month, Class A operations
are $4.50/million, Class B operations are $0.36/million, and egress is free.
Large immutable objects keep build operations far below those request
allowances. At 20 GB total, storage beyond the free allowance is about
$0.15/month.

Do not put a million thumbnails into R2 merely to complete the archive. Source
display URLs are enough for the search experience and avoid another million
writes. If reliability measurements later justify owned thumbnails, create a
separate versioned derivative tier, retain source provenance, and budget both
object operations and provider bandwidth. A 25 KB average WebP would add about
25 GB per million images, which is still inexpensive to store but changes the
build and request profile.

Refreshes are incremental. A record with changed descriptive metadata but the
same media identity reuses its embedding. New or changed media produces a new
embedding row; removed or newly restricted media produces a tombstone. Compact
and retrain the serving index when accumulated deltas cross a declared size or
quality threshold. Every refresh remains reproducible from its source manifest
and prior build state.

#### GPU Lease and Automatic Destruction

No GPU build may depend on the launching laptop remaining awake. Launching a
RunPod or Vast worker requires an explicit maximum runtime and maximum estimated
cost. The launcher refuses to create compute without both limits.

Treat provider resources as expiring leases with three independent destruction
paths:

1. Set the provider's hard expiration during creation where available. RunPod
   Pods use `--terminate-after`, not `--stop-after`, because stopped Pods can
   continue billing persistent storage.
2. A lifecycle supervisor runs as PID 1 and owns the lease. It starts the
   embedding worker as a child process. Any child exit—success, typed failure,
   uncaught exception, assertion, cancellation, or signal—causes the supervisor
   to upload a terminal receipt if possible and permanently destroy its own
   resource. RunPod exposes `RUNPOD_POD_ID` and a pod-scoped `RUNPOD_API_KEY`;
   Vast exposes `CONTAINER_ID` and `CONTAINER_API_KEY`. Vast workers call the
   destroy-instance API rather than merely stopping.
3. An independent scheduled Cloudflare Worker scans managed leases and provider
   resources. It destroys resources past their hard deadline, resources that
   have stopped making progress for the declared stall interval, and tagged
   orphan resources with no valid lease. This reaper runs independently of the
   laptop and GPU container.

The embedding worker never owns lifecycle cleanup and never runs as PID 1. The
supervisor forwards termination signals, gives the child a bounded checkpoint
window, reaps all child processes, captures the exit status, and invokes the
provider adapter from a `finally` path. It has its own short destruction timeout
and does not wait indefinitely for logging, upload, or the provider response.
Once a provider accepts deletion, the connection may disappear before a response
is observed; the external reaper verifies deletion independently.

No in-container cleanup can run after `SIGKILL`, a kernel OOM, host power loss,
or loss of the container runtime. Native expiration and the external reaper are
therefore part of the guarantee rather than optional monitoring.

The launch sequence closes partial-failure gaps:

```text
write pending lease with provider, release, shard range, deadline, and cost cap
    -> create tagged provider resource with its native hard expiration
    -> record provider resource ID in the lease
    -> worker heartbeats progress and completed shard receipts
    -> worker uploads and validates all durable output
    -> mark lease complete
    -> request permanent provider destruction
    -> reaper verifies the resource no longer exists
```

Provider resource names and tags contain a generated lease ID, never credentials.
The reaper also lists provider resources by the LakeQL managed tag so it can find
an instance created just before the launcher lost the API response. Creation,
completion, destruction requests, and reaping are idempotent.

The hard deadline is authoritative even when work remains. A worker checkpoints
the current completed shard and exits before the deadline; a later lease resumes
the missing immutable shards. Heartbeats improve cleanup latency but cannot
extend the hard deadline. An extension is a new authenticated lease decision
with a newly calculated cost cap.

R2 credentials granted to workers are limited to the release staging prefix.
Provider lifecycle credentials use the provider-injected instance scope wherever
available. The independent reaper keeps account-level provider credentials in
Cloudflare secrets, not in manifests, container images, command lines, logs, or
the launching laptop's continuing process.

Use ephemeral provider disks and permanently destroy completed resources.
Durable outputs and logs required for diagnosis must reach R2 before the destroy
request. The release status command reports active leases, their accumulated
estimated cost, last progress, deadline, and confirmed provider deletion so
cost safety is observable rather than implicit.

RunPod Serverless and Vast Serverless use the same one-shard worker contract, but
their managed runtime owns worker teardown. A serverless adapter submits bounded
jobs, configures execution and idle timeouts, and verifies terminal job state;
it does not call the Pod/instance self-destruction adapter. Dedicated Pods and
instances remain useful when measured model-loading and shard duration make
serverless cold starts more expensive. Provider choice must not change shard
inputs, output receipts, idempotency, or release semantics.

For the contiguous million-image build, prefer a self-destructing dedicated or
interruptible instance. Current RunPod rates make its Flex Serverless pools
roughly 1.6–2.8 times the hourly Pod price for comparable GPU classes before
counting billed startup, model loading, and idle timeout:

| GPU pool | Community Pod | Flex Serverless equivalent | Serverless/Pod |
| --- | ---: | ---: | ---: |
| A5000-class 24 GB | $0.27/hour | $0.684/hour | 2.53x |
| A40-class 48 GB | $0.44/hour | $1.224/hour | 2.78x |
| RTX 4090-class 24 GB | $0.69/hour | $1.116/hour | 1.62x |

RunPod Flex becomes cheaper only when the fraction of wall-clock time that would
otherwise keep a Pod allocated falls below approximately 40%, 36%, or 62% for
those respective pools. The bulk build should keep a GPU continuously fed, so it
does not receive the scale-to-zero advantage.

Vast documents no separate Serverless price tier: workers use underlying
marketplace compute, storage, and bandwidth rates. It can therefore be a good fit
for irregular incremental refresh shards. Configure true scale-to-zero
explicitly: zero active minimum, zero cold-worker minimum, a finite inactivity
timeout, and a bounded maximum worker count. Loading time is billable, and
inactive workers continue billing storage and bandwidth. Destroy the endpoint
after a bounded release run unless it is intentionally retained for ongoing
refresh traffic.

Source and pricing references:

- <https://registry.opendata.aws/smithsonian-open-access/>
- <https://iiif.si.edu/>
- <https://developers.cloudflare.com/r2/pricing/>
- <https://developers.cloudflare.com/r2/platform/limits/>
- <https://www.runpod.io/pricing>
- <https://docs.runpod.io/serverless/pricing>
- <https://docs.runpod.io/runpodctl/reference/runpodctl-pod>
- <https://docs.runpod.io/pods/templates/environment-variables>
- <https://vast.ai/pricing>
- <https://docs.vast.ai/guides/serverless/pricing>
- <https://docs.vast.ai/guides/instances/docker-environment>
- <https://docs.vast.ai/api-reference/instances/destroy-instance>

The intended showcase order is:

1. Gaia as the visual launch experience and direct-render proof.
2. City Pulse as the clearest relational operator and CPU comparison.
3. The Semantic Museum as the strongest application-oriented vector-search
   experience.

## Risks

### Transfer-Dominated Queries

Cold object-store queries may spend most of their time fetching and decoding.
The cost planner must keep these fragments on CPU unless fusion, input size, or
small output makes acceleration worthwhile. Residency metrics must distinguish a
warm-query result from a cold-query result.

### Portable Type Limitations

WGSL's portable numeric types cover less than LakeQL's logical type system.
Physical shape preservation and semantic capability negotiation are prerequisites,
not optional optimizations.

### Device Diversity

Integrated GPUs, discrete GPUs, software adapters, browser limits, and power
policies differ substantially. Planning must use negotiated limits and
conservative cost classes, expose its reasoning, and retain a complete CPU path.

### Resource Pressure

Browsers can reject allocations, lose devices, or protect responsiveness with
watchdogs. Bounded tiling, explicit accelerator budgets, deterministic cleanup,
and work-unit replay are required before enabling automatic placement.

### Benchmark Presentation

Server-class Sirius results, kernel-only timings, warm caches, and direct-render
paths answer different questions. Demo telemetry and published benchmarks must
label hardware, browser, cold/warm state, residency, transferred bytes, and
whether output was read back or rendered directly.

## Open Design Questions

- Which Parquet physical types can remain narrow without increasing decoder
  complexity or bundle size for CPU-only consumers?
- Which additional aggregate snapshot representations permit exact GPU-local
  accumulation and deterministic CPU fan-in beyond count/min/max?
- Can renderer-owned buffers satisfy a stable interoperability contract without
  coupling `lakeql-core` to canvas or scene libraries?
- Which browser automation environments provide trustworthy real-GPU
  conformance and timing lanes in continuous integration?
