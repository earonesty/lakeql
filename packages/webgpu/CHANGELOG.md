# lakeql-webgpu

## 0.2.0

### Minor Changes

- f8a4d39: Add accelerator-neutral physical planning, type-preserving numeric vectors, and
  the optional `lakeql-webgpu` backend. Support nullable selection, exact
  count/min/max and bounded grouped reductions, exact f32 vector scoring with
  stable bounded top-k, immutable device-resident vector candidates, accelerator
  budgets, explain statistics, device-loss handling, and bounded CPU replay.

### Patch Changes

- Depend on the public `lakeql` host package instead of the private, unpublished
  `lakeql-core` workspace so the Lance and WebGPU plugins install from npm with a
  single compatible LakeQL runtime.
- Updated dependencies [f7e1c58]
- Updated dependencies [f8a4d39]
  - lakeql@0.8.0
