# Releasing packages

Merging feature PRs records pending changesets; it does not bump versions or
publish packages. `.github/workflows/release.yml` publishes only on release tags.

1. Start a release branch from current main and run `pnpm exec changeset status`,
   then `pnpm exec changeset version`. Review the generated manifests and changelogs.
2. Keep packages below 1.0 until a stable API release is intentional. Changesets
   can promote peer dependents to 1.0 when `lakeql` has a minor bump, because their
   published `workspace:^` peer range moves to a new 0.x minor. For pre-1.0 packages,
   represent that compatibility boundary with a minor version bump instead. For
   this release the generated Lance and WebGPU 1.0.0 versions are therefore 0.3.0.
   Their published peer range follows lakeql 0.9.0; this does not widen support to
   untested older lakeql releases.
3. Run `pnpm install --lockfile-only`, `pnpm check`, and the site build, and merge
   the version/changelog PR after its checks pass.
4. Tag the merged release commit for each package to publish. For this release:
   `v0.9.0`, `lakeql-lance@0.3.0`, and `lakeql-webgpu@0.3.0`.

Each tag publishes only its named package. The workflow verifies the package
version matches the tag, runs the release gate, publishes with npm trusted
publishing, and creates a GitHub release. A main push rebuilds the demo site with
its updated package version, but is not proof that npm publishing occurred.
