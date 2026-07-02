# Release and dev Branching Process

This document describes the recommended workflow for managing dev, release, and production branches, and how versioning is handled using GitVersion.

### Important Caveat

**Do not merge dev to main or vice-versa, this will cause overlapping versions**

## Branches

- **dev**: Integration and dev testing. All features and fixes are merged here first. Versioned as `{0.X.X}` (e.g., 0.3.1, 0.3.2, ...).
- **release/x.y**: Created from `main` for each new release cycle (e.g., `release/1.0`). Only features/fixes approved for release are merged here (can be cherry-picked from `dev`). Used for final dev, hotfixes, and release candidate builds.
- **main**: Only receives merges from `release/x.y` when a release is finalized. Version increments (e.g., 1.0.0, 1.1.0, ...).

## Workflow

1. **Feature Development**
   - Developers branch from `dev` for new features/fixes.
   - Merge feature branches into `dev` for integration and dev testing.

2. **Preparing a Release**
   - When ready to release, create a `release/x.y` branch from `main` (e.g., `release/1.0`).
   - Copy only the code from `dev` that is approved for release into `release/x.y`.
   - Perform final dev and hotfixes on `release/x.y`.

3. **Production Release**
   - Merge `release/x.y` into `main` to finalize the release.
   - Tag the release as needed.
   - Version on `main` increments (e.g., 1.0.0).

4. **Post-Release**
   - Create a new `release/x.y+1` branch from `main` for the next cycle.
   - Continue merging new features/fixes into `dev` for the next release cycle.

## Versioning Details

- The `dev` branch uses a 0 major version due to semantic versioning and PEP 440 not having any overlap.
- `dev` and `main` will co-exist as two diverging branches, `main` as what is released and `dev` for anything in progress.
- Only features that pass dev are copied into the release branch.

## Example

1. Multiple features are merged into `dev` and tested (e.g., 0.4.0, 0.4.1, ...).
2. Four features are selected for release and copied into `release/1.1`.
3. After final dev, `release/0.4` is merged into `main` (version becomes 1.1.0).
4. `main` becomes the source for the next release branch `release/1.2`.
5. New features can continue to be merged into `dev`.
6. Features that are intended for the next release, can be copied into `release/1.2` once the branch is created (post-release).

