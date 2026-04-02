---
date: 2026-04-02
title: "Version as partition key with manifest-based resolution"
status: decided
chosen-approach: "Version partition + manifest file_path resolution"
tags: [arrow, partitioning, versioning, manifest, pipdata, piptm]
---

# Version as Partition Key with Manifest-Based Resolution

## Context

The `prepare_for_arrow()` pipeline in `{pipdata}` now injects a `version`
column (e.g. `"v01_v04"`, derived from `metadata$vermast` and
`metadata$veralt`) into every survey data.table. Currently this column is
stored inside the Parquet file but is **not** used for partitioning.

The team wants multiple survey versions to coexist in the Arrow repository
simultaneously, so that different PIP releases can reference different
versions of the same country/year/welfare survey. The manifest JSON is the
mechanism that tells `{piptm}` which version to load.

## Requirements

### Functional

1. **Version as partition key**: The `version` column must become part of
   the Hive-style partition directory structure:
   ```
   country=BOL/year=2020/welfare=INC/version=v01_v04/BOL_2020_EH_INC_ALL-0.parquet
   ```

2. **Atomic value**: `"v01_v04"` is a single, indivisible string — it must
   not be split into `vermast` and `veralt` at the partition level.

3. **Multiple versions coexist**: Old versions remain on disk when a new
   version is written. The directory tree may contain:
   ```
   version=v01_v04/
     BOL_2020_EH_INC_ALL-0.parquet
   version=v01_v05/
     BOL_2020_EH_INC_ALL-0.parquet
   ```

4. **Manifest-driven version selection**: `{piptm}` never enumerates or
   compares version directories. It reads the `file_path` field from the
   release manifest, which already contains the full partition path including
   the `version=` segment. The manifest is the **sole version-resolution
   layer**.

5. **Education columns preserved as-is**: Related change already
   implemented — `educat4`, `educat5`, `educat7` are kept with their
   original names and levels (no mapping to a single `education` column).

### Non-functional

- No performance regression: adding one more partition level has negligible
  impact on `arrow::open_dataset()` since `{piptm}` reads explicit file
  paths from the manifest, not directory scans.
- Backward compatibility: existing validation functions must be updated to
  accept the new partition structure.

## Approaches Considered

### Approach 1: Version as partition key + manifest file_path resolution (chosen)

**Summary**: Add `version` as a 4th partition directory level. The manifest's
`file_path` field encodes the full path including `version=...`. `{piptm}`
reads whatever the manifest points to — no version-comparison logic needed.

**Pros**:
- Clean separation: storage layer (partitions) and resolution layer
  (manifest) align naturally.
- Multiple versions coexist on disk with zero conflict.
- Each release manifest is a complete, self-contained snapshot — fully
  reproducible.
- `arrow::open_dataset()` can still be used for ad-hoc exploration by
  filtering `version == "v01_v04"`.
- Minimal conceptual overhead for `{piptm}` consumers — they follow the
  manifest path, period.

**Cons**:
- Orphan version directories accumulate over time (old versions that no
  manifest references). Needs an eventual cleanup/garbage-collection
  strategy.
- One more directory level in the partition tree.

**Effort**: Medium — touches schema, write path, manifest generation, and
validation across both packages.

**Recommended?**: Yes.

### Approach 2: Version only inside Parquet (status quo), manifest selects by filename

**Summary**: Keep the current 3-level partition structure. Encode version
in the filename (e.g. `BOL_2020_EH_INC_ALL_v01_v04-0.parquet`). The
manifest `file_path` still resolves which file to read.

**Pros**:
- No partition structure change.
- Simpler directory tree.

**Cons**:
- Multiple versions of the same survey land in the **same** directory,
  making `arrow::open_dataset()` on a partition pick up all versions
  simultaneously — requires explicit file-list filtering for any ad-hoc
  query.
- Filename-based versioning is fragile and harder to validate.
- No Hive-style filtering on version.

**Effort**: Small.

**Recommended?**: No — loses the clean separation that Hive partitioning
provides and complicates ad-hoc exploration.

### Approach 3: Separate version directories outside the Hive tree

**Summary**: Store each version in a top-level directory
(`v01_v04/country=.../year=.../welfare=.../...`), keeping the inner
partition structure unchanged.

**Pros**:
- Clear physical separation of versions.
- Inner partition structure unchanged.

**Cons**:
- Breaks `arrow::open_dataset()` discovery — version is outside the Hive
  tree, so you can't filter on it as a partition column.
- Duplicates the full partition tree per version — wasteful when only a
  few surveys change between versions.
- Manifest `arrow_root` would need to vary per version or become
  version-aware.

**Effort**: Medium.

**Recommended?**: No — introduces more complexity than Approach 1 without
clear benefits.

## Decision

**Approach 1** selected. Version becomes the 4th Hive partition key.
The manifest's `file_path` field is the sole version-resolution mechanism
for `{piptm}`.

## Impact Summary

### `{pipdata}` changes (write side)

| File | Change |
|------|--------|
| `arrow_generation.R` — `.build_partition_dir()` | Add `version` parameter |
| `arrow_generation.R` — `write_survey_parquet()` | Extract `version` from dt, pass to `.build_partition_dir()` |
| `arrow_generation.R` — `.validate_for_write()` | Add `version` to partition key consistency check |
| `manifest_generation.R` — `.build_manifest_file_path()` | Add `version=` segment |
| `manifest_generation.R` — `build_manifest_entry()` | Include `version` field |
| `manifest_generation.R` — `generate_release_manifest()` | Pass `version` through pipeline |

### `{piptm}` changes (read/validation side)

| File | Change |
|------|--------|
| `inst/schema/arrow-schema.json` | Add `version` to `partition_keys` and `columns`; update `educat4/5/7` replacing `education` |
| `R/schema.R` — `pip_arrow_schema()` | Add `version` field; replace `education` with `educat4/5/7` |
| `R/validate_parquet.R` — `.VP_PARTITION_KEYS` | Add `"version"` |
| `R/validate_parquet.R` — `.vp_canonical_schema()` | Add `version` field; replace `education` with `educat4/5/7` |
| `R/validate_parquet.R` — `.vp_parse_partition_path()` | Parse `version=` directory component |
| `R/validate_parquet.R` — `validate_parquet_data()` | Add path-matching check for `version` |

### Future consideration

- **Orphan cleanup**: Eventually implement a utility that compares all
  `version=` directories against all known manifests and flags/removes
  unreferenced ones. Not urgent for Phase 0.

## Next Steps

1. Update `arrow-schema.json` and `schema.R` in `{piptm}` to add `version`
   as a partition key and replace `education` with `educat4/5/7`.
2. Update `validate_parquet.R` in `{piptm}` to handle the new partition
   structure.
3. Update `arrow_generation.R` in `{pipdata}` to write with the 4-level
   partition.
4. Update `manifest_generation.R` in `{pipdata}` to include `version=`
   in file paths.
5. Run end-to-end test with BOL 2020 data to verify the full pipeline.
6. Ready to proceed with `/cg-plan` for implementation.
