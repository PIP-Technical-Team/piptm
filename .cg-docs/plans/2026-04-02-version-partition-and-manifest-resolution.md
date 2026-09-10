---
date: 2026-04-02
title: "Version as partition key with manifest-based resolution"
status: active
brainstorm: ".cg-docs/brainstorms/2026-04-02-version-partition-and-manifest-resolution.md"
language: "R"
estimated-effort: "medium"
tags: [arrow, partitioning, versioning, manifest, pipdata, piptm, schema]
---

# Plan: Version as Partition Key with Manifest-Based Resolution

## Objective

Add `version` (e.g. `"v01_v04"`) as a 4th Hive partition key in the Arrow
repository so that multiple survey versions coexist on disk. The release
manifest's `file_path` field becomes the sole version-resolution mechanism
for `{piptm}`. Also update the schema contract to replace the single
`education` column with the original `educat4`, `educat5`, `educat7`
columns preserved as-is from GMD data.

## Context

**What exists today:**

- `{pipdata}` `arrow_prep.R` already injects `version` as a data column
  and keeps `educat4/5/7` (changes landed in current session).
- `{pipdata}` `arrow_generation.R` partitions on 3 keys:
  `country_code / surveyid_year / welfare_type`. Version is not a partition
  key.
- `{pipdata}` `manifest_generation.R` builds `file_path` with 3 partition
  levels. `build_manifest_entry()` has `vermast`/`veralt` as separate
  fields but no `version` field.
- `{piptm}` `arrow-schema.json` defines 3 partition keys and lists
  `education` as a single dictionary column.
- `{piptm}` `schema.R` (`pip_arrow_schema()`) mirrors the JSON: no
  `version` field, single `education` dict column.
- `{piptm}` `validate_parquet.R` validates against 3 partition keys, checks
  `education` levels against a fixed 4-level set.
- Tests exist for `manifest_generation.R` but not for `arrow_generation.R`
  or `arrow_prep.R`.

**Brainstorm decision:** Approach 1 — version becomes the 4th Hive
partition key. Manifest `file_path` is the sole resolution mechanism.

## Implementation Steps

### Step 1: Update `{piptm}` schema definition (`arrow-schema.json`)

- **File**: `piptm/inst/schema/arrow-schema.json`
- **Details**:
  1. Add `"version"` to the `partition_keys` array (position 4).
  2. Add `"version"` to the `required_columns` array.
  3. Remove `"education"` from `optional_columns` and
     `breakdown_dimensions`. Add `"educat4"`, `"educat5"`, `"educat7"`.
  4. Add a new column entry for `version`:
     ```json
     {
       "name": "version",
       "arrow_type": "utf8",
       "r_type": "character",
       "required": true,
       "nullable": false,
       "is_partition_key": true,
       "source": "metadata",
       "source_field": ["vermast", "veralt"],
       "construction": "paste0(tolower(vermast), '_', tolower(veralt))",
       "constraints": {
         "pattern": "^v[0-9]{2}_v[0-9]{2}$",
         "unique_per_file": true
       },
       "description": "Combined master and alternative version string. Atomic value — never split into components at the partition level."
     }
     ```
  5. Replace the `education` column entry with three entries for `educat4`,
     `educat5`, `educat7` — all optional, nullable, `dictionary(int32, utf8)`,
     no fixed level constraints (levels are survey-specific).
  6. Bump schema `version` to `"2.0.0"` and update `"updated"` date.
- **Tests**: Schema JSON is not tested directly — validated indirectly
  through `pip_arrow_schema()` tests.
- **Acceptance criteria**: JSON parses cleanly; `partition_keys` has 4
  entries; `version` column is defined; `education` column is gone;
  `educat4/5/7` are present.

### Step 2: Update `{piptm}` R schema functions (`schema.R`)

- **File**: `piptm/R/schema.R`
- **Details**:
  1. `pip_arrow_schema()`:
     - Add `version = list(type = arrow::utf8(), required = TRUE)` to
       `$fields`.
     - Remove `education` field. Add `educat4`, `educat5`, `educat7` as
       optional `dict_type` fields.
     - Remove `education` from `$levels`. (No fixed levels for
       `educat4/5/7` — levels are survey-specific.)
  2. `pip_required_cols()` and `pip_allowed_cols()` are derived from
     `pip_arrow_schema()` — no code changes needed, they pick up the new
     fields automatically.
- **Tests**: Add a `test-schema.R` in `piptm/tests/testthat/`:
  - `pip_arrow_schema()$fields` contains `version` with `utf8` type.
  - `pip_arrow_schema()$fields` contains `educat4`, `educat5`, `educat7`.
  - `pip_arrow_schema()$fields` does NOT contain `education`.
  - `pip_required_cols()` includes `"version"`.
  - `pip_allowed_cols()` includes `"educat4"`, `"educat5"`, `"educat7"`.
- **Acceptance criteria**: `pip_required_cols()` returns 8 columns (was 7).
  `pip_allowed_cols()` returns 13 columns (was 11). No `education` in
  either.

### Step 3: Update `{piptm}` validation functions (`validate_parquet.R`)

- **File**: `piptm/R/validate_parquet.R`
- **Details**:
  1. Module-level constants:
     - `.VP_PARTITION_KEYS`: add `"version"`.
     - Remove `.VP_EDU_LEVELS`.
  2. `.vp_canonical_schema()`:
     - Add `arrow::field("version", arrow::utf8())`.
     - Remove `arrow::field("education", dict_type)`.
     - Add `arrow::field("educat4", dict_type)`,
       `arrow::field("educat5", dict_type)`,
       `arrow::field("educat7", dict_type)`.
  3. `.vp_parse_partition_path()`:
     - Add extraction of `version=` directory component.
     - Return `version` in the output list.
  4. `validate_parquet_data()`:
     - Add `"version"` to the partition-key consistency loop.
     - Add path-matching check: data `version` vs directory `version=`.
     - Remove the `education` level-conformance check.
     - Add factor-type check for `educat4/5/7` (if present, must be
       factor; no fixed level validation).
  5. `validate_partition_consistency()`:
     - The partition-key consistency check (Check 3) already uses
       `.VP_PARTITION_KEYS` — adding `"version"` there propagates
       automatically. However, the `col_select` call uses
       `dplyr::any_of(.VP_PARTITION_KEYS)`, so `version` is automatically
       included.
     - The docstring example path should be updated to include
       `version=v01_v04/`.
- **Tests**: Update/create `test-validate-parquet.R` in
  `piptm/tests/testthat/`:
  - `.vp_parse_partition_path()` extracts `version` correctly from a
    4-level path.
  - `validate_parquet_schema()` accepts a file with `version` column.
  - `validate_parquet_schema()` rejects a file missing `version` column.
  - `validate_parquet_data()` detects `version` mismatch between data and
    path.
  - `validate_parquet_data()` accepts `educat4` / `educat5` as factors.
  - `validate_parquet_data()` does NOT reject arbitrary `educat4` levels.
- **Acceptance criteria**: All existing tests still pass after
  modifications. New tests cover `version` partition key and `educat4/5/7`
  replacing `education`.

### Step 4: Update `{pipdata}` write path (`arrow_generation.R`)

- **File**: `pipdata/R/arrow_generation.R`
- **Details**:
  1. `.build_partition_dir()`: add `version` parameter, append
     `paste0("version=", version)` segment.
  2. `write_survey_parquet()`:
     - Extract `version <- dt[1L, version]` alongside other scalar keys.
     - Pass `version` to `.build_partition_dir()`.
     - Update `rel_path` construction to include `version=`.
  3. `.validate_for_write()`:
     - Add `"version"` to the partition-key consistency loop.
     - Remove `education` level check. Add factor-type check for
       `educat4/5/7`.
  4. `.build_arrow_schema()`: uses `piptm::pip_arrow_schema()` which will
     already be updated — no code changes needed here as long as `{piptm}`
     is rebuilt first.
  5. Module-level constants (`.SCHEMA_GEN`, `.REQUIRED_COLS_GEN`, etc.):
     derived from `piptm::pip_arrow_schema()` — automatically pick up
     changes after `{piptm}` is rebuilt.
  6. Update `dim_cols` in `write_survey_parquet()` from
     `c("gender", "area", "education", "age")` to
     `c("gender", "area", "educat4", "educat5", "educat7", "age")`.
  7. Update all docstrings and header comments to show 4-level partition
     structure.
- **Tests**: Create `test-arrow-generation.R` in
  `pipdata/tests/testthat/`:
  - `.build_partition_dir()` returns path with `version=` segment.
  - `write_survey_parquet()` writes to the correct 4-level directory.
  - `.validate_for_write()` rejects data with inconsistent `version`
    values.
  - Round-trip: write then read back, verify `version` column matches.
- **Acceptance criteria**: BOL 2020 test case writes to
  `country=BOL/year=2020/welfare=INC/version=v01_v04/BOL_2020_EH_INC_ALL-0.parquet`.

### Step 5: Update `{pipdata}` manifest generation (`manifest_generation.R`)

- **File**: `pipdata/R/manifest_generation.R`
- **Details**:
  1. `.build_manifest_file_path()`: add `version` parameter, insert
     `paste0("version=", version)` segment between `welfare=` and the
     filename.
  2. `build_manifest_entry()`: add `version` parameter and include it in
     the returned list.
  3. `.manifest_dim_cols()`: replace `"education"` with `"educat4"`,
     `"educat5"`, `"educat7"`.
  4. `generate_release_manifest()`:
     - The inventory must now contain a `version` column, OR it must be
       derived from `vermast`/`veralt` inline. Since the inventory has
       `vermast` and `veralt` as separate columns, derive `version` as
       `paste0(tolower(vermast), "_", tolower(veralt))` when building each
       entry.
     - Pass `version` to `.build_manifest_file_path()` and
       `build_manifest_entry()`.
  5. Update manifest JSON structure documentation in the header comment to
     show `version` field and 4-level `file_path`.
- **Tests**: Update `test-manifest-generation.R`:
  - `make_fixture_dt()`: add `version` column.
  - `write_fixture_parquet()`: update partition path to include `version=`.
  - `build_manifest_entry()`: verify `version` field is present in output.
  - `.build_manifest_file_path()`: verify `version=` segment in path.
  - `generate_release_manifest()`: verify `file_path` in JSON includes
    `version=`.
  - `discover_parquet_dimensions()`: verify `educat4`/`educat5` are
    discovered instead of `education`.
- **Acceptance criteria**: Generated manifest JSON has `version` field per
  survey and `file_path` includes 4-level partition path. All existing
  manifest tests pass after update.

### Step 6: End-to-end verification

- **Details**:
  1. Rebuild `{piptm}` (`devtools::load_all()` in piptm).
  2. Rebuild `{pipdata}` (`devtools::load_all()` in pipdata).
  3. Run the BOL 2020 test case from the R console:
     ```r
     pip_id <- inv[country_code == "BOL" & surveyid_year == 2020 &
                   module == "ALL", pip_id]
     dt <- prepare_for_arrow(bol_2020, bol_2020_meta, pip_id = pip_id)
     str(dt)  # verify version col, educat4/5 cols, no education col
     ```
  4. Write to a temp Arrow repo and verify directory structure:
     ```r
     tmp <- tempdir()
     result <- write_survey_parquet(dt, arrow_repo_path = tmp)
     list.files(tmp, recursive = TRUE)
     # Expect: country=BOL/year=2020/welfare=INC/version=v01_v04/...
     ```
  5. Run `{piptm}` validation on the written file:
     ```r
     piptm::validate_parquet_schema(result$file_path)
     piptm::validate_parquet_data(result$file_path)
     ```
- **Acceptance criteria**: All validation passes. Directory structure is
  4-level. `version` column round-trips correctly.

## Testing Strategy

| Layer | Package | Test file | Coverage |
|-------|---------|-----------|----------|
| Schema definition | `{piptm}` | `test-schema.R` | `version` field present, `education` removed, `educat4/5/7` added |
| Partition path parsing | `{piptm}` | `test-validate-parquet.R` | `version=` extraction, 4-level paths |
| Schema validation | `{piptm}` | `test-validate-parquet.R` | Accept/reject files with/without `version` |
| Data validation | `{piptm}` | `test-validate-parquet.R` | `version` path matching, `educat4/5/7` factor check |
| Partition dir build | `{pipdata}` | `test-arrow-generation.R` | 4-level path construction |
| Write path | `{pipdata}` | `test-arrow-generation.R` | Correct directory, round-trip |
| Manifest path | `{pipdata}` | `test-manifest-generation.R` | `version=` in file_path, `version` field in entry |

**Edge cases to cover:**
- `version` with unusual but valid format (e.g. `"v01_v99"`)
- Survey with only `educat4`, only `educat5`, both, or neither
- Multiple versions of the same survey coexisting in different `version=`
  directories

## Documentation Checklist

- [x] `inject_metadata_cols()` roxygen — already updated (includes `vermast`/`veralt`)
- [ ] `arrow-schema.json` — update column descriptions and partition_keys
- [ ] `pip_arrow_schema()` roxygen — update `@examples` to show `version`
- [ ] `.build_partition_dir()` roxygen — add `version` param docs
- [ ] `write_survey_parquet()` roxygen — update partition structure in docs
- [ ] `build_manifest_entry()` roxygen — add `version` param
- [ ] `generate_release_manifest()` roxygen — update manifest JSON example
- [ ] Header comments in `arrow_generation.R` — update partition structure
- [ ] Header comments in `manifest_generation.R` — update manifest JSON example

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `{piptm}` not rebuilt before `{pipdata}` — schema constants stale | Medium | Test failures | Implementation order: Steps 1–3 (`{piptm}`) before Steps 4–5 (`{pipdata}`). Document dependency. |
| Existing Parquet files in Arrow repo have 3-level paths | Medium | Validation failures for old files | Old files will fail `validate_parquet_schema()` (missing `version` column). This is acceptable — a re-generation is expected. |
| Orphan `version=` directories accumulate | Low (long-term) | Disk bloat | Out of scope for now. Document as future work (cleanup utility). |
| `educat4/5/7` level sets vary across surveys | Expected | Schema drift within partitions | `validate_partition_consistency()` Check 2 (identical schemas) will flag this. Acceptable: different surveys have different education columns. The check applies within a `version=` directory which holds a single file. |

## Out of Scope

- Orphan version cleanup utility
- `{piptm}` production data-loading functions (`load_survey_microdata()`)
- Manifest loading/caching in `{piptm}` `.onLoad()`
- Any changes to `arrow_prep.R` — already done in current session
- Performance benchmarking of 4-level vs 3-level partitioning
