---
date: 2026-04-03
title: "Manifest Generation and Version Partition Filtering"
status: active
brainstorm: ".cg-docs/brainstorms/2026-04-03-manifest-generation-version-partition.md"
language: "R"
estimated-effort: "medium"
tags: [manifest, partitions, version, pipdata, piptm, phase-0, arrow]
---

# Plan: Manifest Generation and Version Partition Filtering

## Objective

Transition the manifest from storing physical `file_path` per entry to storing
four logical partition filter keys (`country_code`, `year`, `welfare_type`,
`version`). Update `{pipdata}` manifest generation to use the release inventory
as the authoritative source, add dimension introspection, and produce the new
JSON schema. Update `{piptm}` manifest loading and data access to use
`open_dataset() |> filter()` with partition pushdown instead of
`read_parquet(file_path)`.

This plan partially supersedes the April 2 plan
(`2026-04-02-version-partition-and-manifest-resolution.md`) whose schema and
validation steps are already implemented and landed. The `version` partition
key, `educat4/5/7` columns, and updated validation functions are in place.
This plan builds on that foundation.

## Context

**What exists today (after the April 2 plan):**

- `{piptm}` schema (`schema.R`, `arrow-schema.json`): fully updated — `version`
  is a required field, `educat4/5/7` replace `education`, 14 total columns.
- `{piptm}` validation (`validate_parquet.R`): fully updated — 4-level partition
  parsing, version path matching, `educat4/5/7` factor checks.
- `{pipdata}` `arrow_generation.R`: fully updated — 4-level partition directories
  (`country/year/welfare/version`), lazy schema accessors.
- `{pipdata}` `manifest_generation.R`: partially updated — already stores
  `version` field, uses 4-level `file_path`, discovers `educat4/5/7` as
  dimensions. But the manifest still stores `file_path` and the consumption
  pattern in `{piptm}` uses `read_parquet(file_path)`.
- `{piptm}` has NO manifest loading, `.onLoad()`, or data access code yet —
  these are new.

**What this plan adds:**

1. New manifest JSON schema: replace `file_path` with four partition filter keys
2. Release-inventory-driven generation in `{pipdata}`
3. Dimension introspection using Arrow `open_dataset() |> filter()` at generation time
4. Manifest loading + caching in `{piptm}` via `.onLoad()`
5. `load_survey_microdata()` in `{piptm}` using `open_dataset() |> filter()`
6. Updated `{piptm}` internal environment setup

**Brainstorm decision:** Approach 1 — Release-Inventory-Driven Manifest with
Partition Filters. See
`.cg-docs/brainstorms/2026-04-03-manifest-generation-version-partition.md`.

## Implementation Steps

### Step 1: Update `{pipdata}` manifest entry schema (remove `file_path`, add partition keys)

- **Files**: `pipdata/R/manifest_generation.R`
- **Details**:
  1. `build_manifest_entry()` — remove `file_path` parameter and return field.
     Add explicit `country_code`, `year`, `welfare_type`, `version` as the
     four partition filter keys (these already exist but were duplicated with
     `file_path`). Add `survey_acronym` and `module` (already present). Add
     `dimensions` field (rename from `available_dimensions`). Remove `vermast`,
     `veralt` — these are internal to generation; the manifest consumer only
     needs `version`.
  2. Updated return structure per entry:
     ```r
     list(
       pip_id         = ...,
       survey_id      = ...,
       country_code   = ...,
       year           = ...,
       welfare_type   = ...,
       version        = ...,
       survey_acronym = ...,
       module         = ...,
       dimensions     = c("area", "gender", "age")
     )
     ```
  3. Remove `.build_manifest_file_path()` — no longer needed since we don't
     store physical paths in the manifest. (The function still exists in
     `generate_release_manifest()` for file-existence checks at generation
     time — factor it into a private helper that constructs the path solely
     for the validation/introspection step, not for manifest output.)
  4. Rename `.build_manifest_file_path()` to `.derive_parquet_path()` (internal
     only, used for file existence checks during generation). Keep the same
     logic. Update the single call site in `generate_release_manifest()`.
- **Tests**: Update `test-manifest-generation.R`:
  - `build_manifest_entry()` returns list without `file_path`, `vermast`,
    `veralt`. Contains `dimensions` (not `available_dimensions`).
  - Verify field names match the new schema exactly.
- **Acceptance criteria**: `build_manifest_entry()` returns 9 fields matching
  the brainstorm schema. No `file_path` in the entry.

### Step 2: Update `{pipdata}` manifest generation to produce new JSON format

- **Files**: `pipdata/R/manifest_generation.R`
- **Details**:
  1. `generate_release_manifest()` — update the top-level JSON wrapper:
     - Remove `arrow_root` from the JSON output (the consumer configures this
       independently via env var).
     - Add `generated_at` field (rename from `created_at`).
     - Rename `surveys` array to `entries`.
     - Keep `release` field (rename from `release_id` to match brainstorm
       schema: `"release": "20260206"`).
  2. Updated top-level JSON structure:
     ```json
     {
       "release": "20260206",
       "generated_at": "2026-04-03T17:00:00Z",
       "entries": [ ... ]
     }
     ```
  3. Update the loop that builds entries: call the new
     `build_manifest_entry()` signature (no `file_path`, no `vermast`/`veralt`).
     Still use `.derive_parquet_path()` internally to check file existence
     and call `discover_parquet_dimensions()`.
  4. The `arrow_root` parameter remains on the function signature (needed at
     generation time to find files) — it's just not written to the manifest JSON.
  5. Rename `release_id` parameter to `release` for consistency with the JSON
     field name.
  6. Update the summary `data.table` returned invisibly: replace
     `available_dimensions` column with `dimensions`.
- **Tests**: Update `test-manifest-generation.R`:
  - Parsed JSON has `release` (not `release_id`), `generated_at` (not
    `created_at`), `entries` (not `surveys`), no `arrow_root`.
  - Each entry in `entries` matches the Step 1 schema.
  - Summary dt has `dimensions` column.
- **Acceptance criteria**: Generated manifest JSON conforms to the brainstorm
  schema. Existing `set_as_current` pointer logic still works.

### Step 3: Add dimension introspection via `open_dataset() |> filter()`

- **Files**: `pipdata/R/manifest_generation.R`
- **Details**:
  1. Update `discover_parquet_dimensions()` to accept either a single file path
     OR a set of partition filter arguments (`arrow_root`, `country_code`,
     `year`, `welfare_type`, `version`). When filter arguments are provided,
     use `arrow::open_dataset(arrow_root) |> filter(...) |> schema` to
     determine available columns via partition pushdown rather than opening a
     single file.
  2. Alternative (simpler, recommended): keep
     `discover_parquet_dimensions(file_path)` as-is — the file path is already
     derived during generation. The brainstorm says "inspect each partition" but
     the current approach of opening the single file is equivalent since each
     `(country, year, welfare, version)` partition contains exactly one Parquet
     file. No change needed to `discover_parquet_dimensions()`.
  3. **Decision**: Keep `discover_parquet_dimensions()` unchanged — it already
     reads the schema from the single Parquet file and returns the intersection
     with the known dimension universe. This is correct and efficient.
- **Tests**: Existing tests in `test-manifest-generation.R` already cover
  `discover_parquet_dimensions()` with `educat4`, `educat5`, `age`, `gender`,
  `area`. No new tests needed for this step.
- **Acceptance criteria**: No regression in dimension discovery. Function still
  works correctly.

### Step 4: Create `{piptm}` package environment and `.onLoad()` manifest loading

- **Files**: `piptm/R/zzz.R` (new), `piptm/R/manifest.R` (new)
- **Details**:
  1. Create `.piptm_env` package environment in `zzz.R`:
     ```r
     .piptm_env <- new.env(parent = emptyenv())
     ```
  2. `.onLoad()` in `zzz.R`:
     - Read `PIPTM_MANIFEST_DIR` env var. If unset, store `NULL` and skip
       manifest loading (dev/testing mode).
     - Read `PIPTM_ARROW_ROOT` env var. Store in `.piptm_env$arrow_root`.
     - If manifest dir is set: scan for `manifest_*.json` files, parse each
       with `jsonlite::fromJSON()`, convert `entries` to `data.table`, store
       in `.piptm_env$manifests` (named list keyed by `release`).
     - Read `current_release.json` if it exists, store the release ID in
       `.piptm_env$current_release`.
  3. Create `manifest.R` with accessor and utility functions:
     - `piptm_manifest_dir()` — return the configured manifest directory.
     - `piptm_arrow_root()` — return the configured Arrow root.
     - `piptm_manifests()` — return the named list of loaded manifests.
     - `piptm_current_release()` — return the default release ID.
     - `piptm_manifest(release = NULL)` — return the `data.table` for a
       specific release (defaults to current). Error if not found.
     - `set_manifest_dir(path)` — override manifest dir at runtime (for
       dev/testing). Triggers re-scan.
     - `set_arrow_root(path)` — override Arrow root at runtime.
     - `.load_manifests(manifest_dir)` — internal function that performs the
       scan + parse. Called by `.onLoad()` and `set_manifest_dir()`.
  4. Manifest `data.table` columns: `pip_id`, `survey_id`, `country_code`,
     `year`, `welfare_type`, `version`, `survey_acronym`, `module`,
     `dimensions` (list column — each element is a character vector).
- **Tests**: Create `piptm/tests/testthat/test-manifest.R`:
  - `.load_manifests()` correctly parses a fixture manifest JSON into a
    named list of `data.table`s.
  - `piptm_manifest("20260206")` returns the correct `data.table`.
  - `piptm_manifest()` defaults to current release.
  - `piptm_manifest("nonexistent")` errors.
  - `set_manifest_dir()` with a temp dir containing fixture JSONs updates
    `.piptm_env$manifests`.
  - `set_arrow_root()` updates `.piptm_env$arrow_root`.
- **Acceptance criteria**: After `.onLoad()` with a valid manifest dir, all
  accessor functions return correct values. Manifests are `data.table`s with
  correct column names and types.

### Step 5: Create `{piptm}` `load_survey_microdata()` using `open_dataset() |> filter()`

- **Files**: `piptm/R/load_data.R` (new)
- **Details**:
  1. `load_survey_microdata(release, country_code, year, welfare_type)`:
     - Look up manifest for the given release.
     - Filter manifest `data.table` for matching
       `country_code`/`year`/`welfare_type`.
     - Extract `version` from the manifest entry.
     - Open shared Arrow dataset: `arrow::open_dataset(.piptm_env$arrow_root)`.
     - Filter with partition pushdown:
       ```r
       arrow::open_dataset(arrow_root) |>
         dplyr::filter(
           country_code == cc,
           surveyid_year == yr,
           welfare_type == wt,
           version == ver
         ) |>
         dplyr::collect() |>
         data.table::as.data.table()
       ```
     - Return the `data.table`.
  2. Also return the manifest entry's `dimensions` as an attribute or
     alongside the data (e.g. `attr(dt, "dimensions") <- entry$dimensions`),
     so downstream code knows which breakdown columns are available without
     inspecting the data.
  3. Error if no matching manifest entry found. Error if Arrow dataset
     returns 0 rows (file missing or partition filter mismatch).
  4. Multiple-survey load: `load_surveys(release, entries_dt)` — accepts a
     filtered manifest `data.table` (multiple rows) and returns a single
     `data.table` with all surveys combined. Uses a single
     `open_dataset() |> filter(...)` call with `%in%` filters on the
     partition keys.
- **Tests**: Create `piptm/tests/testthat/test-load-data.R`:
  - Write fixture Parquet files in a temp Arrow repo with 4-level partitions.
  - Write a fixture manifest JSON.
  - `load_survey_microdata()` returns correct data for a single survey.
  - `load_survey_microdata()` errors on non-existent release.
  - `load_survey_microdata()` errors on non-matching survey.
  - `load_surveys()` returns combined data for multiple surveys.
  - Verify `dimensions` attribute is set.
- **Acceptance criteria**: `load_survey_microdata()` returns a `data.table`
  with correct schema. Arrow partition pushdown is used (verified by checking
  that the query doesn't scan the entire dataset).

### Step 6: Wire up manifest-based loading in `{piptm}` infrastructure

- **Files**: `piptm/DESCRIPTION`, `piptm/NAMESPACE`
- **Details**:
  1. Add new imports to `DESCRIPTION`:
     - `jsonlite` (for manifest parsing)
     - Verify `arrow`, `dplyr`, `data.table` are already listed.
  2. Update `NAMESPACE` via roxygen2 `@importFrom` tags in the new files.
  3. Document all exported functions with roxygen2.
  4. Run `devtools::document()` to regenerate `NAMESPACE` and `.Rd` files.
- **Tests**: `devtools::check()` passes with no errors or warnings.
- **Acceptance criteria**: Package installs cleanly. All new functions are
  exported and documented.

### Step 7: End-to-end verification

- **Details**:
  1. Create a temp Arrow repo with 2–3 fixture surveys (different countries,
     years, welfare types, versions).
  2. Generate a manifest using `{pipdata}`:
     ```r
     generate_release_manifest(
       release           = "20260206",
       arrow_root        = tmp_arrow,
       release_inventory = fixture_inventory,
       output_path       = file.path(tmp_manifest, "manifest_20260206.json")
     )
     ```
  3. Point `{piptm}` at the manifest:
     ```r
     piptm::set_manifest_dir(tmp_manifest)
     piptm::set_arrow_root(tmp_arrow)
     ```
  4. Load a survey:
     ```r
     dt <- piptm::load_survey_microdata(
       release      = "20260206",
       country_code = "COL",
       year         = 2010,
       welfare_type = "INC"
     )
     ```
  5. Verify: correct columns, correct rows, `dimensions` attribute set,
     version matches manifest entry.
  6. Verify `{piptm}` validation still passes on the loaded data:
     ```r
     piptm::validate_parquet_schema(parquet_file_path)
     ```
- **Acceptance criteria**: Full round-trip from Arrow repo → manifest
  generation → manifest loading → data loading works. Data matches
  expectations.

## Testing Strategy

| Layer | Package | Test file | Coverage |
|-------|---------|-----------|----------|
| Manifest entry schema | `{pipdata}` | `test-manifest-generation.R` | New entry format without `file_path`; `dimensions` field |
| Manifest JSON format | `{pipdata}` | `test-manifest-generation.R` | `release`, `generated_at`, `entries` top-level fields |
| Manifest loading | `{piptm}` | `test-manifest.R` (new) | `.load_manifests()`, `piptm_manifest()`, `set_manifest_dir()` |
| Data loading | `{piptm}` | `test-load-data.R` (new) | `load_survey_microdata()`, `load_surveys()`, partition pushdown |
| `.onLoad()` | `{piptm}` | `test-manifest.R` (new) | Env var configuration, manifest caching |
| End-to-end | `{piptm}` | `test-load-data.R` (new) | Write fixtures → generate manifest → load data |

**Edge cases to cover:**

- Manifest with 0 entries (all surveys missing from Arrow repo) → error
- Release not found in loaded manifests → informative error
- `PIPTM_MANIFEST_DIR` not set → graceful degradation (dev mode)
- `PIPTM_ARROW_ROOT` not set → error on first data load attempt
- Survey in manifest but Parquet file missing from disk → error with clear message
- Multiple versions of same survey coexisting (different releases reference
  different versions) → each loads correctly via its own version filter
- Manifest with `dimensions = []` (no breakdown columns) → data loads correctly
  with only required columns

## Documentation Checklist

- [ ] `build_manifest_entry()` roxygen — update to remove `file_path`, `vermast`, `veralt`; add `dimensions` field docs
- [ ] `generate_release_manifest()` roxygen — update JSON schema example, rename `release_id` → `release`
- [ ] `piptm_manifest_dir()`, `piptm_arrow_root()`, `piptm_manifests()`, `piptm_current_release()`, `piptm_manifest()` — new roxygen docs
- [ ] `set_manifest_dir()`, `set_arrow_root()` — new roxygen docs
- [ ] `load_survey_microdata()`, `load_surveys()` — new roxygen docs
- [ ] `.onLoad()` in `zzz.R` — internal documentation of env var contract
- [ ] `docs/project-context.md` — update manifest schema example and data flow diagram
- [ ] `compound-gpid.md` — update Architecture Notes to reflect partition-filter loading pattern

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Breaking change to `build_manifest_entry()` signature | Certain | Tests fail until updated | Steps 1–2 update both code and tests together. No downstream consumers exist yet outside this project. |
| `open_dataset()` performance with many partitions | Low | Slow queries | Arrow's Hive-partitioned dataset with pushdown is designed for this. Benchmark with real data in Step 7. |
| `PIPTM_MANIFEST_DIR` misconfigured in production | Low | `.onLoad()` fails | Informative error message. Fallback to `set_manifest_dir()` override. |
| Manifest schema drift (future fields added) | Low (long-term) | Parse errors | Use `jsonlite::fromJSON(..., simplifyVector = FALSE)` and extract known fields. Unknown fields are silently ignored. |
| `version` filter returns 0 rows (version mismatch between manifest and disk) | Medium | Silent empty result | Explicit row-count check after `collect()`. Error if 0 rows returned. |
| Concurrent Arrow writes and reads during manifest generation | Low | Partial file reads | Generation reads only metadata (schema). Full data is never read during generation. Arrow writes are atomic per file. |

## Out of Scope

- `table_maker()` top-level API function (Phase 2)
- `compute_measures()` orchestrator (Phase 1)
- Core measure functions (`compute_fgt()`, `compute_gini()`, etc.) (Phase 1)
- Manifest inspection CLI utilities
- Performance benchmarking of partition pushdown vs. direct `read_parquet()`
- Orphan version cleanup utility
- Migration of existing 3-level partition data (re-generation expected)
- `{pipdata}` changes to `arrow_prep.R` or `arrow_generation.R` (already complete)

## Dependency Note

Steps 1–3 modify `{pipdata}`. Steps 4–6 create new code in `{piptm}`. There
is no cross-dependency between them — they can be implemented in parallel or
sequentially. Step 7 requires both packages to be updated.
