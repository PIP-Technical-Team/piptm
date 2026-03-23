---
date: 2026-03-17
title: "Arrow Data Preparation Pipeline"
status: active
brainstorm: "2026-03-16-data-pipeline-architecture.md"
language: R
estimated-effort: medium
tags: [phase-0, arrow, parquet, data-preparation, pipdata, schema, validation]
---

# Plan: Arrow Data Preparation Pipeline

## Objective

Implement the data preparation pipeline that takes clean survey datasets produced by {pipdata} (`.qs2` files) and generates validated, schema-consistent, partitioned Parquet datasets in the Master Arrow Repository. This work is a prerequisite for all downstream {piptm} functionality (manifest generation, data loading, and measure computation).

## Context

### What exists today

- {pipdata} produces harmonized survey datasets as `.qs2` files with a known naming convention (`<country>_<year>_<acronym>_<vermast>_<veralt>_<welfare>.qs2`)
- The Master Arrow Repository structure has been designed (3-level partition: `country_code / year / welfare_type`) and documented in `docs/project-context.md`
- Architecture decisions are settled: Manifest-First with Lazy Validation, shared Arrow repository, version-encoded filenames
- {piptm} exists as a skeleton R package with `arrow`, `data.table`, `jsonlite`, `rlang`, `collapse` in Imports

### What the brainstorms decided

- {pipdata} owns the `.qs2` → Parquet conversion and Arrow repository population
- {piptm} does NOT write to the Arrow repository — it only reads
- The manifest is generated after Arrow partitions are written
- Multiple survey versions coexist in the same partition directory; the manifest resolves which file to read

### Constraints

- The data preparation functions live in **{pipdata}**, not {piptm}
- However, the schema contract and validation logic must be co-designed so {piptm} can rely on it
- This plan defines the work from {piptm}'s perspective: what contract {pipdata} must fulfill, plus any shared utilities or schema definitions that {piptm} needs to validate against

### Ownership boundary

| Responsibility | Owner |
|---|---|
| `.qs2` → Parquet conversion | {pipdata} |
| Arrow repository write operations | {pipdata} |
| Schema definition & validation rules | **Shared** (defined here, enforced in both) |
| Manifest generation | {pipdata} (or shared utility) |
| Manifest loading & lazy validation | {piptm} |
| Parquet reading & computation | {piptm} |

## Implementation Steps

### Phase 0A: Schema Contract Definition

Define the exact column schema that every Parquet file in the Arrow repository must conform to. This is the contract between {pipdata} and {piptm}.

#### Step 1: Define the Arrow Schema Specification

- **Files**: `inst/schema/arrow_schema.json` (in {piptm}, the canonical reference)
- **Details**:
  - Define required columns with name, Arrow data type, and constraints:

    | Column | Arrow Type | Constraints | Notes |
    |---|---|---|---|
    | `country_code` | `utf8` | ISO3, non-null | Partition key |
    | `year` | `int32` | Non-null, > 0 | Partition key |
    | `welfare_type` | `utf8` | "INC" or "CON", non-null | Partition key |
    | `welfare` | `float64` | Non-null | PPP-adjusted welfare value |
    | `weight` | `float64` | Non-null, > 0 | Survey sampling weight |
    | `survey_id` | `utf8` | Non-null | Full survey identifier |
    | `survey_acronym` | `utf8` | Non-null | Short survey name |
    | `gender` | `dictionary(int8, utf8)` | Nullable | Breakdown dimension |
    | `area` | `dictionary(int8, utf8)` | Nullable | Breakdown dimension (urban/rural) |
    | `education` | `dictionary(int8, utf8)` | Nullable | Breakdown dimension |
    | `age` | `dictionary(int8, utf8)` | Nullable | Breakdown dimension (age group) |

  - Breakdown dimension columns (`gender`, `area`, `education`, `age`) are optional per survey — their presence/absence is recorded in the manifest's `available_dimensions` field
  - Store the schema as a JSON document for language-agnostic reference
  - Provide an R helper `piptm_arrow_schema()` that returns an `arrow::schema()` object

- **Tests**: verify `piptm_arrow_schema()` returns a valid `arrow::Schema` with correct columns and types
- **Acceptance criteria**: schema JSON exists; R helper is exported, documented, and tested

#### Step 2: Define Validation Rules

- **Files**: `R/validate_parquet.R`
- **Details**:
  - Implement `validate_parquet_schema(file_path)` — checks a single Parquet file against the canonical schema:
    - All required columns present
    - Column types match expected Arrow types
    - No unexpected null values in required columns
  - Implement `validate_parquet_data(file_path)` — data-level checks:
    - `weight` > 0 for all rows
    - `welfare` is finite (no `Inf`, `NaN`)
    - `welfare_type` ∈ {"INC", "CON"}
    - `country_code` matches the partition directory
    - `year` matches the partition directory
    - Breakdown dimension values are from expected factor levels (if known)
  - Return a structured validation result (list with `$valid`, `$errors`, `$warnings`)

- **Tests**: valid files pass; invalid files (missing columns, bad weights, partition mismatch) produce specific errors
- **Acceptance criteria**: both functions implemented, tested, and documented with clear error messages

---

### Phase 0B: Arrow Dataset Generation (in {pipdata})

This phase is implemented in {pipdata} but planned here to ensure alignment with {piptm}'s expectations.

#### Step 3: Implement Survey-to-Parquet Conversion

- **Files**: (in {pipdata}) `R/arrow_generation.R`
- **Details**:
  - Implement `convert_survey_to_parquet(qs2_path, arrow_root)`:
    1. Read `.qs2` file with `qs2::qs_read()`
    2. Validate/coerce to canonical schema:
       - Ensure required columns exist
       - Cast types to match Arrow schema (e.g., breakdown dimensions → dictionary/factor)
       - Drop columns not in the schema (keep only schema-defined columns)
    3. Derive partition path from data: `country=<country_code>/year=<year>/welfare=<welfare_type>/`
    4. Derive filename from `.qs2` filename: `<survey_id>-0.parquet`
    5. Write Parquet file using `arrow::write_parquet()` (single file, not `write_dataset()`)
       - Use explicit `arrow::schema()` to enforce column types
       - Use snappy compression (default)
    6. Return the relative file path written

  - Design choice: write individual Parquet files (not `write_dataset()`) because:
    - We need control over the filename (version-encoded)
    - Partition directories already exist or are created explicitly
    - `write_dataset()` generates its own filenames which don't match our convention

- **Tests**: round-trip fidelity; schema conformance; correct filename and partition path
- **Acceptance criteria**: single `.qs2` → correctly partitioned, schema-conformant Parquet file

#### Step 4: Implement Batch Conversion Pipeline

- **Files**: (in {pipdata}) `R/arrow_batch.R`
- **Details**:
  - Implement `generate_arrow_repository(qs2_dir, arrow_root)`:
    1. Scan `qs2_dir` for all `.qs2` files
    2. For each `.qs2` file:
       - Call `convert_survey_to_parquet()` to write the Parquet file
       - Skip if the target Parquet file already exists (append-only model)
       - Log success/failure/skip
    3. Return a summary data.table: `survey_id`, `file_path`, `status`, `n_rows`, `file_size`

  - The function is **release-agnostic** — it converts all available `.qs2` files into the shared Arrow repository. Release scoping happens downstream via manifests.
  - Edge cases: failed validation → error entry in summary, skip; existing Parquet → skip

- **Tests**: batch conversion of fixture files; skips for existing files; summary report structure
- **Acceptance criteria**: all `.qs2` files in the directory are accounted for (converted, skipped, or errored)

---

### Phase 0C: Partition Validation & Repository Integrity

#### Step 5: Implement Repository-Level Validation

- **Files**: `R/validate_repository.R`
- **Details**:
  - Implement `validate_arrow_repository(arrow_root, manifest)`:
    1. For each entry in the manifest:
       - Verify the file exists at `arrow_root / file_path`
       - Verify the file passes `validate_parquet_schema()`
       - Verify `available_dimensions` in manifest match actual columns in the file
    2. Check for orphan files (Parquet files in the repository not referenced by any manifest) — warning only
    3. Return structured validation result with per-survey status

  - This is a **batch validation** function, intended to run once after Arrow generation + manifest creation, not per API call (per-call validation is the lazy validation in `load_survey_microdata()`)

- **Tests**: valid repo passes; missing files, schema mismatches, and dimension mismatches produce specific errors
- **Acceptance criteria**: full integrity check with actionable per-survey report

#### Step 6: Implement Partition Consistency Checks

- **Files**: `R/validate_parquet.R` (extend Step 2)
- **Details**:
  - Add `validate_partition_consistency(arrow_root, partition_path)`:
    1. Read all Parquet files in a single partition directory
    2. Verify all files have the same schema
    3. Verify partition key values (`country_code`, `year`, `welfare_type`) match the directory path
    4. Check for duplicate `survey_id` values across files (should not occur)
  - This catches issues like schema drift between survey versions in the same partition

- **Tests**: consistent partitions pass; schema mismatches and duplicate `survey_id` values produce errors
- **Acceptance criteria**: within-partition consistency verified; schema drift caught

---

### Phase 0D: Manifest Generation

#### Step 7: Implement Manifest Generation Function

- **Files**: (in {pipdata} or shared utility) `R/manifest_generation.R`
- **Details**:
  - Implement `generate_release_manifest(release_id, arrow_root, release_inventory, output_path, set_as_current = FALSE)`:
    1. Load release inventory (data.table with survey metadata)
    2. For each survey in the inventory:
       - Resolve the correct Parquet file path in the Arrow repository:
         - Build partition path: `country=<country_code>/year=<year>/welfare=<welfare_type>/`
         - Match file by `survey_id` pattern in filename
       - Read Parquet file schema to discover available breakdown dimensions
       - Build manifest entry with all required fields (see project-context.md schema)
    3. Assemble manifest JSON:
       ```json
       {
         "release_id": "<release_id>",
         "arrow_root": "<arrow_root>",
         "surveys": [ ... ]
       }
       ```
    4. Write to `output_path` (e.g., `manifests/manifest_<release_id>.json`)
    5. If `set_as_current = TRUE`, write/overwrite `current_release.json` in the same directory:
       ```json
       {
         "current_release": "<release_id>",
         "updated_at": "<ISO 8601 timestamp>"
       }
       ```

  - Version resolution: when multiple Parquet files exist in a partition, the inventory's `vermast`/`veralt` fields determine which file matches
  - Dimension discovery: read the Parquet schema, check which of `gender`, `area`, `education`, `age` are present as non-null columns
  - `current_release.json` is just a pointer — the manifest itself is identical regardless of whether it's the current release

- **Tests**: valid JSON output; dimension discovery matches actual columns; correct version resolution; `set_as_current` writes pointer file
- **Acceptance criteria**: manifest conforms to project-context.md schema; file paths relative to `arrow_root`

---

## Testing Strategy

### Test Data

- Create a small set of fixture `.qs2` files (3–5 surveys) with known values:
  - 1 survey with all 4 breakdown dimensions
  - 1 survey with only 2 dimensions (e.g., `gender`, `area`)
  - 1 survey with no breakdown dimensions (welfare + weight only)
  - 1 income survey (`welfare_type = "INC"`)
  - 1 consumption survey (`welfare_type = "CON"`)
- Fixture files stored in `tests/testthat/fixtures/` (small — <100 rows each)
- Hand-compute expected values for validation tests

### Test Levels

1. **Unit tests** — individual functions (schema validation, single-file conversion, manifest entry construction)
2. **Integration tests** — end-to-end pipeline (`.qs2` dir → Arrow repository → manifest → validation)
3. **Contract tests** — verify that a generated Parquet file can be read by {piptm}'s future `load_survey_microdata()` function

### Edge Cases

- Survey with zero rows (should error or warn)
- Survey with all-NA breakdown dimension (column present but empty)
- Extremely large survey (performance, not correctness — defer to benchmarking)
- Duplicate survey in inventory (same `survey_id` twice)
- Unicode in country names or survey acronyms (should not affect partition keys, which are ISO3 codes)

---

## Documentation Checklist

- [ ] Function documentation (roxygen2) for all exported functions
- [ ] `inst/schema/arrow_schema.json` with inline documentation
- [ ] README section: "Arrow Dataset Preparation" (or separate vignette)
- [ ] Inline comments for version resolution logic and partition path construction
- [ ] Usage examples in roxygen2 `@examples` sections (using fixture data)
- [ ] Update `docs/project-context.md` if schema decisions change

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Schema drift between {pipdata} versions | Parquet files become inconsistent | Canonical schema in {piptm}'s `inst/schema/`; validation at write time in {pipdata} |
| Network latency on PIP infrastructure | Batch validation/generation is slow | Parallelize file operations; validate only requested surveys lazily at runtime |
| `.qs2` files missing expected columns | Conversion fails | Validate before conversion; clear error messages with file path and missing columns |
| Multiple Parquet files per partition with different schemas | Downstream confusion | Partition consistency check (Step 6); each survey version written with canonical schema |
| Manifest ↔ repository drift | Manifest references files that don't exist or have wrong schema | Repository-level validation (Step 5) as a post-generation gate check |
| Breakdown dimension factor levels change between releases | Inconsistent category labels across releases | Document expected factor levels in schema; validate at write time |

---

## Out of Scope

- **Computation engine implementation** — covered separately in Phase 1 (see FGT brainstorm)
- **Manifest loading in {piptm}** — covered in Phase 1 (`.onLoad()` implementation)
- **`load_survey_microdata()` implementation** — covered in Phase 2
- **`table_maker()` API** — covered in Phase 2
- **Raw microdata ingestion / harmonization** — owned by {pipdata}, not planned here
- **API layer and platform integration** — future phase
- **Performance optimization** — premature at this stage; revisit after functional correctness is established
- **Marginals and totals computation** — deferred per FGT brainstorm decision

---

## File Structure Summary

### In {piptm}

```
R/
├── validate_parquet.R         # Schema + data validation (Steps 2, 6)
├── validate_repository.R      # Repository-level validation (Step 5)
├── arrow_schema.R             # piptm_arrow_schema() helper (Step 1)
inst/
└── schema/
    └── arrow_schema.json      # Canonical schema definition (Step 1)
tests/
└── testthat/
    ├── fixtures/              # Small .qs2 and .parquet fixture files
    ├── test-arrow-schema.R    # Step 1 tests
    ├── test-validate-parquet.R  # Steps 2, 6 tests
    └── test-validate-repository.R  # Step 5 tests
```

### In {pipdata} (for reference — not implemented here)

```
R/
├── arrow_generation.R         # Survey-to-Parquet conversion (Step 3)
├── arrow_batch.R              # Batch pipeline (Step 4)
└── manifest_generation.R      # Manifest + current_release (Steps 7, 8)
tests/
└── testthat/
    ├── test-arrow-generation.R
    ├── test-arrow-batch.R
    └── test-manifest-generation.R
```

---

## Dependency on Downstream Phases

This plan produces:

1. **A populated Arrow repository** with validated, schema-consistent Parquet files
2. **A release manifest** (`manifest_<release_id>.json`) with survey metadata and file paths
3. **A `current_release.json`** pointer file
4. **Validation utilities** in {piptm} for lazy validation at runtime

These outputs are the direct inputs to:

- **Phase 1**: Manifest loading (`.onLoad()`), `load_survey_microdata()`, and the computation engine
- **Phase 2**: `table_maker()` API integration

The schema contract (Step 1) is the single most important deliverable — it defines the interface between {pipdata} and {piptm} and must be agreed upon before any Parquet files are written.
