---
date: 2026-06-23
title: "Variable Registry — Build, Load, and API Metadata Endpoints"
status: completed
scope: "Standard"
brainstorm: null
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
phases: 2
completed-phases: [1, 2]
current-phase: null
tags: [registry, api, plumber, metadata, phase-4, categories, covariates]
execution-report: ".cg-docs/work-reports/2026-06-23-variable-registry.md"
---

# Plan: Variable Registry — Build, Load, and API Metadata Endpoints

## Objective

Implement a release-specific variable registry that acts as the single source
of truth for all variable-level metadata in the Table Maker system. The
registry powers three plumber API metadata endpoints consumed by the Step 2
wizard UI. Remove the outdated static `pip_tablemaker_categories()` and
`pip_tablemaker_covariates()` functions and replace them with registry-backed
accessors.

## Context

### Background

Table Maker Step 2 presents the user with three decisions:

1. **Decision 1 — Sample base**: filter the survey population by categorical
   variables and their subcategories (e.g. keep only "Secondary education:
   Not completed" × "Age group: 20–65").
2. **Decision 2 — Analysis variable**: choose a variable to compute statistics
   on (e.g. Welfare, Age, Poverty status) and select up to four statistics.
3. **Decision 3 — Layout covariates**: drag categorical variables into four
   layout slots (Columns, Rows, Super Columns, Super Rows) to slice the table.

The UI renders all three decisions from metadata endpoints served at startup.
Those endpoints need a structured, release-specific data source — the variable
registry — which merges two layers:

- **Layer 1 (automated)**: the pipdata recode spec for the release, loaded via
  `pipload::pip_read(id = "recode_spec", format = "qs2", alias = "pip_inv")`.
  Provides variable type, recode type, and category code/label mappings.
- **Layer 2 (manual)**: `inst/extdata/tm_variable_spec.yaml`. Provides UI
  labels, roles, and stat groups for each variable.

### Endpoint → Registry Mapping

| Endpoint | Role filter | Fields returned |
|---|---|---|
| `GET /analysis-variables` | `"analysis_var" %in% roles` | `varname`, `label`, `type`, `poverty_line_slider`, `stat_groups` |
| `GET /categories` | `"filter" %in% roles` | `varname`, `label`, `subcategories [{code, label}]` |
| `GET /covariates` | `"covariate" %in% roles` | `varname`, `label`, `n_categories`, `pov_status_mutex`, `poverty_line_slider` |

`poverty_line_slider` and `pov_status_mutex` are derived by the view
accessors (not stored in the registry): `poverty_line_slider = tm_type == "poverty"`;
`pov_status_mutex = varname == "pov_status"`.

Reference JSON shapes in `analysis-var.json`, `categories.json`, and
`covariates.json` define the expected response structure. Actual category
codes and labels are authoritative from the pipdata recode spec; the reference
files define field names and envelope shape.

### A fourth endpoint (`GET /statistics`) serves a static, grouped catalogue
of available statistics and does NOT use the registry (see Out of Scope).

### Current state

- `R/categories.R` — static hardcoded `pip_tablemaker_categories()` function
  with inline subcategory constants. Outdated and not release-aware.
- `R/covariates.R` — static `pip_tablemaker_covariates()` derived from
  `pip_tablemaker_categories()`. Outdated and not release-aware.
- `R/registry.R` — empty file; wiring not yet done.
- `R/zzz.R` — `PIPTM_REGISTRY_DIR` env var is read but never used. The
  `.piptm_env$registry_dir` slot is initialised but `piptm_load_registry()`
  is never called.
- `inst/plumber/plumber.R` — `GET /categories` and `GET /covariates`
  delegate to the legacy static functions. `GET /analysis-variables` is a
  TODO stub. No release parameter on any metadata endpoint.

### Derived variables and inline categories

`age_group`, `hsize_group`, and `wquintile` are in the UI spec as
`filter`/`covariate` roles but are NOT present in the pipdata recode spec
(they are derived from `age`, `hsize`, or computed at survey load time).
Their category definitions must be embedded directly in
`tm_variable_spec.yaml` under an optional `categories` field, which
`build_variable_registry()` uses in lieu of a pipdata lookup.

### Out-of-scope flag: `/measures` endpoint

The current `GET /measures` endpoint and `pip_tablemaker_measures()` were
updated in plan `2026-06-12-replace-pip-measures-with-tablemaker-measures.md`.
However, the reference `statistics.json` suggests the endpoint structure may
have changed again (grouped by stat type, with human-readable labels per
measure). **Flag for follow-up review** — out of scope here.

### Pattern reference: manifest loading

`piptm_load_registry()` mirrors `.load_manifests()` in `R/manifest.R`
exactly: scan directory → parse JSON files → store in
`.piptm_env$registries` keyed by release ID → emit startup messages.

## Requirements

| ID  | Requirement | Source |
|-----|-------------|--------|
| R1  | `build_variable_registry(release)` merges UI spec YAML + pipdata recode spec → `registry_dir/<release>.json` | user request |
| R2  | `piptm_load_registry(registry_dir)` reads all JSONs into `.piptm_env$registries` at package load | user request |
| R3  | `piptm_variable_registry(release = NULL)` accessor; stops if release not found | user request |
| R4  | `piptm_analysis_variables(release)` returns analysis_var-role entries with field mapping | endpoint context |
| R5  | `piptm_filter_categories(release)` returns filter-role entries with subcategories | endpoint context |
| R6  | `piptm_layout_covariates(release)` returns covariate-role entries with n_categories | endpoint context |
| R7  | `GET /analysis-variables` plumber endpoint, release-aware | endpoint context |
| R8  | `GET /categories` updated to registry-backed, release-aware | endpoint context |
| R9  | `GET /covariates` updated to registry-backed, release-aware | endpoint context |
| R10 | Remove `pip_tablemaker_categories()`, `pip_tablemaker_covariates()`, all `._SUBCATS` constants | cleanup |
| R11 | `tm_variable_spec.yaml` extended with inline `categories` for `age_group`, `hsize_group`, `wquintile` | derived vars gap |
| R12 | `yaml` added to `DESCRIPTION Suggests:` | dependency |
| R13 | `pov_status` registry entry stores `n_categories = 2L` | covariates.json reference |
| R14 | `zzz.R` calls `piptm_load_registry()` when `PIPTM_REGISTRY_DIR` is set | startup wiring |

---

## Phase 1: Registry Machinery

### 1. Extend `tm_variable_spec.yaml` — inline categories for derived variables

- **Requirements**: R11
- **Files**: `inst/extdata/tm_variable_spec.yaml`
- **Details**:

  Add an optional `categories` block to `age_group`, `hsize_group`, and
  `wquintile`. Each category entry has `code` (character) and `label`
  (character), consistent with the registry entry format:

  ```yaml
  age_group:
    ui_label: "Age group"
    roles: [filter, covariate]
    stat_groups: []
    categories:
      - code: "0-14"
        label: "0 to 14"
      - code: "15-24"
        label: "15 to 24"
      - code: "25-64"
        label: "25 to 64"
      - code: "65+"
        label: "65 and above"

  hsize_group:
    ui_label: "Household size"
    roles: [filter, covariate]
    stat_groups: []
    categories:
      - code: "1"
        label: "1 person"
      - code: "2-3"
        label: "2 to 3 persons"
      - code: "4-6"
        label: "4 to 6 persons"
      - code: "7+"
        label: "7 or more persons"

  wquintile:
    ui_label: "Welfare quintile"
    roles: [filter, covariate]
    stat_groups: []
    categories:
      - code: "1"
        label: "Q1 (bottom 20%)"
      - code: "2"
        label: "Q2"
      - code: "3"
        label: "Q3"
      - code: "4"
        label: "Q4"
      - code: "5"
        label: "Q5 (top 20%)"
  ```

  `build_variable_registry()` must check for `spec_var$categories` first; if
  present (and the variable is not a special case and not in the pipdata spec),
  use those inline categories instead of warning-and-skipping.

- **Test Scenarios**:
  - ✅ `age_group` entry has 4 categories with codes `"0-14"`, `"15-24"`, etc.
  - ✅ `wquintile` entry has 5 categories
  - ✅ `hsize_group` entry has 4 categories
- **Tests**: `tests/testthat/test-registry.R`
- **Acceptance criteria**: No warning emitted for `age_group`, `hsize_group`,
  or `wquintile` when `build_variable_registry()` runs.

### 2. Implement `R/registry.R` — core functions

- **Requirements**: R1, R2, R3, R12, R13
- **Files**: `R/registry.R`, `DESCRIPTION`
- **Details**:

  **2a. Internal constants**

  ```r
  .RECODE_TO_TM_TYPE <- c(
    range_clamp              = "continuous",
    binary_map               = "binary",
    haven_labels             = "categorical",
    binned_from_continuous   = "categorical",
    quantile_from_continuous = "categorical"
  )
  .SPECIAL_CASE_VARNAMES <- c("welfare", "pov_status", "weight")
  ```

  **2b. `.mapping_to_categories(mapping)`** (internal)

  Converts a named list `list("1" = "Male", "0" = "Female")` →
  `list(list(code = "1", label = "Male"), list(code = "0", label = "Female"))`.
  Returns `NULL` if `mapping` is NULL or empty.

  **2c. `.build_registry_entry(varname, spec_var, pip_vars)`** (internal)

  Builds one entry. Dispatch path:

  1. If `varname %in% .SPECIAL_CASE_VARNAMES` → hardcode `tm_type`:
     - `welfare` → `"welfare"`, `pov_status` → `"poverty"`, `weight` → `"continuous"`
     - `n_categories`: `pov_status` → `2L`, others → `NULL`
     - `categories` → `NULL` for all special cases
  2. Else if `spec_var$categories` is non-NULL → use inline YAML categories:
     - `tm_type` → `"categorical"`
     - `categories` → use inline list (already code/label format)
     - `n_categories` → `length(spec_var$categories)`
  3. Else → look up `pip_vars[[varname]]`:
     - If NULL → `warning()` and return `NULL` (caller drops it)
     - Else → derive `tm_type` from `recode_type` via `.RECODE_TO_TM_TYPE`
       (hard `stop()` if `recode_type` unknown), extract `mapping`, call
       `.mapping_to_categories()`, set `n_categories`

  Entry structure:
  ```r
  list(
    varname      = varname,          # character
    ui_label     = spec_var$ui_label,# character
    tm_type      = tm_type,          # welfare / poverty / continuous / binary / categorical
    roles        = spec_var$roles,   # character vector
    stat_groups  = spec_var$stat_groups %||% character(0L),
    n_categories = n_cats,           # integer or NULL
    categories   = categories        # list of code/label lists, or NULL
  )
  ```

  Note: `%||%` — use `if (is.null(x)) y else x` pattern; do not rely on
  `rlang::%||%` unless already imported.

  **2d. `build_variable_registry(release, registry_dir = NULL, verbose = TRUE)`** (exported)

  Steps:
  1. Resolve `registry_dir`: argument → `dirname(.piptm_env$manifest_dir) / "variable_registry"` → hard `stop()` if neither available
  2. `dir.create(registry_dir, recursive = TRUE)` if needed
  3. Load UI spec: `yaml::read_yaml(system.file("extdata", "tm_variable_spec.yaml", package = "piptm"))` — guard with `requireNamespace("yaml", quietly = TRUE)` and hard `stop()` if missing
  4. Load pipdata spec: `pipload::pip_read(id = "recode_spec", format = "qs2", alias = "pip_inv", verbose = verbose)`
  5. Iterate over `names(spec$variables)`, call `.build_registry_entry()` for each
  6. Drop `NULL`s: `Filter(Negate(is.null), registry)`
  7. Serialise: `jsonlite::toJSON(registry, auto_unbox = FALSE, null = "null", pretty = TRUE)`
  8. Write to `file.path(registry_dir, paste0(release, ".json"))`
  9. Return registry invisibly

  **2e. `piptm_load_registry(registry_dir)`** (internal, NOT exported)

  Mirrors `.load_manifests()`:
  1. Initialise `.piptm_env$registries <- list()`
  2. If `!dir.exists(registry_dir)` → `packageStartupMessage()`, `return(invisible(NULL))`
  3. `list.files(registry_dir, pattern = "\\.json$", full.names = TRUE)`
  4. For each file: `release_id <- tools::file_path_sans_ext(basename(f))`; `tryCatch(jsonlite::read_json(f), ...)` — on error, `packageStartupMessage()` and skip
  5. Store: `.piptm_env$registries[[release_id]] <- parsed`
  6. Return `invisible(NULL)`

  **2f. `piptm_variable_registry(release = NULL)`** (exported)

  ```r
  release <- if (is.null(release)) piptm_current_release() else release
  reg <- .piptm_env$registries[[release]]
  if (is.null(reg)) {
    cli::cli_abort(
      c(
        "No variable registry found for release {.val {release}}.",
        "i" = "Available: {.val {sort(names(.piptm_env$registries))}}",
        "i" = "Run {.fn build_variable_registry} to create it."
      )
    )
  }
  reg
  ```

  **2g. Add `yaml` to `DESCRIPTION Suggests:`** (R12)

- **Test Scenarios**:
  - ✅ Special case `pov_status` has `tm_type = "poverty"`, `n_categories = 2L`, `categories = NULL`
  - ✅ Special case `welfare` has `tm_type = "welfare"`, `n_categories = NULL`
  - ✅ `haven_labels` variable → `tm_type = "categorical"`, categories derived from mapping
  - ✅ `binary_map` variable → `tm_type = "binary"`, 2 categories
  - ✅ `range_clamp` variable → `tm_type = "continuous"`, `categories = NULL`, `n_categories = NULL`
  - ✅ Inline-categories variable (`age_group`) → `tm_type = "categorical"`, 4 categories, no warning
  - 🛑 Unknown `recode_type` → `stop()` with message mentioning variable name
  - 🛑 `piptm_variable_registry("nonexistent")` → `cli_abort()` with available releases
  - ⚠️ Standard variable missing from pipdata spec → `warning()`, entry is NULL, dropped
- **Tests**: `tests/testthat/test-registry.R`
- **Acceptance criteria**: All unit test scenarios pass. `build_variable_registry()` writes a valid JSON with one entry per non-skipped variable. `piptm_variable_registry()` returns a named list with correct entry structure.

### 3. Wire `zzz.R` — registry loading on package load

- **Requirements**: R14
- **Files**: `R/zzz.R`
- **Details**:

  In `.onLoad()`, after the manifest loading block, add a parallel block for
  the registry. Follow the exact same guard pattern used for manifests:

  ```r
  # --- Variable registry -------------------------------------------------------
  # Only load if the directory is configured and exists.  Missing directory is
  # not an error — registries are produced manually via build_variable_registry()
  # and may not exist on a fresh install or CI runner.
  if (nzchar(registry_dir_opt) && dir.exists(registry_dir_opt)) {
    .piptm_env$registry_dir <- registry_dir_opt
    tryCatch(
      piptm_load_registry(registry_dir_opt),
      error = function(e) {
        packageStartupMessage(
          "[piptm] Failed to load variable registries from: ", registry_dir_opt,
          "\n  ", conditionMessage(e)
        )
      }
    )
  }
  ```

  Also initialise the `registries` slot at the top of `.onLoad()` alongside
  the other slot initialisations:
  ```r
  .piptm_env$registries <- list()
  ```

- **Test Scenarios**:
  - ✅ After `devtools::load_all()` with `PIPTM_REGISTRY_DIR` pointing to a
    directory containing one registry JSON, `names(.piptm_env$registries)`
    is non-empty
  - ✅ With `PIPTM_REGISTRY_DIR` unset, package loads silently with
    `.piptm_env$registries` as empty list
- **Tests**: `tests/testthat/test-registry.R` (use `withr::local_envvar()`)
- **Acceptance criteria**: `devtools::load_all()` with a valid registry dir
  populates `.piptm_env$registries`. No error when dir is absent.

---

## Phase 2: API Layer, Cleanup, and Tests

### 4. Add view-layer accessor functions

- **Requirements**: R4, R5, R6
- **Files**: `R/registry.R` (add to existing file)
- **Details**:

  These functions retrieve the registry, filter by role, and reshape fields
  to match the reference JSON shapes. They are the bridge between the internal
  registry structure and the API response format.

  Field mapping (registry → API):
  - `ui_label` → `label`
  - `tm_type` → `type`
  - `poverty_line_slider` derived: `tm_type %in% c("poverty")` → `TRUE`
  - `pov_status_mutex` derived: `varname == "pov_status"` → `TRUE`

  **4a. `piptm_analysis_variables(release = NULL)`** (exported, R4)

  Filter: `"analysis_var" %in% entry$roles`
  Returns list of:
  ```r
  list(
    varname             = entry$varname,
    label               = entry$ui_label,
    type                = entry$tm_type,
    poverty_line_slider = entry$tm_type == "poverty",
    stat_groups         = entry$stat_groups
  )
  ```

  **4b. `piptm_filter_categories(release = NULL)`** (exported, R5)

  Filter: `"filter" %in% entry$roles`
  Returns list of:
  ```r
  list(
    varname        = entry$varname,
    label          = entry$ui_label,
    subcategories  = entry$categories   # already [{code, label}] format
  )
  ```

  **4c. `piptm_layout_covariates(release = NULL)`** (exported, R6)

  Filter: `"covariate" %in% entry$roles`
  Returns list of:
  ```r
  list(
    varname             = entry$varname,
    label               = entry$ui_label,
    n_categories        = entry$n_categories,
    pov_status_mutex    = entry$varname == "pov_status",
    poverty_line_slider = entry$varname == "pov_status"
  )
  ```

- **Test Scenarios**:
  - ✅ `piptm_analysis_variables()` contains `welfare`, `pov_status`, `age`, `educy`, `hsize`, `weight`, `electricity`, `imp_san_rec`, `imp_wat_rec`
  - ✅ `pov_status` has `poverty_line_slider = TRUE` in analysis vars; all others `FALSE`
  - ✅ `piptm_filter_categories()` contains `gender`, `area`, `age_group`, `wquintile`, etc. (filter-role vars only)
  - ✅ Every entry in `piptm_filter_categories()` has a `subcategories` list (not NULL)
  - ✅ `piptm_layout_covariates()` contains `pov_status`, `gender`, `area`, `age_group`, etc.
  - ✅ Only `pov_status` has `pov_status_mutex = TRUE` in covariates
  - ✅ `welfare`, `educy`, `age` (analysis_var-only) are NOT in `piptm_filter_categories()` or `piptm_layout_covariates()`
- **Tests**: `tests/testthat/test-registry.R`
- **Acceptance criteria**: All role-filtered accessor tests pass. Field names match reference JSON shapes exactly.

### 5. Update `inst/plumber/plumber.R` — registry-backed endpoints

- **Requirements**: R7, R8, R9
- **Files**: `inst/plumber/plumber.R`
- **Details**:

  **5a. Add `GET /analysis-variables`** (R7)

  Replace the existing TODO stub:
  ```r
  #* Return the catalogue of analysis variables for Decision 2
  #*
  #* Filters the variable registry to entries with role "analysis_var" and
  #* returns varname, label, type, poverty_line_slider, and stat_groups for
  #* each. The UI uses this to populate the analysis variable dropdown and
  #* to know which statistics are valid for each variable.
  #*
  #* @param release:character Release ID (optional; defaults to current)
  #* @serializer json list(na = "null")
  #* @get /analysis-variables
  function(release = NULL, res) {
    out <- capture_with_warnings({
      rel  <- resolve_release(release)
      data <- piptm::piptm_analysis_variables(rel)
      list(data = data, rel = rel)
    })
    if (!is.null(out$error)) return(api_error(out$error, 422L, res))
    api_response(out$result$data, warnings = out$warnings,
                 meta = list(release = out$result$rel))
  }
  ```

  **5b. Replace `GET /categories`** (R8)

  Remove the static `pip_tablemaker_categories()` call. Replace with:
  ```r
  function(release = NULL, res) {
    out <- capture_with_warnings({
      rel  <- resolve_release(release)
      data <- piptm::piptm_filter_categories(rel)
      list(data = data, rel = rel)
    })
    if (!is.null(out$error)) return(api_error(out$error, 422L, res))
    api_response(out$result$data, warnings = out$warnings,
                 meta = list(release = out$result$rel))
  }
  ```

  **5c. Replace `GET /covariates`** (R9)

  Remove the static `pip_tablemaker_covariates()` call. Replace with:
  ```r
  function(release = NULL, res) {
    out <- capture_with_warnings({
      rel  <- resolve_release(release)
      data <- piptm::piptm_layout_covariates(rel)
      list(data = data, rel = rel)
    })
    if (!is.null(out$error)) return(api_error(out$error, 422L, res))
    api_response(out$result$data, warnings = out$warnings,
                 meta = list(release = out$result$rel))
  }
  ```

  Update the router comment header to include `/analysis-variables` in the
  endpoint list.

- **Test Scenarios**:
  - ✅ `GET /analysis-variables` returns success envelope with `pov_status`
    having `poverty_line_slider = true`
  - ✅ `GET /categories` returns success envelope with `subcategories` field
    per entry
  - ✅ `GET /covariates` returns success envelope with `pov_status` having
    `pov_status_mutex = true`
  - ✅ All three endpoints accept `release` param; default to current release
  - ❌ Invalid `release` → 422
- **Tests**: `tests/testthat/test-api-endpoints.R` (add cases)
- **Acceptance criteria**: All three endpoints return the correct envelope
  shape. Static function references removed. Release parameter plumbed through.

### 6. Remove legacy static code; update `DESCRIPTION` and `NAMESPACE`

- **Requirements**: R10, R12
- **Files**: `R/categories.R`, `R/covariates.R`, `DESCRIPTION`, `NAMESPACE`
- **Details**:

  **6a. `R/categories.R`**: Remove the body of `pip_tablemaker_categories()`
  and all `._SUBCATS` internal constants. Replace file content with a
  tombstone comment:
  ```r
  # categories.R — REMOVED
  #
  # pip_tablemaker_categories() and all ._SUBCATS constants were removed
  # in favour of the variable registry. See R/registry.R and
  # piptm_filter_categories().
  #
  # Plan: .cg-docs/plans/2026-06-23-variable-registry.md (Step 6)
  NULL
  ```

  **6b. `R/covariates.R`**: Same treatment — remove
  `pip_tablemaker_covariates()`. Tombstone comment referencing the plan.

  **6c. `DESCRIPTION`**: Add `yaml` to `Suggests:`.

  **6d. `NAMESPACE`**: Regenerate with `devtools::document()` after
  adding `@export` tags to `piptm_analysis_variables`,
  `piptm_filter_categories`, `piptm_layout_covariates`, and
  `build_variable_registry`, and removing the old exports.

- **Test Scenarios**:
  - ✅ `devtools::check()` reports no undefined export warnings
  - 🛑 `piptm::pip_tablemaker_categories()` errors with "could not find function"
  - 🛑 `piptm::pip_tablemaker_covariates()` errors with "could not find function"
- **Tests**: `devtools::check()` clean run
- **Acceptance criteria**: NAMESPACE exports `piptm_analysis_variables`,
  `piptm_filter_categories`, `piptm_layout_covariates`,
  `build_variable_registry`, `piptm_variable_registry`. Does NOT export
  `pip_tablemaker_categories`, `pip_tablemaker_covariates`.

### 7. Tests — `tests/testthat/test-registry.R`

- **Requirements**: R1–R9, R11, R13
- **Files**: `tests/testthat/test-registry.R` (new)
- **Details**:

  Use a synthetic fixture: a minimal `tm_variable_spec.yaml`-like list in
  memory and a synthetic `pip_vars` list to test `.build_registry_entry()`
  without a live pipdata connection.

  For load/accessor tests: write a fixture registry JSON to a temp dir using
  `withr::local_tempdir()`, call `piptm_load_registry()` directly, then test
  `piptm_variable_registry()` and the three view accessors.

  **Test groups:**

  1. `.mapping_to_categories()`: NULL input → NULL; empty list → NULL;
     named list → correct code/label pairs.
  2. `.build_registry_entry()` — special cases: welfare, pov_status (n_categories=2),
     weight.
  3. `.build_registry_entry()` — standard vars: range_clamp, binary_map,
     haven_labels.
  4. `.build_registry_entry()` — inline YAML categories: age_group, wquintile.
  5. `.build_registry_entry()` — missing from pipdata → warning + NULL return.
  6. `.build_registry_entry()` — unknown recode_type → stop().
  7. `piptm_load_registry()`: empty dir → silent; JSON in dir →
     `.piptm_env$registries` populated; malformed JSON → skipped.
  8. `piptm_variable_registry()`: valid release → list; invalid release →
     `cli_abort()`.
  9. `piptm_analysis_variables()`: correct filter (analysis_var role);
     `poverty_line_slider` correct for pov_status vs. others.
  10. `piptm_filter_categories()`: correct filter (filter role); every entry
      has non-NULL subcategories.
  11. `piptm_layout_covariates()`: correct filter (covariate role);
      `pov_status_mutex` only TRUE for pov_status; `n_categories` values
      correct; welfare/educy absent.

  Also extend `tests/testthat/test-api-endpoints.R` with 5 cases covering
  the three new/updated endpoints.

- **Test Scenarios**: All groups above (≥30 test cases)
- **Tests**: `tests/testthat/test-registry.R`
- **Acceptance criteria**: All tests pass. `devtools::test()` zero failures.
  Pre-existing `test-categories.R` tests removed or updated (the tested
  function no longer exists; tests should verify the function is gone).

---

## Testing Strategy

- **Unit tests** (Step 7): Synthetic fixtures; no live pipdata or Arrow
  connection required. Test internal helpers and accessors in isolation.
- **Integration** (Step 5 / Step 7 extension): Hit plumber endpoints
  programmatically against a fixture registry loaded via `withr::local_envvar()`.
- **Manual smoke test**: `devtools::load_all()` → `build_variable_registry("20260401_TEST")` → `piptm_filter_categories()` → inspect output.

## Documentation Checklist

- [ ] roxygen2 for `build_variable_registry()` — parameters, return value, side effect (writes JSON), when to call
- [ ] roxygen2 for `piptm_variable_registry()` — return structure, error condition
- [ ] roxygen2 for `piptm_analysis_variables()`, `piptm_filter_categories()`, `piptm_layout_covariates()` — reference response field names
- [ ] `zzz.R` header comment: add `PIPTM_REGISTRY_DIR` to the env-var documentation block
- [ ] Update `run_api()` docstring endpoint table to include `/analysis-variables`

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Derived variables (`age_group`, `hsize_group`, `wquintile`) not in pipdata recode spec | Registry build warns-and-skips them, leaving gaps in `/categories` | Step 1 adds inline YAML categories; inline branch in `.build_registry_entry()` handles them before the pipdata lookup |
| `test-categories.R` tests reference `pip_tablemaker_categories()` (now deleted) | Test suite fails after Step 6 | Step 7 explicitly removes/rewrites those tests |
| `jsonlite::read_json()` vs `fromJSON()` — `read_json()` preserves list structure but returns character scalars as length-1 character vectors | Registry loaded from disk differs from registry returned by `build_variable_registry()` | Use `simplifyVector = FALSE` consistently; validate accessor round-trip in tests |
| `pov_status` `n_categories` stored as NULL if not hardcoded | `/covariates` shows NULL for pov_status category count; UI breaks | C9 / Step 2c hardcodes `n_categories = 2L` for pov_status |
| Binary variables (`gender`, `area`, `imp_wat_rec`) codes in pipdata are `"1"/"0"` or `"male"/"female"` strings — reference `categories.json` shows integer codes `1`/`2` | Response shape mismatch vs reference JSON | Codes are authoritative from pipdata; reference JSON defines structure not values. Document discrepancy in accessor roxygen. |

## Out of Scope

- `GET /statistics` — static grouped catalogue (reference: `statistics.json`),
  no registry dependency; separate implementation
- Review/update of `GET /measures` / `pip_tablemaker_measures()` vs
  `statistics.json` — **flagged for follow-up**
- Derived binary analysis variables seen in `analysis-var.json` reference
  (`female`, `male`, `urban`, `rural`, `youth`, `elderly`, `paid_emp`,
  `inf_emp`, `employer`) — require new `tm_variable_spec.yaml` entries and
  possibly new pipdata derived columns; separate scope
- UI implementation
- `table_maker()` computation logic changes
- `pipdata` package changes

---

## Completion Contract

### Outcome

`build_variable_registry(release)` produces a release-specific JSON registry
from the UI spec YAML and pipdata recode spec. On package load,
`.onLoad()` populates `.piptm_env$registries`. Three plumber endpoints
(`GET /analysis-variables`, `GET /categories`, `GET /covariates`) are
registry-backed, release-aware, and return responses matching the reference
JSON shapes. All legacy static functions are removed.

### Verification Surface

| ID | Evidence Required | Command / Artifact | Required |
|----|-------------------|--------------------|----------|
| V1 | `build_variable_registry()` creates valid JSON | `file.exists(file.path(registry_dir, paste0(release, ".json")))` | yes |
| V2 | `piptm_load_registry()` populates `.piptm_env$registries` | `length(names(.piptm_env$registries)) > 0` after load | yes |
| V3 | Each entry has 7 canonical fields | unit test on `names(entry)` | yes |
| V4 | `tm_type` derivation correct for all 5 recode types | unit tests: continuous / binary / categorical / welfare / poverty | yes |
| V5 | Special cases (welfare, pov_status, weight) hardcoded | unit test | yes |
| V6 | `pov_status` has `n_categories = 2L` (not NULL) | unit test | yes |
| V7 | `piptm_analysis_variables()` returns `{varname, label, type, poverty_line_slider, stat_groups}` | unit test + field name check | yes |
| V8 | `piptm_filter_categories()` returns `{varname, label, subcategories: [{code, label}]}` | unit test + field name check | yes |
| V9 | `piptm_layout_covariates()` returns `{varname, label, n_categories, pov_status_mutex, poverty_line_slider}` | unit test + field name check | yes |
| V10 | `GET /analysis-variables` returns success envelope | integration test | yes |
| V11 | `GET /categories` returns success envelope with `subcategories` field | integration test | yes |
| V12 | `GET /covariates` returns success envelope with `pov_status_mutex` field | integration test | yes |
| V13 | `piptm_variable_registry("nonexistent")` stops with informative error | `expect_error(…, "No variable registry found")` | yes |
| V14 | `pip_tablemaker_categories()` and `pip_tablemaker_covariates()` removed | `devtools::check()` clean | yes |
| V15 | All pre-existing tests pass | `devtools::test()` zero failures | yes |

### Constraints

| ID | Constraint | Check |
|----|------------|-------|
| C1 | `piptm_load_registry()` mirrors `.load_manifests()` pattern exactly | code review |
| C2 | `piptm_load_registry()` uses `jsonlite::read_json()` (not `fromJSON` default) | code review |
| C3 | `build_variable_registry()` serialises with `auto_unbox = FALSE, null = "null", pretty = TRUE` | code review |
| C4 | `yaml` added to `DESCRIPTION Suggests:` | DESCRIPTION |
| C5 | No bare `%||%` — use `if (is.null(x)) y else x` | code review |
| C6 | `piptm_load_registry()` NOT exported | NAMESPACE |
| C7 | All legacy static functions and `._SUBCATS` constants removed | NAMESPACE + file |
| C8 | Accessor field mapping: `ui_label` → `label`, `tm_type` → `type`; derived fields `poverty_line_slider` and `pov_status_mutex` added by accessors (not stored in registry) | code review |
| C9 | `pov_status` registry entry stores `n_categories = 2L` at build time | unit test V6 |

### Boundaries

- **Allowed**: Remove `pip_tablemaker_categories()`, `pip_tablemaker_covariates()`, all `._SUBCATS` constants
- **Allowed**: Add `piptm_analysis_variables()`, `piptm_filter_categories()`, `piptm_layout_covariates()`, `build_variable_registry()`, `piptm_variable_registry()` as exported
- **Allowed**: Add `GET /analysis-variables`; update `GET /categories` and `GET /covariates` to be release-aware and registry-backed
- **Allowed**: Extend `tm_variable_spec.yaml` with inline `categories` for derived vars
- **Out of scope**: `GET /statistics` endpoint — separate implementation
- **Out of scope**: `/measures` endpoint review vs `statistics.json` — follow-up
- **Out of scope**: Derived binary analysis variables from reference JSON (`female`, `male`, `urban`, etc.)
- **Out of scope**: UI implementation, `table_maker()` logic, `pipdata` changes

### Iteration Policy

1. Variable in UI spec, missing from pipdata spec, no inline YAML categories → `warning()` and skip; partial registry produced
2. Unknown `recode_type` in pipdata spec → hard `stop()` in `build_variable_registry()`
3. `registry_dir` missing at startup → `packageStartupMessage()`, silent continue
4. Malformed registry JSON → per-file `packageStartupMessage()`, skip, continue loading
5. `piptm_variable_registry()` for missing release → `cli_abort()` with informative message and available releases

### Blocked-Stop Conditions

- `tm_variable_spec.yaml` not found → `stop()` in `build_variable_registry()`
- Unknown `recode_type` not in `.RECODE_TO_TM_TYPE` → `stop()` in `build_variable_registry()`
