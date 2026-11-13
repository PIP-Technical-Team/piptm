---
date: 2026-11-12
depth: architecture
type: standard
plan: .cg-docs/plans/2026-11-12-metadata-rework-and-table-maker-decoupling.md
findings:
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: skipped
  P2.7: fixed
  P2.8: fixed
  P2.9: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
  P3.5: skipped
  P3.6: fixed
  P3.7: fixed
  P3.8: fixed
---

## Review Report

**Review mode**: architecture (explicit user mode; mandatory emphasis on `@cg-architecture`, `@cg-performance`, `@cg-testing`)
**Scope**: `git diff 8ddde07 -- . ':!.cg-docs'` — full metadata-rework plan (`.cg-docs/plans/2026-11-12-metadata-rework-and-table-maker-decoupling.md`, all 3 phases, committed `4ee5758` + uncommitted Phase 3 docs). 56 files, ~2,350 insertions / 5,630 deletions.
**Findings**: 20 (P0: 0, P1: 3, P2: 9, P3: 8)

**Brain findings applied/verified**:
1. `.build_filters_content()` uses the safe `[`-based lookup with raw-code fallback (never unsafe `[[` on an atomic `setNames` vector) — confirmed fixed by `@cg-architecture` and `@cg-data-quality`. (`.cg-docs/solutions/bugs/2026-10-13-hsize-group-filter-category-codes.md`)
2. `include_metadata` is fully purged from the hand-patched OpenAPI spec machinery (`summary_by_path`, `description_by_path`, `required_by_path`, `enrich_param`, `requestBody`) — confirmed by `@cg-architecture`. (`.cg-docs/solutions/bugs/2026-08-31-swagger-req-parameter-exposed.md`)
3. ANSI-escape warning-capture concern is moot — `table_maker()` no longer captures runtime warnings into metadata. (`.cg-docs/solutions/bugs/2026-09-14-ansi-escape-codes-in-description-warnings.md`)

### P0 — BLOCKING (immediate remediation required)
None.

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-testing] `tests/testthat/test-description-builder.R:379-403` — Mock-based "never reads microdata" guard test is vacuous.
  **Why**: The test mocks `load_surveys`/`load_survey_microdata` via `local_mocked_bindings()` and asserts `expect_no_error()`, but `build_description_model()` never calls either function by name — a grep for both names in `R/description_builder.R` returns zero matches. The mocks are never invoked, so the test would pass even if the builder secretly read microdata through a different path (a renamed loader, `piptm_survey_data()`, or direct `arrow::open_dataset()`). The companion static-grep test immediately below it is the one actually doing useful guard work.
  **Fix**: Mock the actual I/O boundary the registry/manifest accessors could plausibly cross, or delete the mock-based test since the grep test already covers this guarantee at the source level.
  **Status**: **Applied** (`[safe_auto]` reclassified to a direct fix) — the mock-based test was removed from `tests/testthat/test-description-builder.R`; the static source-scan test (already present) remains as the sole, meaningful zero-microdata guard. Verified: `test-description-builder.R` PASS=63, FAIL=0 after removal.

- **[P1.2]** [cg-version-control] Uncommitted Phase 3 changeset mixes docs, roxygen/`man/` sync, and internal bookkeeping in one working tree.
  **Why**: The pending diff bundles (1) user-facing docs (`NEWS.md`, `README.md`, `SWAGGER_GUIDE.md`, `docs/api-contract.md`, `docs/metadata-endpoint-spec.md`), (2) source + `man/` sync (`R/api.R`, `R/description_renderer.R`, `R/description_renderer_html.R`, regenerated `.Rd`), and (3) `.cg-docs/`/`roadmap.json` bookkeeping. If `R/api.R` contains functional changes rather than only roxygen edits, bundling it with pure docs obscures a behavior change in the commit history.
  **Fix**: Run `git diff -- R/api.R` before committing. If doc-only, one `docs(metadata): finalize Phase 3 documentation and roadmap bookkeeping` commit is fine. If it contains logic changes, split into a `fix`/`feat` commit (with its `.Rd`) plus a separate docs commit.
  **Status**: `[manual]` — needs user decision before the Phase 3 commit; not auto-applied.

- **[P1.3]** [cg-data-quality] `R/description_builder.R` `.build_layout_content()` — `pov_status` layout-slot poverty-line validation is silently skipped rather than enforced, unlike `table_maker()`.
  **Why**: `table_maker()` hard-aborts (`table_maker.R`) if `pov_status` is in `by` without a valid `poverty_line`. `.build_layout_content()` instead does `if (length(pl) == 0L) next` — it silently omits that row from the rendered layout table instead of erroring. `build_description_model()` is `@export`ed with direct-call `@examples`, so any caller that bypasses the Plumber route (which separately enforces this via `.validate_table_shape()`) gets a metadata document that misrepresents the requested table shape instead of an error.
  **Fix**: Move the `pov_status`/`poverty_line` coherence check into `.validate_and_normalize_table_params()` (`R/table_params.R`) so both `table_maker()` and `build_description_model()` enforce it identically regardless of caller, per the plan's own "single source of truth" design intent (R1/C2).
  **Status**: `[manual]` — touches the shared validation contract; needs user approval before changing `R/table_params.R`'s public behavior.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-performance] `R/description_builder.R` + `R/registry.R` — Registry is independently re-resolved and fully re-scanned by each content builder.
  **Why**: `build_description_model()` fetches `piptm_variable_registry(resolved_release)` once, but `.build_filters_content()`, `.build_layout_content()`, and `.build_statistics_content()` each separately call `piptm_filter_categories()`/`piptm_layout_covariates()`/`piptm_stat_groups()`, each of which internally re-resolves the release and re-scans the full registry. A single `/metadata` request performs 4-5 independent full registry scans instead of 1, on a path explicitly designed to resolve in `<5ms`.
  **Fix**: Factor `piptm_registry.R`'s `role %in% entry$roles` filtering into small pure helpers that accept an already-loaded registry object, and pass the single fetched `registry` into each `.build_*_content()` helper instead of re-fetching inside each one.
  **Status**: `[manual]` — requires changing exported `R/registry.R` accessor signatures; out of this review's safe-fix scope.

- **[P2.2]** [cg-testing] `tests/testthat/test-table-params.R` — The `table_maker()` ↔ `.partition_surveys_by_dimensions()` agreement test only compares `pip_id` set membership, not full partition semantics.
  **Why**: The test does call the real `table_maker()` and compare against the internal partition helper (genuinely load-bearing, not just internal self-consistency), but it doesn't test `filter_base` + `by` combined, nor exclusion-reason agreement, nor the "all excluded" case jointly.
  **Fix**: Add a combined `filter_base` + `by` agreement case and assert on exclusion reasons if `table_maker()` surfaces them.
  **Status**: `[advisory]` — not applied; additional test coverage, not a defect.

- **[P2.3]** [cg-testing] `R/description_builder.R` `build_description_model()` — Several specifically-requested edge cases are untested at the function level (only reachable/tested via the API layer).
  **Why**: Empty `filter_base = list()` (vs `NULL`), a registry present for the release but missing a specific `pip_id`'s entry (vs no registry at all), and gapped/duplicate layout slots are tested at `test-api-metadata.R`'s HTTP layer but not directly against `build_description_model()`. A direct caller bypassing the Plumber route would not be covered by these tests.
  **Fix**: Add direct unit tests calling `build_description_model()` with these three input shapes.
  **Status**: `[manual]` — test-writing, not auto-applied this pass.

- **[P2.4]** [cg-testing] Deleted `tests/testthat/test-description-builder-schema-validation.R` — some defensive-coding behaviors (registry-failure fallback for layout labels, NA `n_categories` handling, zero-row `excluded`/non-zero `included` combination) aren't clearly re-covered under the new function signatures.
  **Fix**: Add direct unit tests against the new `.build_layout_content()`/`.build_surveys_content()` signatures for these specific scenarios.
  **Status**: `[manual]`.

- **[P2.5]** [cg-documentation] `docs/metadata-endpoint-spec.md` — The top-level "Historical/Superseded" note is accurate, but the 900+ lines of superseded pseudocode below it (old `.build_description_metadata()`, `build_cell_definition()`, 9-section schema, `include_metadata` parameter) remain detailed and prescriptive, with no per-section reiteration of supersession.
  **Fix**: Add a short inline `> SUPERSEDED` marker at the start of each major section (`## EXECUTIVE SUMMARY`, `## Architecture`, `## Implementation`, etc.) so a reader jumping to a mid-file section via search doesn't miss the top banner.
  **Status**: `[manual]` — editorial judgment call on a large historical doc; deferred to user.

- **[P2.6]** [cg-version-control] Commit `4ee5758` spans both Phase 1 (decoupling) and Phase 2 (route unification) in one 51-file commit.
  **Fix**: For future multi-phase work, commit per phase so each is independently revertible/bisectable. Not worth splitting retroactively.
  **Status**: `[advisory]`.

- **[P2.7]** [cg-reproducibility] `R/description_builder.R` `provenance$generated_at` — The one genuinely non-deterministic field (a UTC timestamp) isn't documented as the sole field callers must exclude from equality/diff/cache-key comparisons.
  **Fix**: Document in `build_description_model()`'s `@return` that `provenance$generated_at` is the only run-to-run-varying field for identical inputs; consider exposing a stable fingerprint hash of the other 5 sections for programmatic cache keys.
  **Status**: `[advisory]`.

- **[P2.8]** [cg-data-quality] `R/description_builder.R` `build_description_model()` — No type/arity validation on `column`/`row`/`superColumn`/`superRow` (documented as "character scalar... or NULL" but unchecked).
  **Why**: A direct caller passing a vector (e.g. `column = c("gender","area")`) is silently flattened into two `by` dimensions under one slot — the Plumber route's `.validate_table_shape()` would reject this, but `build_description_model()` itself does not.
  **Fix**: Enforce the same scalar/NA/blank checks inside `.validate_and_normalize_table_params()` so the exported function enforces its own documented contract independent of the HTTP layer.
  **Status**: `[manual]`.

- **[P2.9]** [cg-data-quality] `R/description_builder.R` / `R/table_params.R` — No duplicate-slot-value guard outside the Plumber layer.
  **Why**: `.validate_table_shape()` rejects assigning the same variable to multiple slots; `build_description_model()`/`.validate_and_normalize_table_params()` do not. A direct call with `column = "gender", superColumn = "gender"` silently produces `by = c("gender","gender")`, which changes `.partition_surveys_by_dimensions()`'s overlap-count arithmetic and renders a duplicated row in the layout table.
  **Fix**: Add a duplicate check (dedupe or abort) inside `.validate_and_normalize_table_params()`.
  **Status**: `[manual]`.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-performance] `R/description_builder.R` `.build_filters_content()` / `.build_layout_content()` — `Filter()`-per-iteration O(n) lookup instead of O(1) name-indexed map.
  **Fix**: Build a name-indexed lookup once before the loop.
  **Status**: **Applied** (`[safe_auto]`) — both functions now build `cats_by_var`/`covs_by_var` via `stats::setNames()` once, then do `[[varname]]` lookups. Verified: `test-description-builder.R` PASS=63, FAIL=0.

- **[P3.2]** [cg-performance] `R/description_builder.R:1` — Unused `@importFrom data.table copy fifelse` after the rewrite (old call sites removed).
  **Fix**: Drop `copy`/`fifelse` from the import line; regenerate `NAMESPACE`.
  **Status**: **Applied** (`[safe_auto]`) — import line trimmed to `data.table, setorder, as.data.table`; `devtools::document()` regenerated `NAMESPACE` cleanly.

- **[P3.3]** [cg-code-quality] `inst/plumber/plumber.R:16` — Stale file-header endpoint inventory still says `POST /metadata` while the route (and its own in-file comment at the implementation) is `GET|POST /metadata`.
  **Fix**: Update the header line.
  **Status**: **Applied** (`[safe_auto]`) — header now reads `GET|POST /metadata`.

- **[P3.4]** [cg-documentation] `R/table_params.R` — `.validate_and_normalize_table_params()`/`.partition_surveys_by_dimensions()` have complete `@param`/`@return`/`@keywords internal` but no `@examples`.
  **Fix**: Optional `@examples \dontrun{}` or an `@details` cross-reference to `tests/testthat/test-table-params.R` as the executable spec.
  **Status**: `[advisory]` — not required since both are `@keywords internal`, not exported.

- **[P3.5]** [cg-version-control] Branch `step3_metadata` doesn't follow the repo's emerging `type/scope` convention (`feat/`, `refactor/`).
  **Fix**: Prefer `refactor/metadata-table-maker-decoupling` for future branches; not worth renaming a pushed shared branch now.
  **Status**: `[advisory]`.

- **[P3.6]** [cg-reproducibility] `R/description_builder.R` `.build_surveys_content()` — `excluded_list` wasn't sorted by `pip_id` for symmetry with the already-sorted `loaded_list`.
  **Fix**: Add `data.table::setorder(excluded_list, pip_id)`.
  **Status**: **Applied** (`[safe_auto]`) — `excluded_list` is now sorted by `pip_id` when non-empty. Verified: `test-description-builder.R` PASS=63, FAIL=0.

- **[P3.7]** [cg-data-quality] `R/description_builder.R` `.build_layout_content()` — `n_categories` column shape is inconsistent across rows (sometimes `"N: label1, label2"`, sometimes a bare count string, sometimes a hardcoded `pov_status` string).
  **Fix**: Standardize to one consistent `"N: labels"` shape, synthesizing generic labels when real ones are unavailable.
  **Status**: `[advisory]`.

- **[P3.8]** [cg-data-quality] `R/description_builder.R` `.build_statistics_content()` — `poverty_line$value` is always a formatted display string, never a raw numeric, so programmatic consumers can't easily extract the numeric poverty line.
  **Fix**: Document the two differing display conventions (`provenance` vs `statistics_selected`) in `@return`, or add a raw numeric field alongside the formatted string.
  **Status**: `[advisory]`.

### ✅ Passed
- **cg-architecture**: No P0–P2 structural/modularity/dependency issues. Confirmed `table_params.R` is a clean, dependency-neutral shared module consumed by both `table_maker()` and `build_description_model()` with correct dependency direction (diamond shape, no cross-calls between `table_maker.R` and `description_builder.R`). Confirmed `/table` and `/metadata` share `validate_table_input()` with no duplicated parsing logic.
- **cg-performance**: Core <5ms claim verified — `build_description_model()` reaches zero `load_surveys()`/`load_survey_microdata()`/`arrow::` calls. No regression from removing `table_maker()`'s `withCallingHandlers()` wrapper (the default path never paid that cost). No problematic `data.table` copies or `rbindlist` patterns.
- **cg-testing**: GET/POST `/metadata` equivalence test (`test-api-metadata.R`) does a real byte-identical comparison after stripping only the timestamp line — not a shallow 200-check. Deleted `test-cell-definition.R`, `test-table-maker-metadata.R` confirmed as covering fully-retired code with no live coverage loss. Test independence/testthat-3 conventions: no violations.
- **cg-code-quality**: No debug code, dead/unreachable code, naming inconsistencies, DRY violations, secrets, or TODO/FIXME/HACK/XXX found. `.Rbuildignore` already excludes `.cg-docs/`. roxygen `@param`/`@return` complete for all new/rewritten public surface.
- **cg-documentation**: `docs/api-contract.md`, `SWAGGER_GUIDE.md`, `NEWS.md`, `README.md`, `R/api.R` roxygen, and `R/description_renderer*.R` `@examples` all verified accurate and internally consistent with the new contract; no dangling references to retired concepts in live files.
- **cg-reproducibility**: No locale-dependent formatting beyond the flagged timestamp; no new dependency gaps; no hardcoded absolute paths; no RNG/seed concerns. Shared-helper extraction is a net reproducibility improvement (eliminates prior divergence risk between compute and metadata paths).
- **cg-data-quality**: No validation silently dropped at the HTTP/Plumber layer (`/metadata` uses the same `validate_table_input()` as `/table`). Unknown measures still abort via `.classify_measures()`; missing stat-group entries degrade gracefully. Type coercions in `.build_surveys_content()` match `table_maker()`'s own coercions.

### ⚠️ Incomplete Reviews
None — all 8 dispatched agents returned usable output.
