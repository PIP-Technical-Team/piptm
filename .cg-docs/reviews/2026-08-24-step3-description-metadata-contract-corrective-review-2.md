---
date: 2026-08-25
depth: full
type: standard
plan: .cg-docs/plans/2026-08-24-step3-description-metadata-contract-corrective.md
findings:
  P0.1: open
  P0.2: open
  P1.1: open
  P1.2: open
  P1.3: open
  P1.4: open
  P1.5: open
  P1.6: open
  P2.1: open
  P2.2: open
  P2.3: open
  P2.4: open
  P2.5: open
  P2.6: open
  P2.7: open
  P2.8: open
  P2.9: open
  P2.10: open
  P3.1: fixed
---

# Step 3 Description Feature: Technical Debrief

## Review Report

**Review mode**: full
**Scope**: Repository implementation review; the working tree had no uncommitted diff. The feature was traced from the current source and its completed corrective plan.
**Files reviewed**: 10 primary implementation, API, documentation, and test files, plus loader/registry contracts and project context.
**Findings**: 19 (P0: 2, P1: 6, P2: 10, P3: 1)

**Auto-routing applied**: The project configuration requests thorough review. The feature is statistical and reproducibility-sensitive, with an API/module boundary, so data-quality and architecture concerns received mandatory emphasis.

## 1. Architecture Overview

The feature is a sidecar metadata and rendering layer around the existing computation engine. The normal table path remains a `data.table`; callers opt into metadata with `table_maker(with_meta = TRUE)`.

```text
R caller or GET /description
        |
        v
table_maker(..., with_meta = TRUE)
        |
        +-- validate request and classify measures
        +-- resolve release and manifest entries
        +-- pre-exclude surveys by manifest/filter/dimension availability
        +-- load Arrow/Parquet survey data through load_surveys()
        +-- compute measures with compute_measures()
        +-- apply population-share suppression
        +-- attach survey metadata and build execution sidecar
        |
        v
table result: data + specification + execution + provenance + warnings
        |
        +--> build_description_model()
        |       normalizes metadata into presentation sections
        |
        +--> render_description_markdown()
                emits the user-facing Markdown document
```

The Plumber `/description` endpoint validates the request, parses `filter_base`, resolves the release, calls `table_maker()` with metadata enabled, builds the model, renders Markdown, and returns both under the standard API response envelope. It recomputes the result from request parameters; it does not consume a previously returned `/table` result.

The metadata contract intentionally separates:

- `specification`: requested survey IDs and analytical parameters, with registry labels;
- `execution`: resolved release/PPP, surveys selected after pre-filters, staged exclusions, normalized filters, dispatched measures, suppression state, and physical PPP column;
- `provenance`: package version and resolved release;
- `warnings`: human-readable conditions captured during computation.

This is the right overall architecture and preserves backward compatibility for `with_meta = FALSE`. The execution-truth distinction is consistent with the project context and supersedes the older `included_surveys`/family-name schema.

## 2. Code Walkthrough

### `R/table_maker.R`

- `.build_specification()` at lines 99-190 turns request arguments into labeled nested records. It queries release-specific registries for analysis-variable labels, measure labels/families, dimension categories, and filter labels. It preserves the original `release` request while resolving a release for registry lookups.
- `.record_exclusions()` at lines 196-210 centralizes appending `pip_id`, reason, and stage records to the metadata state.
- `.format_dropped_info()` at lines 216-226 formats warning text for surveys missing requested filter or dimension variables.
- The metadata state is initialized at lines 340-373 only when `with_meta = TRUE`. It contains requested IDs, loaded/excluded surveys, resolved release/PPP, physical welfare column, normalized filters, measure names, suppression, and warnings.
- The warning handler at lines 378-386 is registered around the computation expression at lines 924-929, so `cli_warn()` calls in the compute path are captured into metadata.
- The manifest and pre-filter phases at lines 488-647 identify unknown IDs, surveys missing filter dimensions, and surveys missing requested `by` dimensions. Exclusions are recorded with stages `manifest`, `filter_pre`, and `dimension_pre`.
- The empty pre-filter branch at lines 650-669 returns an empty result record for metadata mode and attempts to populate PPP provenance from requested manifest entries.
- The normal loading path at lines 671-755 records candidate loaded surveys, calls `load_surveys()`, records the requested PPP and physical welfare column, and aborts on empty loaded data or missing metadata columns.
- `compute_measures()` is called once for the batch at lines 785-796. This preserves the project’s grouped orchestration optimization.
- Survey metadata is reconstructed from the full loaded microdata at lines 798-813, then joined back to the result.
- Suppression at lines 824-917 computes `pop_share` when necessary, records a threshold/count, retains share-family measures, and removes non-share rows for below-threshold cells.
- Lines 936-968 assemble the public sidecar. The returned `measures_computed` currently means requested/dispatched measure names, not necessarily measure rows remaining after suppression.

### `R/description.R`

- `build_description_model()` at lines 34-183 validates the top-level sidecar, maps resolved release/PPP/physical column into `metadata`, maps loaded/excluded surveys into `surveys`, builds the sample/statistics sections, carries suppression and warnings, and calls `.generate_cell_definition()`.
- The model validates `loaded_surveys` as a `data.table` with four required columns. Exclusion records are accepted only when they are a non-empty `data.table`, but their required columns are not validated.
- `render_description_markdown()` at lines 199-375 emits sections for metadata, surveys, sample base, statistics, layout, cell definition, suppression, and warnings. It handles the zero-loaded-survey case without emitting a survey table.
- Layout roles are reconstructed from `spec$by` order: last is columns, second-to-last rows, then super-columns and super-rows.
- `build_table_description()` at lines 406-430 is a convenience wrapper that recomputes `table_maker()` with metadata enabled, then builds and renders.
- `.generate_cell_definition()` at lines 442-482 describes aggregate or disaggregated cells and appends the poverty line/PPP. It uses specification measure labels and execution PPP.

### `inst/plumber/plumber.R` and `helpers.R`

- `/description` at lines 196-267 shares the table validator but independently parses `filter_base`, resolves the release, invokes `table_maker()`, builds the model, and renders Markdown.
- `capture_with_warnings()` catches the combined computation/description operation. The endpoint returns `model` and `markdown` under `data`, with warnings from both the API wrapper and table metadata.
- The endpoint’s response `meta` currently contains only the resolved release.
- The shared validator enforces required analysis variables, measures, IDs, PPP, poverty-line, dimensions, and suppression threshold. It does not validate `filter_base` JSON because parsing is endpoint-local.

## 3. Edge-Case Handling

### Covered reasonably

- Missing manifest IDs: warning plus `manifest` exclusion records in metadata mode.
- Missing filter dimensions: warning plus `filter_pre` exclusion records.
- Missing requested dimensions: warning plus `dimension_pre` exclusion records; partial dimension matches are excluded rather than NA-filled.
- All pre-filter exclusions in metadata mode: empty `loaded_surveys`, exclusion records, and a renderer message instead of an empty survey table.
- Empty loaded data after Arrow loading: computation aborts with an actionable loader error.
- Suppression: threshold validation, internal `pop_share` calculation, retained share-family rows, and suppressed-cell count are implemented.
- Missing physical PPP column in a normal populated path: the code now fails loudly rather than silently storing an NA column.
- No `by`: layout section is omitted and suppression records zero applicable cells.
- Missing execution metadata and malformed loaded-survey shape: some boundary validation exists in `build_description_model()`.

### Partially covered or misleading

- Loader-level PPP filtering: `load_surveys()` may skip surveys lacking the requested PPP column after `loaded_surveys` was already recorded. Such surveys are not recorded as exclusions, so the description can falsely claim they contributed.
- Empty-result suppression: the all-pre-filter branch never initializes suppression metadata, so a configured threshold is rendered as “Suppression is disabled.”
- Measures: the model renders requested specification measures. Suppression can remove non-share rows, and empty results contain no measure rows, but neither fact is reflected in the description.
- Filters: the description lists the normalized/requested filters but says “full survey sample” in the cell definition even when filters are active. It does not state AND/IN semantics or effective denominators.
- Warnings: computation warnings are captured, but warnings generated during post-computation specification building are outside the handler. Renderer-generated PPP warnings are not copied into model warnings.
- API errors: malformed `filter_base` JSON is caught as a 422 computation error rather than a 400 malformed-input error; computation, model, and renderer failures share a broad boundary.
- Exclusion schema: non-empty exclusion tables lacking `stage` can pass model construction and render incomplete rows.

### Not handled

- Missing-value treatment is not described. The model has no per-cell loaded/non-missing/effective denominator information or explicit welfare/weight missingness policy.
- Immutable source provenance is incomplete. Release and package version are recorded, but manifest identity/hash, per-survey partition version, and Arrow data identity are not.
- Layout roles are not explicit in the metadata model; they remain a positional convention.
- `/description` is not tied to a prior `/table` result. It always performs a fresh computation against mutable manifests, registries, and Arrow data.
- Markdown values are not escaped, so registry labels, reasons, categories, or package-level IDs containing pipes/newlines can corrupt the document.
- Duplicate manifest IDs are not rejected before loading and can duplicate observations and alter weighted statistics.

## 4. Additional Components and Assumptions

- `load_surveys()` is a critical part of the description contract, not merely an implementation detail. Its skip/warning behavior must be reflected in `execution$loaded_surveys` and `excluded_surveys`.
- The manifest is the authority for survey identity, release selection, dimensions, welfare columns, and partition versions. A logical release ID alone is not necessarily immutable if artifacts can be replaced in place.
- Registry labels and categories are data dependencies. A description can be numerically correct but semantically misleading if labels come from a different release or if lookup errors silently fall back to raw names.
- The API and direct R paths do not preserve exactly the same requested-release semantics: `/description` resolves `release` before passing it to `table_maker()`, so omitted release can no longer remain distinguishable from explicitly supplied release in `specification`.
- The current feature returns both a structured R-oriented model and Markdown. If JSON consumers are supported, `data.table`/nested-list shapes and `NULL` versus empty semantics need a versioned contract.
- The project’s existing Arrow contract relies on one file per partition and sorted welfare for inequality measures. The description should preserve enough partition/version provenance to make that contract auditable.
- The package currently has no committed environment lockfile, so exact dependency versions for Arrow, Plumber, data.table, collapse, and registry tooling are not captured by this feature.

## 5. Strengths and Weaknesses

### Weaknesses first

#### P0 — Blocking correctness risks

- **[P0.1] [cg-data-quality] `R/table_maker.R:674-680`, `R/load_data.R:483-506` — PPP-skipped surveys are reported as loaded.** `loaded_surveys` is populated before `load_surveys()` applies PPP availability filtering. A skipped survey is neither removed nor recorded as an exclusion. This can produce a false statement about which survey contributed to the statistics. The same issue can become order-dependent when mixed PPP coverage is requested.
  **Fix**: make the loader return retained/skipped survey metadata and selected physical columns, or apply identical PPP filtering before recording `loaded_surveys`; record a loader/PPP exclusion stage.
- **[P0.2] [cg-data-quality] `R/description.R:111-128`, `R/table_maker.R:700-755` — Missing-value treatment is absent from the factual description.** The prose claims a full sample or weighted population without disclosing missing welfare, analysis-variable, or weight handling. Different measure families can therefore use different effective samples while the description implies a common denominator.
  **Fix**: define/enforce a missingness policy and expose loaded, non-missing, and weighted-contributing counts or an explicit policy in metadata and Markdown.

#### P1 — Critical correctness and contract risks

- **[P1.1] `R/table_maker.R:650-669`, `R/description.R:139-153` — Empty results falsely report suppression disabled.** Initialize suppression metadata for all execution branches and render “no cells available” separately from disabled suppression.
- **[P1.2] `R/table_maker.R:409-412`, `R/description.R:124-128,305-306` — Measures describe the request, not returned output.** Track requested, dispatched, and returned measures separately; state which non-share measures were removed by suppression.
- **[P1.3] `R/description.R:448-478` — Cell definition says full sample despite filters.** Use execution-level normalized filters and wording that describes the filtered population.
- **[P1.4] `R/description.R:351-358` — Suppression prose overstates removal.** It removes non-share measures while retaining share-family measures, but the active-suppression text says cells are suppressed without this qualification.
- **[P1.5] `R/description.R:163-172,315-336` — Layout roles are inferred rather than represented.** A client or UI ordering change can yield a description that labels rows/columns incorrectly while the table remains valid. Store explicit roles or a normalized role-keyed layout object.
- **[P1.6] `R/table_maker.R:936-940`, `R/description.R:41-83` — Specification labels can be built from mutable/different registry state.** Resolve once and pass the same resolved release/registry snapshot through computation and specification construction; do not silently suppress analysis-variable lookup failures.

#### P2 — Important maintainability, reproducibility, and API risks

- **[P2.1] `R/table_maker.R:801-813` — Full microdata scan for survey metadata.** Use already-selected manifest entries instead of scanning all respondent rows; this threatens the stated metadata overhead target.
- **[P2.2] `R/table_maker.R:714-745` — PPP metadata resolution duplicates loader work and assumes uniform full welfare-variable vectors.** Return loader provenance or resolve only the selected physical column after retained-survey filtering.
- **[P2.3] `R/table_maker.R:924-940` — Warnings during metadata construction are outside the capture boundary.** Capture specification construction too, or record those warnings separately and consistently.
- **[P2.4] `inst/plumber/plumber.R:235-267` — `/description` duplicates request parsing/execution and recomputes.** Share normalization/execution helpers and explicitly define whether the endpoint describes a fresh computation or an existing table result.
- **[P2.5] `inst/plumber/plumber.R:235-260` — Malformed JSON and domain failures share 422 handling.** Parse/validate `filter_base` before execution and distinguish 400 input errors, 422 valid-domain/data failures, and 500 implementation failures.
- **[P2.6] `R/description.R:253-336` — Markdown interpolation is unescaped.** Escape pipes, backticks, backslashes, and line breaks in IDs, labels, categories, and reasons.
- **[P2.7] `R/table_maker.R:495-497`, loader contract — Duplicate manifest IDs can duplicate data.** Validate manifest primary-key uniqueness before constructing paths.
- **[P2.8] `R/table_maker.R:956-960` — Provenance is not immutable enough.** Add manifest identity/content hash, selected partition versions, and stable Arrow source identifiers.
- **[P2.9] `R/description.R:56-109` — Nested metadata validation is incomplete.** Validate exclusion columns, scalar types, suppression shape, measure records, filters, categories, and optional semantics at the model boundary.
- **[P2.10] Tests — Highest-risk branches lack contract assertions.** Add fixtures/tests for PPP-skipped surveys, missing values, suppression/returned measures, all-excluded suppression, layout permutations, warning shape, release drift, malformed filters, deterministic ordering, and API status distinctions.

#### P3 — Minor

- **[P3.1] `inst/plumber/helpers.R:205-218` — Validator documentation contains duplicated text in the `measures` bullet.** Correct the sentence so the public contract is readable.

### Strengths

- The sidecar design keeps the default `table_maker()` output unchanged while enabling structured descriptions and future renderers.
- The corrective schema is materially clearer than the original implementation: requested IDs, loaded surveys, staged exclusions, resolved PPP, physical welfare column, normalized filters, and dispatched measure names have distinct fields.
- Batch computation remains a single `compute_measures()` call, preserving the project’s grouped-collapse performance strategy.
- Pre-load dimension filtering prevents invalid partial-match tables and records why surveys were excluded.
- Suppression is implemented as an unconditional disclosure guard, computes internal population shares when needed, retains share-family measures, and records a suppression count.
- The renderer uses list accumulation rather than repeated whole-string concatenation and handles the all-excluded survey-table edge case.
- The API returns both a structured model and Markdown, which supports UI display, downloads, and downstream machine consumption.
- Existing tests cover the central metadata fields, staged pre-filter exclusions, release/PPP happy paths, model sections, renderer sections, and basic endpoint validation/success behavior.

## Passed

- `cg-code-quality`: issues reported above; no separate P0 security issue.
- `cg-testing`: issues reported above; coverage gaps are included in P2.10.
- `cg-documentation`: issues reported above; no separate correctness finding beyond the contract issues listed.
- `cg-version-control`: no secrets or generated survey data found in the scoped feature; repository-history hygiene and lockfile concerns are outside the immediate description correctness blockers.
- `cg-reproducibility`: findings incorporated above; no direct security or immediate numeric-computation defect identified in the normal successful path.
- `cg-performance`: findings incorporated above; batch compute and renderer complexity are sound, but metadata overhead needs measurement.
- `cg-architecture`: findings incorporated above; sidecar architecture is sound but the schema needs versioning and explicit layout roles.
- `cg-data-quality`: findings incorporated above; PPP loader accounting and missingness are the primary data-integrity risks.
- `cg-learnings-researcher`: confirmed the sidecar and request/execution separation as sound, and identified stale all-excluded PPP review status.
- `cg-adversarial`: findings incorporated above; mixed PPP coverage, duplicate manifest IDs, Markdown injection, and malformed JSON were specifically exercised as attack paths.

## Brain and Context Sources

- `.cg-docs/plans/2026-08-24-step3-description-metadata-contract-corrective.md`
- `.cg-docs/brainstorms/2026-08-24-step3-description-metadata-contract-redesign.md`
- `.cg-docs/reviews/2026-08-24-step3-description-metadata-contract-corrective-review.md`
- `.cg-docs/solutions/data-quality/2026-08-24-metadata-schema-execution-truth-pattern.md`
- `compound-gpid.context.md` metadata validation and Arrow provenance sections

## Incomplete Reviews

None. All dispatched agents returned usable output.
