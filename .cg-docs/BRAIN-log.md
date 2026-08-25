# 🧠 Project Brain — Chronological Log

_Generated 2026-08-24 · 81 artifacts (newest first) + 0 roadmap features_

## undated

- **[2026-04-03-manifest-generation-version-partition-review](.cg-docs/reviews/2026-04-03-manifest-generation-version-partition-review.md)** · `review` · _—_ · `—`
  > **Review depth**: standard (+ auto-escalation: `load_data.R` matches `**/load*.R` → `@cg-data-quality` always include…
- **[2026-04-07-computation-engine-review](.cg-docs/reviews/2026-04-07-computation-engine-review.md)** · `review` · _—_ · `—`
  > **Review depth**: targeted (performance + correctness focus on `compute_poverty.R`) **Files reviewed**: `R/compute_po…
- **[2026-04-28-table-maker-approach-b-refactor-review](.cg-docs/reviews/2026-04-28-table-maker-approach-b-refactor-review.md)** · `review` · _—_ · `—`
  > **Review depth**: Thorough (from `compound-gpid.local.md`) **Files reviewed**: 10 (`R/compute_measures.R`, `R/table_m…
- **[2026-04-29-arrow-vs-collapse-benchmark-review](.cg-docs/reviews/2026-04-29-arrow-vs-collapse-benchmark-review.md)** · `review` · _—_ · `—`
  > **Review depth**: Standard (+ auto-escalated: statistical aggregation functions, >200 non-test lines) **Files reviewe…
- **[2026-05-11-api-service-plumber-v2-impl-review](.cg-docs/reviews/2026-05-11-api-service-plumber-v2-impl-review.md)** · `review` · _fixed_ · `—`
  > Reviewed against the completed implementation of `.cg-docs/plans/2026-05-11-api-service-plumber-v2.md` (all 5 steps).…
- **[2026-05-20-validate-parquet-wrapper-review](.cg-docs/reviews/2026-05-20-validate-parquet-wrapper-review.md)** · `review` · _—_ · `—`
  > **Review depth**: thorough **Files reviewed**: `R/compute_inequality.R`, `R/load_data.R`, `R/table_maker.R`, `R/schem…
- **[2026-05-20-validate-parquet-wrapper-review-2](.cg-docs/reviews/2026-05-20-validate-parquet-wrapper-review-2.md)** · `review` · _—_ · `—`
  > **Review depth**: thorough **Files reviewed**: `R/load_data.R`, `tests/testthat/test-load-data.R` **Findings**: 9 (P0…

## 2026-08-24

- **[2026-08-24-step3-description-metadata-contract-corrective-review](.cg-docs/reviews/2026-08-24-step3-description-metadata-contract-corrective-review.md)** · `review` · _—_ · `2026-08-24`
  > **Review mode**: architecture **Files reviewed**: 4 (R/table_maker.R, R/description.R, tests/testthat/test-table-make…
- **[Metadata Schema: Execution Truth Pattern](.cg-docs/solutions/data-quality/2026-08-24-metadata-schema-execution-truth-pattern.md)** · `solution` · _—_ · `2026-08-24`
  > The `table_maker(with_meta=TRUE)` metadata contract mixed **requested** parameters with **actual** execution results,…
- **[Step 3 Description Metadata Contract Redesign](.cg-docs/brainstorms/2026-08-24-step3-description-metadata-contract-redesign.md)** · `brainstorm` · _decided_ · `2026-08-24`
  > This brainstorm audits the current Step 3 description implementation (completed Aug 21, 2026) and redesigns the metad…
- **[Step 3 Description Metadata Contract — Corrective Implementation](.cg-docs/plans/2026-08-24-step3-description-metadata-contract-corrective.md)** · `plan` · _completed_ · `2026-08-24`
  > Redesign the `table_maker(with_meta = TRUE)` metadata contract to achieve execution truth: the description must accur…

## 2026-08-21

- **[2026-08-20-step3-description-implementation-review](.cg-docs/reviews/2026-08-20-step3-description-implementation-review.md)** · `review` · _—_ · `2026-08-21`
  > **Review mode**: standard **Files reviewed**: 10 code files + 5 test files + 1 helper **Findings**: 22 (P0: 3, P1: 3,…

## 2026-08-20

- **[Step 3 Description File - Architecture Revision](.cg-docs/brainstorms/2026-08-20-step3-description-architecture.md)** · `brainstorm` · _decided_ · `2026-08-20`
  > This brainstorm revises and extends the Aug 13 brainstorm (`2026-08-13-step3-description-file.md`), which chose "Stru…
- **[Step 3 Description File - Implementation Plan](.cg-docs/plans/2026-08-20-step3-description-implementation.md)** · `plan` · _completed_ · `2026-08-20`
  > Implement a structured description document for Step 3 table results. The description explains what a generated table…

## 2026-08-13

- **[Step 3 Description File for Table Results](.cg-docs/brainstorms/2026-08-13-step3-description-file.md)** · `brainstorm` · _decided_ · `2026-08-13`
  > Users reach Step 3 of the Table Maker wizard after making multiple decisions across Step 1 (survey selection) and Ste…

## 2026-07-31

- **[API Deployment Documentation](.cg-docs/brainstorms/2026-07-31-api-deployment-documentation.md)** · `brainstorm` · _decided_ · `2026-07-31`
  > Currently, deployment knowledge for the Table Maker API exists only in one person's head. The team needs this documen…
- **[Implement survey-specific variable filtering endpoints](.cg-docs/plans/2026-07-31-survey-specific-variable-filtering.md)** · `plan` · _active_ · `2026-07-31`
  > Update API endpoints so Step 2 UI can pass selected `pip_id` values and receive only variables available across all s…
- **[README API deployment documentation](.cg-docs/plans/2026-07-31-api-deployment-readme.md)** · `plan` · _completed_ · `2026-07-31`
  > - Document API deployment workflow for PIP team in `README.md`. - Cover the 3 deployment pipelines and DEV/QA/PROD en…
- **[Session Storage Endpoints for Survey Selection Persistence](.cg-docs/brainstorms/2026-07-31-session-storage-endpoints.md)** · `brainstorm` · _decided_ · `2026-07-31`
  > UI team requested endpoints to "store pip_ids" to support crash recovery / page refresh resilience. If a user is in S…
- **[Survey-Specific Variable Filtering for Step 2](.cg-docs/brainstorms/2026-07-31-survey-specific-variable-filtering.md)** · `brainstorm` · _decided_ · `2026-07-31`
  > The ITS UI development team requested a way to persist survey selections from Step 1 (survey grid) so that if Step 2 …

## 2026-07-24

- **[Sync testthat suite to current exported functions](.cg-docs/plans/2026-07-24-exported-functions-test-sync.md)** · `plan` · _active_ · `2026-07-24`
  > Bring `tests/testthat` in sync with the current implementation in `R/` for exported functions, treating current funct…

## 2026-07-02

- **[Pop-share threshold as always-active suppression guard](.cg-docs/brainstorms/2026-07-02-pop-share-threshold-always-active.md)** · `brainstorm` · _decided_ · `2026-07-02`
  > Follow-up to `2026-06-01-pop-share-threshold-suppression.md`.  That brainstorm implemented `pop_share_threshold` as a…

## 2026-07-01

- **[Table Endpoint & Computation Layer Refactor — Multi-Analysis-Variable Support](.cg-docs/plans/2026-07-01-table-endpoint-computation-refactor.md)** · `plan` · _completed_ · `2026-07-01`
  > Refactor the piptm computation engine and Plumber API to support multiple analysis variables (not just welfare) while…

## 2026-06-30

- **[Table Maker sample-base filtering \(filter_base\) in loading path](.cg-docs/plans/2026-06-30-table-maker-filter-base-loading-path.md)** · `plan` · _active_ · `2026-06-30`
  > Add a `filter_base` feature that restricts the sample entering computation by filtering microdata in the loading path…

## 2026-06-23

- **[Stage 1: Surveys UI endpoint](.cg-docs/plans/2026-06-23-stage1-surveys-ui.md)** · `plan` · _active_ · `2026-06-23`
  > - Use `piptm_manifest()` and variable registry to assemble UI rows.
- **[Variable Registry — Build, Load, and API Metadata Endpoints](.cg-docs/plans/2026-06-23-variable-registry.md)** · `plan` · _completed_ · `2026-06-23`
  > Implement a release-specific variable registry that acts as the single source of truth for all variable-level metadat…

## 2026-06-12

- **[Replace pip_measures\(\) with pip_tablemaker_measures\(\) and update /measures endpoint](.cg-docs/plans/2026-06-12-replace-pip-measures-with-tablemaker-measures.md)** · `plan` · _completed_ · `2026-06-12`
  > Remove `pip_measures()` and replace it with `pip_tablemaker_measures()` — a richer, static catalogue of analysis vari…

## 2026-06-11

- **[Arrow schema expansion: all 5 propagation points must be updated together](.cg-docs/solutions/data-quality/2026-06-11-arrow-schema-expansion-propagation-pattern.md)** · `solution` · _—_ · `2026-06-11`
  > When new optional columns are added to the PIP Arrow schema (e.g. household size, infrastructure indicators, labour v…
- **[Expand Allowed Column Set in PIP Arrow Pipeline](.cg-docs/plans/2026-06-11-expand-allowed-column-set.md)** · `plan` · _completed_ · `2026-06-11`
  > Add 18 new socioeconomic indicator columns (all `int32`, no extra standardisation required) to the canonical Arrow sc…

## 2026-06-01

- **[Add obs_share and pop_share measures](.cg-docs/plans/2026-06-01-cell-share-measures.md)** · `plan` · _completed_ · `2026-06-01`
  > Based on brainstorm: `.cg-docs/brainstorms/2026-06-01-cell-share-measures.md`
- **[Adding new measures to the computation engine via the registry pattern](.cg-docs/solutions/testing-patterns/2026-06-01-adding-measures-registry-pattern.md)** · `solution` · _—_ · `2026-06-01`
  > Need to extend the computation engine with new measures (`obs_share`, `pop_share`) without breaking existing consumer…
- **[Cell observation share and population share measures](.cg-docs/brainstorms/2026-06-01-cell-share-measures.md)** · `brainstorm` · _decided_ · `2026-06-01`
  > Need to add two new measures to the computation engine: - `obs_share` — proportion of total survey observations in ea…
- **[Pop share threshold suppression](.cg-docs/plans/2026-06-01-pop-share-threshold-suppression.md)** · `plan` · _active_ · `2026-06-01`
  > Based on brainstorm: `.cg-docs/brainstorms/2026-06-01-pop-share-threshold-suppression.md`
- **[Pop share threshold suppression for small cells](.cg-docs/brainstorms/2026-06-01-pop-share-threshold-suppression.md)** · `brainstorm` · _decided_ · `2026-06-01`
  > After adding `pop_share` and `obs_share` measures, we need a configurable suppression rule: if a cell's population sh…

## 2026-05-29

- **[Dimension filter: skip partial-match surveys](.cg-docs/plans/2026-05-29-dimension-filter-skip-partial-surveys.md)** · `plan` · _completed_ · `2026-05-29`
  > Change `table_maker()` so that surveys missing *any* requested breakdown dimension are skipped entirely (not loaded),…
- **[Refactor PPP handling: default to 2021, skip surveys missing welfare column](.cg-docs/plans/2026-05-29-ppp-default-refactor.md)** · `plan` · _completed_ · `2026-05-29`
  > Remove the `ppp_sort` inference/fallback mechanism from both `load_survey_microdata()` and `load_surveys()`. Default …

## 2026-05-28

- **[Remove legacy schema from load_surveys\(\)](.cg-docs/plans/2026-05-28-remove-legacy-schema-load-surveys.md)** · `plan` · _completed_ · `2026-05-28`
  > Refactor `load_surveys()` to assume all entries carry `welfare_vars` (new schema only). Remove all backward-compatibi…

## 2026-05-26

- **[2026-05-26-api-ppp-param-review](.cg-docs/reviews/2026-05-26-api-ppp-param-review.md)** · `review` · _—_ · `2026-05-26`
  > **Review depth**: thorough **Files reviewed**: 4 (`inst/plumber/helpers.R`, `inst/plumber/plumber.R`, `tests/testthat…

## 2026-05-25

- **[2026-05-20-validate-parquet-wrapper-verify-review](.cg-docs/reviews/2026-05-20-validate-parquet-wrapper-verify-review.md)** · `review` · _—_ · `2026-05-25`
  > **Review depth**: light (verify mode) **Files reviewed**: `R/load_data.R`, `tests/testthat/test-load-data.R` **Findin…

## 2026-05-21

- **[Arrow open_dataset on multiple Parquet files silently breaks within-survey welfare sort](.cg-docs/solutions/data-quality/2026-05-21-arrow-multifile-partition-sort-violation.md)** · `solution` · _—_ · `2026-05-21`
  > The `{piptm}` pipeline relies on a **pre-sort contract**: welfare values in each survey's microdata are sorted ascend…
- **[collapse::fsum/fcumsum silently drops NA rows, biasing weighted statistics](.cg-docs/solutions/data-quality/2026-05-21-collapse-silent-na-drop-in-statistics.md)** · `solution` · _—_ · `2026-05-21`
  > `compute_inequality()` calls `collapse::fsum()`, `collapse::fcumsum()`, and `collapse::fmean()` on `welfare` and `wei…
- **[Column Pruning in load_surveys\(\): Implementation Pattern](.cg-docs/solutions/performance-issues/2026-05-21-load-surveys-column-pruning-implementation.md)** · `solution` · _—_ · `2026-05-21`
  > `load_surveys()` always called `open_dataset() |> collect()` without any `select()`, loading all 14 schema columns in…
- **[Remove redundant welfare sort in compute_inequality\(\)](.cg-docs/brainstorms/2026-05-21-remove-redundant-welfare-sort.md)** · `brainstorm` · _decided_ · `2026-05-21`
  > `compute_inequality()` sorts data by welfare within each group before computing Gini. However, the upstream data pipe…
- **[Testing pre-sort contracts: verify the contract is required, not just that it holds](.cg-docs/solutions/testing-patterns/2026-05-21-testing-pre-sort-contracts.md)** · `solution` · _—_ · `2026-05-21`
  > `compute_inequality()` was refactored to remove its internal `setorder()` call, relying instead on a documented pre-s…

## 2026-05-20

- **[Column pruning in load_surveys\(\) for table_maker performance](.cg-docs/brainstorms/2026-05-20-load-surveys-column-pruning.md)** · `brainstorm` · _decided_ · `2026-05-20`
  > Benchmarking (50-iteration resample, 15 surveys over UNC share) showed that loading only the 6 needed columns instead…
- **[Column pruning in load_surveys\(\) for table_maker performance](.cg-docs/plans/2026-05-20-load-surveys-column-pruning.md)** · `plan` · _completed_ · `2026-05-20`
  > Benchmarking showed loading only needed columns instead of all 14 gives a 68% I/O improvement (1.658s → 0.524s) over …
- **[validate_parquet\(\) unified wrapper](.cg-docs/plans/2026-05-20-validate-parquet-wrapper.md)** · `plan` · _completed_ · `2026-05-20`
  > Add a single-entry-point `validate_parquet()` function to `R/validate_parquet.R` that dispatches to the three existin…

## 2026-05-13

- **[2026-05-11-api-service-plumber-v2-review](.cg-docs/reviews/2026-05-11-api-service-plumber-v2-review.md)** · `review` · _—_ · `2026-05-13`
  > **Review depth**: thorough **Files reviewed**: 4 (`inst/plumber/helpers.R`, `inst/plumber/plumber.R`, `tests/testthat…
- **[Programmatic plumber endpoint testing with pr$call\(\) and Hive fixtures](.cg-docs/solutions/testing-patterns/2026-05-13-plumber-programmatic-testing-with-fixtures.md)** · `solution` · _—_ · `2026-05-13`
  > Testing a plumber API without a running HTTP server is non-trivial. Three concrete issues emerged when writing integr…

## 2026-05-11

- **[API Service Layer — Plumber Endpoints \(v2\)](.cg-docs/plans/2026-05-11-api-service-plumber-v2.md)** · `plan` · _completed_ · `2026-05-11`
  > Implement a Plumber-based API service that exposes `table_maker()` and supporting discovery functions to the PIP plat…

## 2026-05-08

- **[API Service Architecture for Table Maker](.cg-docs/brainstorms/2026-05-08-api-service-architecture.md)** · `brainstorm` · _decided_ · `2026-05-08`
  > The computation engine (`table_maker()`) is complete and optimised. The next step is a Plumber-based API service laye…
- **[API Service Layer — Plumber Endpoints](.cg-docs/plans/2026-05-08-api-service-plumber.md)** · `plan` · _superseded_ · `2026-05-08`
  > Implement a Plumber-based API service that exposes `table_maker()` and supporting discovery functions to the PIP plat…

## 2026-04-30

- **[Arrow I/O and Compute Strategy: Lessons from Benchmarking](.cg-docs/solutions/performance-issues/2026-04-30-arrow-io-and-compute-lessons.md)** · `solution` · _—_ · `2026-04-30`
  > The `table_maker()` pipeline was meeting a 3s compute target in isolation but its real-world performance was unknown …

## 2026-04-29

- **[Arrow I/O Strategy Benchmark Results](.cg-docs/solutions/performance-issues/2026-04-29-arrow-vs-collapse-results.md)** · `solution` · _completed_ · `2026-04-29`
  > - **R version**: R version 4.5.2 (2025-10-31 ucrt) - **Platform**: Windows Server 2022 x64 (build 20348) / x86_64-w64…
- **[Arrow vs Collapse End-to-End Benchmark](.cg-docs/brainstorms/2026-04-29-arrow-vs-collapse-benchmark.md)** · `brainstorm` · _decided_ · `2026-04-29`
  > <!-- Valid status values: decided, in-progress, abandoned -->
- **[Arrow vs Collapse End-to-End Benchmark](.cg-docs/plans/2026-04-29-arrow-vs-collapse-benchmark.md)** · `plan` · _completed_ · `2026-04-29`
  > Measure whether pushing column selection, filtering, or partial aggregation into Arrow (before `collect()`) speeds up…
- **[Pipeline Comparison Benchmark Results](.cg-docs/solutions/performance-issues/2026-04-29-pipeline-comparison-results.md)** · `solution` · _completed_ · `2026-04-29`
  > - **R version**: R version 4.5.2 (2025-10-31 ucrt) - **Platform**: Windows Server 2022 x64 (build 20348) / x86_64-w64…

## 2026-04-28

- **[Age binning: mutating 'by' vector before compute_measures causes unknown-dimension error](.cg-docs/solutions/bugs/2026-04-28-age-binning-by-vector-mutated-before-validate.md)** · `solution` · _—_ · `2026-04-28`
  > Calling `table_maker(..., by = "age")` threw:
- **[cli >= 3.4.0 rejects glue expressions starting with a dot](.cg-docs/solutions/bugs/2026-04-28-cli-dot-prefix-glue-expression-rejected.md)** · `solution` · _—_ · `2026-04-28`
  > A `cli_abort()` call threw an error instead of the intended message:
- **[fsetdiff attributed to collapse namespace — function does not exist there](.cg-docs/solutions/bugs/2026-04-28-fsetdiff-wrong-namespace-collapse-vs-datatable.md)** · `solution` · _—_ · `2026-04-28`
  > At runtime, calling `pip_lookup()` with an unmatched triplet threw:
- **[Orchestration Strategy Benchmark](.cg-docs/plans/2026-04-28-orchestration-benchmark.md)** · `plan` · _completed_ · `2026-04-28`
  > Create a standalone benchmark script that compares orchestration strategies for `table_maker()`'s per-survey computat…
- **[Orchestration Strategy Benchmark Results](.cg-docs/solutions/performance-issues/2026-04-28-orchestration-benchmark-results.md)** · `solution` · _completed_ · `2026-04-28`
  > - **R version**: R version 4.5.2 (2025-10-31 ucrt) - **Platform**: Windows Server 2022 x64 (build 20348) / x86_64-w64…
- **[Orchestration Strategy Benchmark — Per-Slice vs Grouped vs nthreads](.cg-docs/brainstorms/2026-04-28-orchestration-benchmark.md)** · `brainstorm` · _decided_ · `2026-04-28`
  > Step 8 of the computation engine plan (`2026-04-07-computation-engine.md`) proposes a per-slice vs grouped benchmark …
- **[Refactor table_maker\(\) to Approach B — Grouped Collapse Orchestration](.cg-docs/plans/2026-04-28-table-maker-approach-b-refactor.md)** · `plan` · _completed_ · `2026-04-28`
  > Replace the per-survey `lapply()` loop in `table_maker()` (Steps 6–7) with Approach B's grouped collapse strategy: bu…

## 2026-04-27

- **[table_maker\(\) Input Parameter Structure](.cg-docs/brainstorms/2026-04-27-table-maker-input-parameters.md)** · `brainstorm` · _decided_ · `2026-04-27`
  > The Step 7 plan defines `table_maker(country_code, year, welfare_type, ...)` with three separate vectors. Filtering t…

## 2026-04-16

- **[Dimension Pre-Filter: Partial Match with NA Fill](.cg-docs/brainstorms/2026-04-16-dimension-prefilter-partial-match.md)** · `brainstorm` · _decided_ · `2026-04-16`
  > The computation engine plan (Step 7, `table_maker()`) originally specified a strict dimension pre-filter: surveys mis…

## 2026-04-14

- **[data.table column name shadows function argument in filter expression](.cg-docs/solutions/bugs/2026-04-14-datatable-column-shadows-function-argument.md)** · `solution` · _—_ · `2026-04-14`
  > `load_survey_microdata("COL", 2010L, "INC")` returned all rows from the manifest instead of the single matching row. …
- **[Hardcoded network paths in .onLoad\(\) break portability across machines](.cg-docs/solutions/environment-issues/2026-04-14-hardcoded-paths-in-onload.md)** · `solution` · _—_ · `2026-04-14`
  > The `piptm` package loaded correctly on the original developer's machine (where `Y:/` was mapped to the PIP network s…
- **[load_surveys\(\) silently skips missing partitions when path helper is not shared](.cg-docs/solutions/bugs/2026-04-14-load-surveys-silent-partial-miss.md)** · `solution` · _—_ · `2026-04-14`
  > When a manifest entry referred to a partition directory that did not exist on disk, `load_surveys()` returned a resul…
- **[Partial-miss regression test pattern for batch data loaders](.cg-docs/solutions/testing-patterns/2026-04-14-partial-miss-regression-test-batch-loaders.md)** · `solution` · _—_ · `2026-04-14`
  > A batch loading function (`load_surveys()`) silently dropped surveys when their partition directories were missing fr…

## 2026-04-07

- **[Computation Engine Design — Family-Based Orchestrator](.cg-docs/brainstorms/2026-04-07-computation-engine-design.md)** · `brainstorm` · _decided_ · `2026-04-07`
  > With Phase 0 (Arrow dataset generation, manifest system, data loading) largely in place, the project needs to design …
- **[Computation Engine — Family-Based Orchestrator](.cg-docs/plans/2026-04-07-computation-engine.md)** · `plan` · _active_ · `2026-04-07`
  > Implement the core computation engine for {piptm}: a family-based measure orchestrator that computes indicators acros…

## 2026-04-03

- **[Manifest Generation and Version Partition Filtering](.cg-docs/brainstorms/2026-04-03-manifest-generation-version-partition.md)** · `brainstorm` · _decided_ · `2026-04-03`
  > The March 17 brainstorm established the multi-release manifest architecture. Since then, the Arrow dataset partition …
- **[Manifest Generation and Version Partition Filtering](.cg-docs/plans/2026-04-03-manifest-generation-version-partition.md)** · `plan` · _active_ · `2026-04-03`
  > Transition the manifest from storing physical `file_path` per entry to storing four logical partition filter keys (`c…
- **[NULL .ALLOWED_COLS_GEN causes .validate_for_write\(\) to reject all schema-valid columns](.cg-docs/solutions/bugs/2026-04-03-null-allowed-cols-gen-validate-for-write.md)** · `solution` · _—_ · `2026-04-03`
  > `generate_arrow_dataset()` returned an `"error"` row for every survey with this message:

## 2026-04-02

- **[Version as partition key with manifest-based resolution](.cg-docs/brainstorms/2026-04-02-version-partition-and-manifest-resolution.md)** · `brainstorm` · _decided_ · `2026-04-02`
  > The `prepare_for_arrow()` pipeline in `{pipdata}` now injects a `version` column (e.g. `"v01_v04"`, derived from `met…
- **[Version as partition key with manifest-based resolution](.cg-docs/plans/2026-04-02-version-partition-and-manifest-resolution.md)** · `plan` · _active_ · `2026-04-02`
  > Add `version` (e.g. `"v01_v04"`) as a 4th Hive partition key in the Arrow repository so that multiple survey versions…

## 2026-03-17

- **[Arrow Data Preparation Pipeline](.cg-docs/plans/2026-03-17-arrow-data-preparation.md)** · `plan` · _active_ · `2026-03-17`
  > Implement the data preparation pipeline that takes clean survey datasets produced by {pipdata} (`.qs2` files) and gen…
- **[Efficient FGT Poverty Measure Computation Design](.cg-docs/brainstorms/2026-03-17-fgt-computation-design.md)** · `brainstorm` · _decided_ · `2026-03-17`
  > Before implementing the computation engine, we needed to design the most efficient strategy for computing Foster-Gree…
- **[Multi-Release Manifest Architecture](.cg-docs/brainstorms/2026-03-17-multi-release-manifest-architecture.md)** · `brainstorm` · _decided_ · `2026-03-17`
  > The original data pipeline architecture brainstorm (2026-03-16) established a Manifest-First with Lazy Validation app…

## 2026-03-16

- **[Data Pipeline Architecture — Ingestion, Manifest, and Computation Design](.cg-docs/brainstorms/2026-03-16-data-pipeline-architecture.md)** · `brainstorm` · _decided_ · `2026-03-16`
  > The {piptm} package is the computation engine for the PIP Table Maker. Before implementing any code, we needed to cla…
