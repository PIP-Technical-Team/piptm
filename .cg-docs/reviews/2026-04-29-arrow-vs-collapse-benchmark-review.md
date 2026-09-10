---
plan: .cg-docs/plans/2026-04-29-arrow-vs-collapse-benchmark.md
findings:
  P1.1: fixed
  P1.2: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P3.1: open
  P3.2: open
  P3.3: open
  P3.4: open
---

## Review Report

**Review depth**: Standard (+ auto-escalated: statistical aggregation functions, >200 non-test lines)
**Files reviewed**: 1 (`benchmarks/arrow-vs-collapse.R`)
**Findings**: 12 (P0: 0, P1: 2, P2: 6, P3: 4)

> Auto-escalation applied: script calls `sum(welfare * weight)` (statistical
> aggregation) and `set.seed()` → `@cg-data-quality` + `@cg-reproducibility`
> added to the standard tier. Script is ~750 non-test lines (>200 threshold) —
> consider `/cg-review thorough` for `@cg-adversarial` coverage.

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-version-control] `benchmarks/arrow-cache.rds` — committed to git, contains absolute UNC paths to the network share.
  **Why**: Fails silently on any machine other than the one that generated it. Paths like `//w1wbgencifs01/...` are not portable; another developer or CI will load stale survey sample paths that no longer exist or point to wrong versions.
  **Fix**: Add `^benchmarks/arrow-cache\.rds$` to `.gitignore`; remove from tracking with `git rm --cached benchmarks/arrow-cache.rds`.

- **[P1.2]** [cg-testing] `benchmarks/arrow-vs-collapse.R` — `decision_push` message states "E2E-3 Push excluded — correctness check failed" but `check_3 = TRUE`; the approach passed the correctness gate and failed inside `bench::mark()` at benchmark time with `NotImplemented: Unifying differing dictionaries`.
  **Why**: The results document misdiagnoses the root cause. "Correctness failed" implies a logic error in the push calculation. The actual cause is Arrow's multi-file dict-index incompatibility — a separate engineering problem that could be fixed (see P2.4). Conflating the two blocks the correct next action.
  **Fix**: Separate the correctness gate from the benchmark-time failure. Add a `bench_failed` flag when `tryCatch` catches an error inside `bench::mark()`. Update the decision message: `"E2E-3 Push excluded — benchmark failed at runtime (Arrow dict incompatibility on multi-file dataset). Correctness check PASSED on fixture."`.

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] `benchmarks/arrow-vs-collapse.R:~296` — `.cast_dict_to_char()` is defined but never called.
  **Why**: Dead code after the refactor that inlined the dict cast into `.arrow_weighted_mean()` and `.arrow_headcount()`. Misleads readers into thinking it is part of the active execution path.
  **Fix**: Remove the function definition (7 lines).

- **[P2.2]** [cg-code-quality] `benchmarks/arrow-vs-collapse.R:~246, ~299, ~315, ~328` — dict column list `c("gender", "area", "educat4", "educat5", "educat7")` hardcoded in 4 separate places.
  **Why**: DRY violation; any schema change (e.g. adding `educat7` to the benchmark) requires 4 edits across the script.
  **Fix**: Add `DICT_SCHEMA_COLS <- c("gender", "area", "educat4", "educat5", "educat7")` to the config block and replace all 4 occurrences with `intersect(by, DICT_SCHEMA_COLS)` (or `intersect(by_cols, DICT_SCHEMA_COLS)`).

- **[P2.3]** [cg-code-quality] `.Rbuildignore` — `benchmarks/` directory not excluded from package build.
  **Why**: `benchmarks/arrow-cache.rds` (~7 KB) and `benchmarks/arrow-vs-collapse-results.png` (~76 KB) and the R script itself would be bundled into the package tarball / installed package. These are development artifacts that should not ship to end users.
  **Fix**: Add `^benchmarks$` to `.Rbuildignore`.

- **[P2.4]** [cg-data-quality] `benchmarks/arrow-vs-collapse.R:~315–347` — `.arrow_weighted_mean()` and `.arrow_headcount()` apply `dplyr::mutate(across(dict_cols, as.character))` inside the Arrow lazy query. On live multi-file datasets, Arrow tries to unify dict indices across files during scan planning, before the cast transformation is applied, triggering `NotImplemented: Unifying differing dictionaries`.
  **Why**: Arrow's execution engine unifies dictionary encodings at the dataset scan level. The per-record `as.character` cast happens after unification, so it cannot prevent the unification error. The check fixture avoids this because all its files are written in the same Arrow session with compatible dict indices.
  **Fix**: Move the dict cast to after `collect()` in R. Remove `mutate(across(dict_cols, as.character))` from the Arrow chain; instead apply `for (col in dict_cols) set(dt, j = col, value = as.character(dt[[col]]))` on the collected `data.table`. This is what `.cast_dict_to_char()` was intended to do before it became dead code (P2.1).

- **[P2.5]** [cg-performance] `benchmarks/arrow-vs-collapse.R:~365` — `e2e_3_push()` performs `1 + N_poverty_lines` full Arrow scans (1 for mean, 1 per poverty line for headcount). With `POVERTY_LINES = c(2.15, 3.65)` this is 3 full scans versus E2E-2's 1.
  **Why**: The approach is structurally O(N_poverty_lines) in I/O. On the declared benchmark it cannot beat E2E-2 regardless of Arrow's aggregation speed, because it reads 3× more data. The decision document does not note this architectural constraint.
  **Fix**: Add a note in `e2e_3_push()` and the results document: "E2E-3 performs 1 + N_poverty_lines Arrow scans per call. With 2 poverty lines this is 3× the I/O of E2E-2. To be competitive, headcount for all poverty lines would need to be computed in a single scan using Arrow's conditional aggregation."

- **[P2.6]** [cg-documentation] `benchmarks/arrow-vs-collapse.R:~620` — `sort_fraction` decision message reports "Arrow sort overhead is 76% of E2E-1" but `io3_med - io2_med` includes dict-to-string cast time (`as.character` on all dim columns via Arrow `mutate`), not only sort time.
  **Why**: The decision "WORTH INVESTIGATING: Implement pre-sorted I/O and skip setorder() in compute_inequality()" is based on a measurement that conflates sort cost with cast cost. The true sort overhead (minus cast) is unknown and could be substantially lower.
  **Fix**: Change the decision message to "Arrow sort + cast overhead is 76% of E2E-1 (upper bound — includes dict cast time). True sort-only overhead is lower." Quantify the cast-only cost by adding an IO-3b approach that only casts without sorting.

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-architecture] `benchmarks/arrow-vs-collapse.R:~233` — `.normalize_arrow_result()` uses `BY_DIMS` from the global script environment instead of accepting it as a parameter.
  **Why**: Hidden coupling to global state; breaks without error if called in any context where `BY_DIMS` is not defined or has a different value.
  **Fix**: Add `by = BY_DIMS` as a default argument: `.normalize_arrow_result <- function(res, by = BY_DIMS)` and use `by` in the body.

- **[P3.2]** [cg-reproducibility] `benchmarks/arrow-vs-collapse.R:~820` — `results_path` has a hardcoded date `"2026-04-29-arrow-vs-collapse-results.md"`.
  **Why**: Re-running the script on any future date creates a second results file rather than overwriting the current one. The `date: 2026-04-29` YAML field in the output also becomes incorrect.
  **Fix**: `today_str <- format(Sys.Date(), "%Y-%m-%d")` in the config block; use `paste0(today_str, "-arrow-vs-collapse-results.md")` for the filename and `today_str` for the YAML `date:` field.

- **[P3.3]** [cg-testing] `benchmarks/arrow-vs-collapse.R:~395` — the correctness check fixture writes one fresh Parquet file per survey in the same R session, producing compatible dict encodings. This does not reproduce the multi-file dict incompatibility that causes E2E-3 to fail on live data.
  **Why**: The fixture is a false positive for E2E-3 correctness in the production environment. A stronger fixture would write files simulating the dict incompatibility (e.g. two separate `write_parquet` calls with different factor level orderings).
  **Fix**: After fixing P2.4, verify E2E-3 passes on the live dataset and add a note that the fixture only tests logical correctness, not Arrow compatibility.

- **[P3.4]** [cg-code-quality] `benchmarks/arrow-vs-collapse.R:~669` — `factor(approach, levels = rev(approach))` applies `rev()` to the full column vector which may contain duplicate approach names if the results table has multiple rows per approach.
  **Why**: Passing a levels vector with duplicates to `factor()` emits a warning and de-duplicates non-deterministically, potentially reordering the plot bars.
  **Fix**: `factor(approach, levels = rev(unique(bm_results$approach)))`.

---

### ✅ Passed

- **cg-version-control** (commit quality): Conventional commit format, detailed body with key numbers, correct branch (`bm-arrow`). No credentials or secrets.
- **cg-reproducibility** (seeds): `set.seed(42L)` (survey sample) and `set.seed(123L)` (correctness fixture) present and correctly scoped.
- **cg-code-quality** (style): Indentation, section headers, and comment style consistent with `orchestration-strategy.R` conventions throughout.
- **cg-performance** (benchmark design): IQR reported via per-iteration quantiles; `bench::mark()` with `check = FALSE` and `memory = TRUE`; `tryCatch` guards on all timed approaches — all correct.
