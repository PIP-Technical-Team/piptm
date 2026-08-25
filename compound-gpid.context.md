---
cg-schema-version: "2026-04-30"
---

# Project Context — Table Maker

Project-specific facts, data source conventions, and domain rules that help
Compound GPID agents work effectively in this codebase. Updated as new
knowledge is discovered.

---

## Arrow / Parquet Data Repository

### Physical layout

- Location: UNC share `//pip-server/pip/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow`
  (replace `pip-server` with the actual internal server name — not committed to version control)
- Partition path: `country_code=X / surveyid_year=Y / welfare_type=Z / version=V / <pip_id>-0.parquet`
- **Base columns**: 30 optional + required columns (see `piptm::pip_allowed_cols()`), plus
  one or more `welfare_*` columns per file (e.g. `welfare_lcu`, `welfare_ppp_2017_01_02`).
  Required (always present): `country_code`, `surveyid_year`, `welfare_type`, `version`,
  `pip_id`, `weight`. Optional breakdown dimensions: `gender`, `area`, `educat4`, `educat5`,
  `educat7`, `age`, `hsize`, `imp_wat_rec`, `imp_san_rec`, `electricity`, `lstatus`,
  `lstatus_year`, `empstat`, `empstat_2`, `empstat_year`, `empstat_2_year`,
  `industrycat10`, `industrycat10_2`, `industrycat10_year`, `industrycat10_2_year`,
  `industrycat4`, `industrycat4_2`, `industrycat4_year`, `industrycat4_2_year`.
- Dictionary-encoded columns (integer index, not plain string): `gender`, `area`, `educat4`, `educat5`, `educat7`
- All other optional columns are `int32` (not dictionary-encoded).
- Survey sizes range from ~7,500 to ~525,000 rows; median ~49,000 rows

### I/O characteristics (measured, 50-iteration resample, 15 surveys)

| Approach | Median (s) | Notes |
|---|---|---|
| All 14 columns | 1.658s | Current `load_surveys()` baseline |
| 6 needed columns | 0.524s | 68% faster — column pruning before `collect()` |
| 6 cols + Arrow sort | 3.337s | 77% overhead vs pruned; not worth it |

- Each scan over the UNC share carries ~400–600ms fixed overhead regardless of
  column count. Scan count matters as much as byte count.
- OS file cache can make single I/O measurements appear misleadingly fast
  (~157ms vs ~524ms repeated). Always benchmark with 30+ iterations.

### Arrow query rules

1. `select()` MUST happen before `collect()` to skip bytes on the network.
   Subsetting after `collect()` provides no I/O benefit.
2. Dictionary-encoded columns (`gender`, `area`, `educat4`, `educat5`,
   `educat7`) must be cast to character with
   `mutate(across(all_of(dict_cols), as.character))` before any
   `group_by + summarise` pushed across multiple Parquet files.
   Reason: files may encode the same category with different integer indices;
   Arrow raises `NotImplemented: Unifying differing dictionaries` without the cast.
3. After `collect()` into R, Arrow automatically converts dict columns to R
   factors. No cast needed for R-side grouping.
4. Arrow CAN sort rows (`arrange()`), but this is expensive over a network
   share (~800ms overhead for 15 surveys). R's `setorder()` on in-memory data
   is far cheaper.
5. **One file per partition — enforced**. Each leaf directory must contain
   exactly one `.parquet` file. `open_dataset()` on multiple files concatenates
   them in file-system order, breaking the within-survey welfare sort invariant
   that `compute_inequality()` relies on. `.build_parquet_paths()` aborts with
   an explicit error if more than one file is found. Do not remove this guard
   without re-establishing and testing the sort contract for the multi-file case.
   See `.cg-docs/solutions/data-quality/2026-05-21-arrow-multifile-partition-sort-violation.md`.

### Schema expansion rule (established 2026-06-11)

When adding new optional columns to the schema, **6 locations must be updated
atomically** — missing any one causes false "extra column" errors:

1. `piptm/R/schema.R` — `pip_arrow_schema()` fields list
2. `piptm/R/validate_parquet.R` — `.vp_canonical_schema()` Arrow schema
3. `pipdata/R/arrow_prep.R` — `validate_pre_write()` §4.8 `optional_dim_cols`
4. `pipdata/R/arrow_prep.R` — `prepare_for_arrow()` `optional_dim_cols`
5. `pipdata/R/arrow_generation.R` — `.validate_for_write()` `optional_dims`
6. `pipdata/R/arrow_generation.R` — `write_survey_parquet()` `dim_cols`

Pass-through `int32` columns (no standardisation needed) also require an
explicit `as.integer()` cast in `prepare_for_arrow()` Step 3b, because survey
microdata often stores them as `numeric` and Arrow's `as_arrow_table(schema)`
rejects type mismatches.

See `.cg-docs/solutions/data-quality/2026-06-11-arrow-schema-expansion-propagation-pattern.md`.

---

## Measure Registry and Arrow Feasibility

The `.MEASURE_REGISTRY` in `R/measures.R` maps 19 measures to three families:
`poverty` (5), `inequality` (2), `welfare` (12).

Arrow push-down feasibility (as of 2026-04-30):

| Feasible in Arrow | Measures |
|---|---|
| ✅ Yes | `headcount`, `poverty_gap`, `severity`, `watts`, `pop_poverty`, `mean`, `sd`, `var`, `min`, `max`, `nobs`, `sum`, `mld` |
| ⚠️ Arrow unweighted only (wrong for PIP) | `p10`, `p25`, `p75`, `p90`, `median` — PIP uses weighted quantiles; Arrow's `quantile()`/`median()` are unweighted |
| ❌ No | `gini` — requires sorted welfare vector for Lorenz curve; not a single-pass scalar aggregate |

### Gini pre-sort contract (established 2026-05-21)

`compute_inequality()` does **not** sort welfare internally. The sort guarantee
flows from the upstream pipeline:
`pipdata::generate_arrow_dataset()` sorts by welfare on write →
`write_survey_parquet()` (Arrow preserves row order) →
`load_surveys()` (`open_dataset` + `collect` preserves order).

Enforcement layers:
- `is.unsorted(welfare_v)` guard in the `by = NULL` path of `compute_inequality()` — errors immediately.
- `length(files) > 1L` guard in `.build_parquet_paths()` — prevents multi-file concatenation from silently breaking the sort.
- `anyNA(welfare_v)` / `anyNA(w)` guards — prevent `collapse` silent NA-drop from biasing results.

If the upstream pipeline ever shards a survey into multiple files, the
multi-file guard fires immediately. Fix: write a single sorted file, then
remove the guard only after the sort contract is re-verified.
See `.cg-docs/solutions/data-quality/2026-05-21-arrow-multifile-partition-sort-violation.md`.

---

## Benchmark Conventions

- Use `proc.time()` splits to separate I/O and compute phases.
- Resample surveys each iteration (`set.seed(BASE_SEED + i)`) so per-iteration
  variance reflects realistic survey size mix, not just OS noise.
- Always run a correctness check on a small fixture before the benchmark loop.
- Adopt threshold: ≥15% improvement required to change the production pipeline.
- Benchmark scripts live in `benchmarks/`; results docs in
  `.cg-docs/solutions/performance-issues/`.

---

## Key Performance Numbers (2026-04-30, live data, 15 surveys, 50 iterations)

### Orchestration (in-memory compute, warm cache)
- Approach A (per-survey lapply): 0.432s
- Approach B (batch GRP): 0.127s — **72% faster** — ADOPTED

### Arrow I/O (cold network reads)
- Full 14-col read: 1.658s
- Column-pruned 6-col read: 0.524s — **68% faster** — ADOPTED (2026-05-21, `cols` param in `load_surveys()`)

### Pipeline E2E (I/O + compute, 15 surveys × headcount/gini/mean/median × gender/area/educat4)
- A1 Current (all cols): 2.370s
- A2 Column-pruned: 1.675s — **29% faster**
- A3 Arrow push-down (genuine, 2 scans): TBD — script updated, not yet re-run

---

## Architecture Notes

The {piptm} package follows a **Manifest-First with Lazy Validation** architecture. On load, it reads all `manifest_*.json` files from `PIPTM_MANIFEST_DIR` into memory (keyed by release ID). No microdata is loaded at startup — all loading is on-demand per `table_maker()` call.

The data pipeline is:

```
Raw survey microdata
→ {pipdata} harmonization
→ Clean survey datasets (.qs2)
→ Arrow/Parquet partitions (partitioned by country_code / year / welfare_type)
→ Release manifest (reproducibility contract)
→ {piptm} computation engine
→ Structured cross-tabulated measure outputs
```

Key internal components:
- `load_survey_microdata()` — manifest lookup + lazy file validation + Arrow loading
- `compute_fgt()`, `compute_gini()`, `compute_mean_welfare()`, etc. — core measure functions
- `compute_measures()` — orchestrator across surveys and breakdown dimensions
- `table_maker()` — top-level API function

## Related Resources

- `docs/project-context.md` — Detailed architecture, manifest schema, computation engine design
- `docs/roadmap.md` — Phased implementation plan
- `inst/schema/arrow-schema.json` — Arrow partition schema
- PIP platform: <https://pip.worldbank.org>

---

## Metadata Validation Patterns

When building metadata schemas (e.g., `table_maker(with_meta = TRUE)`), always separate **specification** (what the user requested) from **execution** (what actually happened):

### Specification = Request
- Preserve original parameter values (may be NULL)
- Store unresolved parameters (e.g., `release = NULL` before resolution)
- Capture user intent exactly as provided

### Execution = Runtime Truth
- Store resolved values (e.g., `resolved_release`, `resolved_ppp`)
- Track actual outcomes (loaded vs excluded surveys)
- Record physical artifacts used (e.g., `ppp_column_used = "welfare_ppp_2021"`)

### Defensive Validation at Boundaries

Before consuming metadata fields, validate:

```r
# Check for NULL on critical fields
if (is.null(exec$resolved_release) || is.null(exec$ppp_column_used)) {
  cli::cli_abort("{.arg table_result} execution metadata incomplete")
}

# Validate data.table type before nrow()
if (!data.table::is.data.table(exec$loaded_surveys)) {
  cli::cli_abort("{.field execution$loaded_surveys} must be a data.table")
}

# Check schema completeness
req_cols <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
missing <- setdiff(req_cols, names(exec$loaded_surveys))
if (length(missing) > 0L) {
  cli::cli_abort("Missing columns: {.val {missing}}")
}
```

### Fail Loudly, Not Silently

Replace silent fallbacks with explicit errors:

```r
# ❌ BEFORE: Silent NA assignment
if (length(result) > 0L) value <- result[[1L]] else value <- NA_character_

# ✅ AFTER: Informative error
if (length(result) > 0L) {
  value <- result[[1L]]
} else {
  cli::cli_abort(
    c(
      "Resolution failed",
      "i" = "Expected value but got empty result",
      "i" = "Available options: {.val {options}}"
    )
  )
}
```

See `.cg-docs/solutions/data-quality/2026-08-24-metadata-schema-execution-truth-pattern.md` for full implementation details and testing patterns.

---

## Wiki Configuration

<!-- folder: wiki -->
<!-- audience: developers -->
<!-- tone: technical -->
