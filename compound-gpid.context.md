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

- Location: UNC share `//w1wbgencifs01/pip/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow`
- Partition path: `country_code=X / surveyid_year=Y / welfare_type=Z / version=V / <pip_id>-0.parquet`
- 14 columns per file: `country_code`, `surveyid_year`, `welfare_type`, `version`, `pip_id`, `survey_acronym`, `welfare`, `weight`, `gender`, `area`, `educat4`, `educat5`, `educat7`, `age`
- Dictionary-encoded columns (integer index, not plain string): `gender`, `area`, `educat4`, `educat5`, `educat7`
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
- Column-pruned 6-col read: 0.524s — **68% faster** — TO BE ADOPTED in `load_surveys()`

### Pipeline E2E (I/O + compute, 15 surveys × headcount/gini/mean/median × gender/area/educat4)
- A1 Current (all cols): 2.370s
- A2 Column-pruned: 1.675s — **29% faster**
- A3 Arrow push-down (genuine, 2 scans): TBD — script updated, not yet re-run
