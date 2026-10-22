---
date: 2026-10-20
title: "Population share threshold: NR cell masking and API value formatting"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Mask in table_maker() (NA + flag), format in API layer"
tags: [table_maker, pop_share_threshold, suppression, api, json, formatting]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Population share threshold: NR cell masking and API value formatting

## Context
`table_maker()` currently drops non-share measure rows for cells whose
population share is below `pop_share_threshold` (default 0.01). Share measures
are kept. The team wants a lower threshold, cell-level "NR" (Non-Representative)
masking instead of row dropping, and consistently formatted output values for
the UI.

Current behaviour (confirmed):
- Cell = `pip_id` x all `by` columns; only active when `by` is non-NULL.
- Share = cell weighted pop / survey weighted pop, both after `filter_base`
  (`.shares_setup()`, `R/compute_shares.R`).
- Below-threshold cells: non-share rows deleted (`R/table_maker.R:877-934`);
  warning lists cells with pop_share; `suppressed_cells` stored in metadata.

## Requirements
| # | Decision |
|---|---|
| D1 | Default `pop_share_threshold = 1e-3` (0.1%; revised from the originally requested 0.01%) in `table_maker()` and plumber `/table`. `NULL` still disables. Validation unchanged. |
| D2 | Denominator unchanged: survey weighted population after `filter_base`. |
| D3 | Rows are kept, not dropped. |
| D4 | ALL measures in a below-threshold cell are masked, including share measures. |
| D5 | Hard suppression: the real value of a masked measure is never sent. |
| D6 | API `value` is a string in every row: formatted number or `"NR"`. Genuine missing stays JSON `null`. |
| D7 | Masking is evaluated on the unrounded, unscaled share. |
| D8 | `population` column removed from `table_maker()` output (UI does not use it). Internal `population` in `.shares_setup()` unaffected. |
| D9 | Warning kept, listing each NR cell with its pop_share; reword from "Suppressing non-share measures" to "Reporting N cells as NR (population share < threshold)". |
| D10 | `description_metadata$suppressed_cells` kept as-is; `/description` wording updated to "reported as NR". |
| D11 | Scale x100: `gini`, `headcount`, `poverty_gap`, `severity`, `pop_share`, `target_within_group_share`, `target_survey_share`. Unchanged: `mld`, `watts`, `mean`, `median`, `sd`, `var`, `min`, `max`, `p10`-`p90`, `sum`. |
| D12 | 2 decimals for all measures; `pop_poverty` and `nobs` as whole numbers. |
| D13 | `mean` (and other amount measures) never rescaled, even for 0/1 `analysis_var`; percentages come from share measures. |
| D14 | Formatting (scale, decimals, `"NR"`) happens only in the API layer via a new helper (e.g. `format_table_output()`); `table_maker()` keeps numeric `value`. |
| D15 | `table_maker()` marks masked rows with `value = NA_real_` and a new character column `flag = "NR"` (`NA` otherwise). `flag` is consumed by the formatter and not emitted in API JSON. |

## Approaches Considered

### Approach 1: NR representation in JSON
1. **String `value` everywhere** (chosen, pending UI confirmation): simplest for
   UI display; numbers become text, so UI sorting/charting must coerce.
2. **Numeric `value` + `flag` field**: type-stable contract; requires UI to read
   a new field. Rejected for now by user preference.
3. **Numeric `value` + `value_display`**: rejected; leaks masked values (violates D5).

### Approach 2: Where masking/formatting lives
- **A. Inside `table_maker()`**: one output everywhere, but R users get strings
  and scaled values.
- **B. API layer only** (chosen): R users keep numeric results; formatting is a
  cheap vectorized pass.

### Approach 3: Share measures under masking
- Keep shares visible (initially chosen, then reversed).
- Mask everything (chosen): below the threshold no measure is significant.

### Approach 4: Binary-variable mean
- A. Never rescale `mean` (chosen): one scale rule per measure name.
- B. x100 when 0/1: rejected; variable-dependent scale and edge cases.

## Decision
Masking in `table_maker()` (value -> NA, flag -> "NR", all measures, no row
drops, `population` column removed); presentation formatting in the API layer
producing string `value` with x100 scaling for proportion/Gini measures,
2 decimals (whole numbers for counts), and `"NR"` for masked cells.

## Risks and notes
- Option 1 (string `value`) changes the type of an existing contract field;
  UI must coerce for sorting/charts. Revisit option 2 if this causes issues.
- `/table/download` (`inst/plumber/plumber.R:288`) coerces `value` with
  `as.numeric()`; must be changed to preserve `"NR"` (otherwise NR silently
  becomes NA in CSV/RDS).
- D9/D10 intentionally expose pop_share of NR cells in warnings/metadata
  (explains why a cell is NR); measure values are never exposed.
- `nobs` is masked too under D4.
- Distinguish NR from genuine NA in the formatter: `flag == "NR"` -> `"NR"`;
  `is.na(value)` otherwise -> JSON `null`.

## Next Steps
1. `R/table_maker.R`: default 1e-3; replace row-drop with `value := NA_real_`,
   `flag := "NR"` for all measures in matched cells; add `flag` column
   (always present); drop `population`; reword warning; update roxygen.
2. Description wording (`R/description_builder.R` and renderers): "suppressed"
   -> "reported as NR"; reflect that shares are also masked.
3. New exported helper `format_table_output(dt)`: x100 lookup (D11), 2-dp
   `formatC` / whole numbers for `pop_poverty`, `nobs` (D12), `"NR"` where
   `flag == "NR"`, `NA` kept as `NA`; drop `flag`. Fully vectorized.
4. `inst/plumber/plumber.R`: `/table` default 1e-3 and call formatter;
   `/table/download` keep `"NR"` strings instead of `as.numeric()`.
   Check `validate_table_input()` default/messages in `inst/plumber/helpers.R`.
5. Docs: `docs/api-contract.md`, `SWAGGER_GUIDE.md` (string `value`, no
   `population`, NR semantics, scales).
6. Tests: update `test-table-maker-suppression.R`, `test-table-maker-v2.R`,
   `test-phase4-table-and-endpoint.R`, `test-api-table-download.R`,
   endpoint tests; add formatter unit tests (scaling, decimals, NR vs NA).
7. Quick benchmark of formatter on ~1M rows.
8. Confirm string `value` with the UI team before release.
