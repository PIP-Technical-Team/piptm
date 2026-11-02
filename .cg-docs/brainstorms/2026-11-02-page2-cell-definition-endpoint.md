---
date: 2026-11-02
title: "Page 2 cell definition endpoint, and phasing of the metadata rework"
status: decided
scope: "Deep"
artifact-schema-version: 1
chosen-approach: "Approach A — three-section cell explanation (Who is in the table, What a cell is, What is shown in that cell) backed by a dedicated inst/extdata/tm_cell_prose.yaml and served by POST /cell-definition; metadata rework deferred to Phase 2"
tags: [cell-definition, page-2, api, endpoint, shares, description-endpoint, metadata, table-maker, ux, wording, phasing]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Page 2 Cell Definition Endpoint, and Phasing of the Metadata Rework

## Context

Page 2 of the PIP Table Maker UI is where the user makes the decisions that
define the table: the sample-base filter (Decision 1), the analysis variable
and statistics (Decision 2), and the covariates placed in the four layout
slots (Decision 3). Today page 2 shows only two terse caption lines under the
live preview (`docs/ui-description.md:120-121`). The user commits to a
configuration without a clear explanation of what a cell in the resulting
table represents.

This brainstorm covers two linked things:

1. **A new page-2 cell explanation** that tells the user, in plain prose, three
   things: who is in the table (the total population), what a cell identifies
   (how rows, columns, super-rows and super-columns define a cell and its own
   population), and what measures are shown inside that cell.
2. **A scope decision** on whether the parallel metadata rework (splitting
   `/description`'s output into a downloadable metadata document, and moving
   it out of `table_maker()`) belongs in this task or a later one.

### Prior work this builds on

- `.cg-docs/brainstorms/2026-09-14-shares-cell-definition-structured-table.md`
  (decided) and its two plans — introduced the four-column shares table inside
  `/description`'s `cell_definition` section and fixed the denominator
  semantics. It did not fix register, and deferred binary-variable phrasing
  (`"<label> is true"`).
- `.cg-docs/brainstorms/2026-09-08-description-html-renderer-and-wording.md` —
  established the lean-dependency, simplest-viable-option posture.
- `.cg-docs/brainstorms/2026-06-01-cell-share-measures.md` — defined the three
  share measures.
- `compound-gpid.context.md` → "API Endpoint Contract Conventions" — two logged
  incidents (2026-10-22, 2026-10-26) from cross-endpoint payload-shape
  coupling. Directly relevant because this task adds a second surface for text
  that `/description` also produces.

### Source-of-truth decision

`docs/ui-description.md` and the engine have drifted (`Share (%)` vs three
share measures, Theil/Palma vs `mld`, percentiles). **The engine is
authoritative**: `GET /statistics`, `GET /analysis-variables` and the compute
functions (`R/compute_shares.R`, `R/compute_poverty.R`,
`R/compute_inequality.R`, `R/compute_summary_stats.R`) define what exists. Two
UI facts from `docs/ui-description.md` remain binding:

- **Step 3 renders one tab per survey** (`:135`): income-based and
  consumption-based surveys never share a cell. Every table described is a
  single survey.
- **At most four statistics per table** (`:23`), stacked inside each cell.

### What exactly is being described

**The table being described is the one `POST|GET /table` produces and Step 3
renders.** Page 2 shows a prospective description of the page-3 artifact.

> Given the same parameters, `/cell-definition` describes the exact table
> `/table` will return.

## Requirements

### Purpose

Explain a cell of the configured table clearly and readably before computation
runs. Every response covers three things, in order, without repeating the
population inside every measure line:

1. **Who is in the table** — the total population of the table (after any
   sample-base filter).
2. **What a cell is** — which combination of rows, columns, super-rows and
   super-columns identifies a cell, illustrated with one concrete example cell
   named in natural English (*"women with no schooling in urban areas"*).
3. **What is shown in that cell** — one bullet per selected measure, naming the
   concrete example group directly rather than using vague pronouns, and for
   share measures stating explicitly **which group counts as 100%** and what
   the cells add up to.

It is **not interactive**: no tooltips, no hover states, no placeholder tokens
for client-side substitution. The text is written out in full on the server.

### Users

- **Primary**: the ITS team, who render the JSON on page 2.
- **Indirect**: PIP platform end users — researchers, policymakers, analysts.

### Inputs

The page-2 parameter set: `analysis_var`, `measures`, `poverty_line`,
`column`, `row`, `superColumn`, `superRow`, `filter_base`, `ppp`, `release`,
`pop_share_threshold`, plus an optional `welfare_type` (`"INC"` / `"CON"`, so
prose can say "income" or "consumption" instead of "welfare").

### Outputs

JSON, standard envelope, containing the three sections in a fixed, easy-to-render
structure.

### Constraints

- **No results, no numbers.** Pure function of (request parameters + release
  registry + `tm_cell_prose.yaml`). Never calls `table_maker()`, never touches
  Arrow or microdata, and reports no computed values or sample counts.
- **No textbook definitions of statistics.** Do not explain that Gini runs from
  0 to 1; state what the cell reports.
- **No bloating `tm_variable_spec.yaml` or `tm_measure_spec.yaml`.** All prose
  fragments live in a dedicated file (`inst/extdata/tm_cell_prose.yaml`) that
  serves only cell explanations.
- **One source of truth**: `/description`'s `cell_definition` section delegates
  to the same builder from day one.
- **No new package dependencies.**

### Scope — explicitly out

- The metadata rework and `POST /table/download/metadata` (deferred to Phase 2).
- Row-percentage and column-percentage share measures (engine feature request,
  captured as a roadmap idea).
- Changes to `/table`, `/table/download`, or the Approach C response format.
- i18n (English only).

## Decision-to-text map

| Decision | Parameter | Effect on the three sections |
|---|---|---|
| Sample-base filter | `filter_base` | **Who is in the table**: names the included group and states that everyone else is excluded first. For shares, establishes that this filtered group is the table's 100%. When absent: *"Everyone in the survey. No filter is applied."* |
| Layout slots | `column`, `row`, `superColumn`, `superRow` | **What a cell is**: lists the active dimensions with their table roles (`education level (row), gender (column), and area (column group)`) and builds the concrete first-cell phrase (*"women with no schooling in urban areas"*). When all four are empty: states that the table has a single cell covering the whole table population. |
| Analysis variable | `analysis_var` | **What is shown in that cell**: selects the subject noun (`daily income`, `age`, `years of education`, `household size`) or, for binary targets in shares, the target predicate (`are employed`, `have access to electricity`). |
| Statistics | `measures` | **What is shown in that cell**: one bullet per measure, in request order. Shares get an extra `100% = …` line stating the denominator and what the cells add up to. |
| Poverty line | `poverty_line`, `ppp` | Included in poverty measure bullets (`living on less than $2.15 a day (2021 PPP)`) and in **What a cell is** when `pov_status` occupies a layout slot. |
| PPP year | `ppp` | Appended as `(2021 PPP dollars)` on money measures; omitted for non-money continuous variables (`age`, `educy`, `hsize`) and shares. |
| `pov_status` in a layout slot + non-poverty measure | `row = "pov_status"`, etc. | **What a cell is** defines "poor" against the poverty line (`living on less than $3.65 a day`), and a trailing note clarifies that the line only splits the rows/columns and does not change how the measure itself is calculated. |
| Suppression threshold | `pop_share_threshold` | Optional trailing note when at least one slot is active: cells below the threshold appear as `"NR"`. |

### The shares case in detail

There are three populations in play for every share calculation (all weighted
by `weight`):

- **$T$ (Table / Sample Base)** — everyone in the survey who passed
  `filter_base` (e.g. *all adults aged 20–65*).
- **$C$ (Cell)** — everyone in $T$ who falls into that specific cell (e.g.
  *rural women aged 20–65*).
- **$C \cap Y=1$ (Target in cell)** — everyone in $C$ with the binary analysis
  variable equal to 1 (e.g. *employed rural women aged 20–65*).

Verified against `R/compute_shares.R:104-185`:

| Measure | Formula | Counts (Numerator) | Out of / 100% (Denominator) | What all cells sum to |
|---|---|---|---|---|
| `pop_share` | $\text{pop}(C) / \text{pop}(T)$ | Everyone in the cell ($C$) | Everyone in the table ($T$) | **100%** across the whole table |
| `target_within_group_share` | $\text{pop}(C \cap Y=1) / \text{pop}(C)$ | People in the cell with $Y=1$ ($C \cap Y=1$) | Everyone in the cell ($C$) | **Does not sum to 100%** (each cell is its own rate) |
| `target_survey_share` | $\text{pop}(C \cap Y=1) / \text{pop}(T)$ | People in the cell with $Y=1$ ($C \cap Y=1$) | Everyone in the table ($T$) | **The overall rate of $Y=1$** in the table ($T$) |

**Why this is the core explanation problem:**
- For all non-share measures (mean, median, Gini, poverty headcount), the
  reference population is always $C$ (the cell).
- For shares, two different reference populations ($C$ vs $T$) live side by
  side. In particular, `target_within_group_share` and `target_survey_share`
  have the **exact same numerator** ($C \cap Y=1$, e.g. the 50 employed rural
  women) and differ **only** in whether the 100% denominator is the cell $C$
  (the 200 rural women → 25%) or the whole table $T$ (all 1,000 adults → 5%).
  Meanwhile `pop_share` and `target_survey_share` share the **exact same
  denominator** ($T$, all 1,000 adults) and differ in whether the numerator is
  all rural women ($C$) or only employed rural women ($C \cap Y=1$).

Therefore, every share measure in the response explicitly surfaces:
1. **`counts`** (the numerator group: $C$ or $C \cap Y=1$),
2. **`out_of`** (the 100% reference population: $T$ or $C$),
3. **`text`** (the plain-English sentence combining both), and
4. **`total_note`** (what all cells in the table add up to).

When no layout slot is selected (`by = NULL`), $C = T$, so `pop_share` is
always 100% and `target_within_group_share` and `target_survey_share` collapse
to the same percentage; the text states this directly.

## Approaches Considered

### Approach A: Three-section cell explanation backed by a dedicated `tm_cell_prose.yaml` (CHOSEN)

Create a standalone `inst/extdata/tm_cell_prose.yaml` curated solely for
natural-English cell explanations (per-category phrases with head-noun
promotion for `"women"` / `"men"`, binary target predicates, and measure
templates). Build an exported pure R function that takes the page-2 parameters
and returns the three sections (*Who is in the table*, *What a cell is*,
*What is shown in that cell*). Expose it via `POST /cell-definition` and have
`/description`'s `cell_definition` section delegate to it.

- **Pros**: Produces natural, specific English (*"women with no schooling in
  urban areas"*) without polluting `tm_variable_spec.yaml` or
  `build_variable_registry()`. Wording can be edited without rebuilding release
  registries. Single source of truth across page 2 and `/description`. Guarded
  against registry drift by a `testthat` sync test plus a runtime fallback to
  `ui_label`.
- **Cons**: Introduces a second YAML file (~60–80 short category phrases, ~22
  target predicates, 23 measure templates) that must stay in sync with the
  variable and measure specs.
- **Effort**: Medium.

### Approach B: Put prose fields inside `tm_variable_spec.yaml` and `tm_measure_spec.yaml`

Add `phrase_frame`, `predicate`, and `plain_definition` directly to the
existing registry YAML files and propagate them through
`build_variable_registry()`.

- **Pros**: Single file per domain.
- **Cons**: Rejected — makes the core registry grow with prose concerns that
  `/categories`, `/covariates`, `/analysis-variables` and `/statistics` do not
  use, and requires rebuilding per-release registry JSONs for every wording
  tweak.
- **Effort**: Medium.

### Approach C: Generic join using only existing `ui_label` values

Compose sentences mechanically from existing registry labels (`"people whose
gender is Female and whose education level is None"`).

- **Pros**: Zero new content to author.
- **Cons**: Rejected — reads badly, especially for categories like `"None"` and
  for the three share measures.
- **Effort**: Small.

## Decision

**Approach A.**

### 1. Dedicated prose file: `inst/extdata/tm_cell_prose.yaml`

Kept separate from `tm_variable_spec.yaml` and `tm_measure_spec.yaml` so the
core registry pipeline stays clean:

```yaml
schema_version: "1.0"

variables:
  gender:
    slot_noun: "gender"
    categories:
      "0": { noun: "women", phrase: "who are women" }
      "1": { noun: "men",   phrase: "who are men" }

  area:
    slot_noun: "area"
    categories:
      "0": { phrase: "in rural areas" }
      "1": { phrase: "in urban areas" }

  educat4:
    slot_noun: "education level"
    categories:
      "1": { phrase: "with no schooling" }
      "2": { phrase: "with primary education" }
      "3": { phrase: "with secondary education" }
      "4": { phrase: "with tertiary education" }

  age_group:
    slot_noun: "age group"
    categories:
      "1": { phrase: "aged 0–14" }
      "2": { phrase: "aged 15–24" }
      "3": { phrase: "aged 25–64" }
      "4": { phrase: "aged 65 and older" }

targets:
  employed:       { predicate: "are employed",                   rate_noun: "employment rate" }
  unemployed:     { predicate: "are unemployed",                 rate_noun: "unemployment rate" }
  in_labor_force: { predicate: "are in the labor force",         rate_noun: "labor force participation rate" }
  imp_wat_rec:    { predicate: "have an improved water source",  rate_noun: "improved-water access rate" }
  imp_san_rec:    { predicate: "have improved sanitation",       rate_noun: "improved-sanitation rate" }
  electricity:    { predicate: "have access to electricity",     rate_noun: "electricity access rate" }

measures:
  mean:   "average {var} of {cell_pop}"
  median: "{var} of the middle person in {cell_pop}, with half below and half above"
  gini:   "{var} inequality within {cell_pop}"
```

**Composition rule (head-noun promotion):** if an active variable supplies a
category `noun` (e.g. `gender` → `"women"`, `age_group` → `"adults aged
20–65"`), it becomes the head noun; otherwise the head noun is `"people"`. All
other active categories attach in slot order as trailing phrases (`"with no
schooling"`, `"in urban areas"`).

**Filter length rule (preventing run-on cell phrases):**
- **Who is in the table ($T$)** always writes out the full filter in plain
  English.
- In **What a cell is ($C$)** and **What is shown in that cell**:
  - **Short filter** (at most 1 filter variable with at most 2 selected
    categories, e.g. *"adults aged 20–65"*): weave the filter phrase directly
    into the cell group name (*"rural women aged 20–65"*), and name the table
    population directly (*"all adults aged 20–65"*).
  - **Long filter** (2+ filter variables or 3+ selected categories): keep the
    cell group name limited to the slot categories (*"rural women with no
    schooling"*) and refer to the table population as *"everyone in the table
    (the filtered group above)"* so the measure lines never turn into run-on
    sentences.

**Drift guards:**
- A `testthat` sync test asserts that every variable, category code, and
  measure in `tm_variable_spec.yaml` / `tm_measure_spec.yaml` has a matching
  entry in `tm_cell_prose.yaml`.
- A runtime fallback uses `ui_label` (`"whose <ui_label> is <cat_label>"`) if
  an entry is ever missing, so production never errors on a prose lookup.

### 2. Approved sample texts

#### Example 1 — Welfare, three measures, three slots, filtered
*(Rows = Education level; Columns = Gender; Super-columns = Urban/Rural.
Filter = Age 20–65, Secondary education not completed. Measures = Mean,
Median, Gini.)*

> **Who is in the table**
> Adults aged 20–65 who did not complete secondary school. Anyone outside this
> group is excluded before the table is built.
>
> **What a cell is**
> Each cell is one combination of education level (row), gender (column), and
> area (column group).
> For example, the first cell is **women · with no schooling · in urban
> areas** — within the adults aged 20–65 who did not complete secondary school.
>
> **What is shown in that cell**
> - **Mean** — average daily income of those women (2021 PPP dollars)
> - **Median** — daily income of the middle person in that group, with half
>   earning less and half earning more
> - **Gini** — income inequality within that group

#### Example 2 — Shares, two slots, filtered
*(Rows = Area; Columns = Gender. Filter = Age 20–65. Analysis variable =
Employed. Measures = all three shares.)*

> **Who is in the table ($T$)**
> All adults aged 20–65 in the survey.
>
> **What a cell is ($C$)**
> Each cell is one combination of area (row) and gender (column).
> For example, the first cell is **rural women aged 20–65**.
>
> **What is shown in that cell**
> The cell shows three percentages. Notice how they count the same or different
> groups against two different **100% totals** (either the cell or the whole
> table):
> - **Population share** — what share of **all adults aged 20–65** are **rural
>   women**?
>   - *Counting*: rural women aged 20–65 (everyone in the cell)
>   - *Out of (100%)*: all adults aged 20–65 (everyone in the table)
>   - *What cells add up to*: **100%** across the whole table.
> - **Target share within cell** — what share of **rural women aged 20–65** are
>   **employed**?
>   - *Counting*: employed rural women aged 20–65
>   - *Out of (100%)*: rural women aged 20–65 (everyone in that one cell)
>   - *What cells add up to*: **does not add up to 100%** (each cell is its own
>     employment rate).
> - **Target share in sample base** — what share of **all adults aged 20–65**
>   are **employed rural women**?
>   - *Counting*: employed rural women aged 20–65 *(same group as above)*
>   - *Out of (100%)*: all adults aged 20–65 (everyone in the table)
>   - *What cells add up to*: **the overall employment rate** of adults aged
>     20–65 across the table.

#### Example 3 — Poverty measures, two slots, no filter
*(Rows = Urban/Rural; Columns = Gender. No filter. Poverty line = $2.15/day.
Measures = Headcount, Poverty gap, Population in poverty.)*

> **Who is in the table**
> Everyone in the survey. No filter is applied.
>
> **What a cell is**
> Each cell is one combination of area (row) and gender (column).
> For example, the first cell is **women in rural areas**.
>
> **What is shown in that cell**
> - **Poverty headcount** — out of all women in rural areas, the percentage
>   living on less than $2.15 a day (2021 PPP)
> - **Poverty gap** — how far below $2.15 a day rural women fall on average,
>   expressed as a percentage of the $2.15 line
> - **Population in poverty** — the number of women in rural areas living on
>   less than $2.15 a day

#### Example 4 — Poverty status used as a row, with a welfare measure
*(Rows = Poverty status at $3.65/day; Columns = Gender. Filter = Rural areas.
Measure = Mean welfare.)*

> **Who is in the table**
> Everyone living in rural areas. Anyone in urban areas is excluded first.
>
> **What a cell is**
> Each cell is one combination of poverty status (row) and gender (column),
> where "poor" means living on less than $3.65 a day (2021 PPP).
> For example, the first cell is **poor women in rural areas**.
>
> **What is shown in that cell**
> - **Mean** — average daily income of poor women in rural areas (2021 PPP
>   dollars)
>
> *Note: the $3.65 line is only used to split the rows into poor and non-poor.
> It does not change how the average itself is calculated.*

#### Example 5 — No rows or columns selected (single-cell table)
*(No slots assigned. Filter = Age 20–65. Measure = Mean welfare.)*

> **Who is in the table**
> Adults aged 20–65. Everyone else is excluded.
>
> **What a cell is**
> No row or column variables are selected, so the table has a single cell
> covering all adults aged 20–65.
>
> **What is shown in that cell**
> - **Mean** — average daily income of adults aged 20–65 (2021 PPP dollars)

### 3. Proposed JSON response shape (`POST /cell-definition`)

```json
{
  "status": "success",
  "data": {
    "schema_version": "1.0",
    "who_is_in_table": {
      "title": "Who is in the table",
      "filtered": true,
      "text": "All adults aged 20\u201365 in the survey. That group is the 100% for the table."
    },
    "what_a_cell_is": {
      "title": "What a cell is",
      "dimensions": [
        { "slot": "rows",    "varname": "educat4", "label": "education level", "example_category": "None" },
        { "slot": "columns", "varname": "gender",  "label": "gender",          "example_category": "Female" }
      ],
      "structure_text": "Each cell is one combination of education level (row) and gender (column).",
      "example_cell_label": "women with no schooling, aged 20\u201365",
      "example_text": "For example, the first cell is women with no schooling, aged 20\u201365."
    },
    "what_is_shown": {
      "title": "What is shown in that cell",
      "intro": "The cell shows three percentages. They differ in which group counts as 100%:",
      "measures": [
        {
          "key": "pop_share",
          "label": "Population share",
          "counts": "rural women aged 20\u201365 (everyone in the cell)",
          "out_of": "all adults aged 20\u201365 (everyone in the table)",
          "text": "Out of all adults aged 20\u201365 in the survey, the percentage who are rural women.",
          "total_note": "All cells in the table add up to 100%."
        },
        {
          "key": "target_within_group_share",
          "label": "Target share within cell",
          "counts": "employed rural women aged 20\u201365",
          "out_of": "rural women aged 20\u201365 (everyone in that one cell)",
          "text": "Out of rural women aged 20\u201365, the percentage who are employed.",
          "total_note": "Does not add up to 100% (each cell is its own employment rate)."
        },
        {
          "key": "target_survey_share",
          "label": "Target share in sample base",
          "counts": "employed rural women aged 20\u201365",
          "out_of": "all adults aged 20\u201365 (everyone in the table)",
          "text": "Out of all adults aged 20\u201365 in the survey, the percentage who are employed rural women.",
          "total_note": "All cells together add up to the overall employment rate of adults aged 20\u201365."
        }
      ]
    },
    "notes": []
  },
  "warnings": [],
  "errors": [],
  "meta": { "release": "20260206" }
}
```

Why this JSON is easy for ITS to render:
- Fixed top-level keys (`who_is_in_table`, `what_a_cell_is`, `what_is_shown`,
  `notes`) matching the three headings one-for-one. No polymorphic fields.
- Every `measures[]` item has `{key, label, text, total_note}` (`total_note` is
  `null` for non-share measures). `key` lets the UI filter by the active
  measure chip on page 3.

### 4. Part 3 — Phasing recommendation

**Two phases. The metadata rework is out of scope for this task.**

- **Phase 1 (this task)**: `inst/extdata/tm_cell_prose.yaml`, the shared cell
  explanation builder, `POST /cell-definition`, delegation of `/description`'s
  `cell_definition` section to the same builder, and `docs/api-contract.md`
  updates.
- **Phase 2 (follow-up task)**: rework `/description` into a downloadable
  metadata document (retaining PPP, release, surveys loaded/excluded,
  suppression summary, warnings; removing overlap with page 2), add
  `POST /table/download/metadata`, and decouple request-derived metadata from
  `table_maker()`.

Rationale:
1. Phase 1 is additive and touches no existing response contract; Phase 2
   modifies `table_maker(include_metadata=)`, `/table`'s `meta` field, and
   `/description`.
2. Deciding what to remove from the metadata document requires page 2's final
   text to exist first.
3. Sharing the builder in Phase 1 prevents the cross-endpoint drift logged in
   `compound-gpid.context.md`.

## Devil's Advocate

1. **Problem validation** — Pre-validated by the 2026-09-14 shares reproduction
   and the user's side-by-side readability review in this brainstorm.
2. **Simplicity check** — Separating `tm_cell_prose.yaml` from
   `tm_variable_spec.yaml` adds one file, but avoids touching
   `build_variable_registry()` and per-release JSON artifacts. A `testthat`
   sync test eliminates the drift risk cheaply.
3. **Effort-value check** — Authoring ~60–80 short category phrases plus ~22
   target predicates and 23 measure templates in one YAML file is small (~1 day)
   and directly produces the jump in readability between the rejected and
   approved samples.
4. **Charter alignment** — Fully deterministic, reproducible, zero Arrow/data
   access, and cross-checked against `compute_shares.R`, `compute_poverty.R`,
   `compute_inequality.R`, and `compute_summary_stats.R`.

## Next Steps

For handoff to `/cg-plan` (Phase 1):

1. Create `inst/extdata/tm_cell_prose.yaml` covering all variables/categories in
   `tm_variable_spec.yaml`, all binary target predicates, and all 23 measures in
   `tm_measure_spec.yaml`, plus a `testthat` sync test and runtime fallback.
2. Implement the exported pure builder (e.g. `build_cell_explanation()`)
   returning `who_is_in_table`, `what_a_cell_is`, `what_is_shown`, and `notes`.
3. Add `POST /cell-definition` in `inst/plumber/plumber.R` and
   `inst/plumber/helpers.R`.
4. Delegate `/description`'s `cell_definition` section to the new builder and
   update both Markdown and HTML renderers.
5. Update `docs/api-contract.md` and `SWAGGER_GUIDE.md`.
6. Add unit tests for all five approved examples and an API route test suite.
