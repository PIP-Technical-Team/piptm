# RE-ENGINEERED TECHNICAL SPECIFICATION & EXECUTION PROMPT
## `/description` Endpoint — Step 3 Dynamic Table Description Generation

---

## EXECUTIVE SUMMARY

**Objective:** Design and implement a new `/description` endpoint that generates factual, dynamically constructed natural-language descriptions of tabular results produced by `table_maker()`. This endpoint serves **Step 3 of the user experience** (ui-description.md), providing users with a comprehensive, human-readable overview of:

1. **What the table contains** (cell semantics, weighting, structure)
2. **Provenance metadata** (release, PPP year, surveys)
3. **User input specifications** (all Step 1 & 2 choices: surveys selected, filters applied, statistics chosen, layout covariates assigned)
4. **Execution details** (loaded vs. excluded surveys, suppression applied, warnings)
5. **Cell definition** (the most complex artifact — mathematically precise population definitions that adapt to all permutations of filters, analysis variables, measures, and covariates)

**Architectural Approach:**
A **3-tier decoupled system**:

1. **Backend Enhancement Layer**: Augment `table_maker()` return payload with `description_metadata` without breaking backward compatibility
2. **Description Data Model Layer**: Structured object with conditional section visibility logic
3. **Rendering Layer**: Pluggable functions (Markdown primary; extensible to HTML/JSON)

**Critical Constraint:** All descriptions must be **strictly factual and definitional** — no interpretation, no policy recommendations, no statistical methodology beyond naming measures.

---

## I. ARCHITECTURE & DATA FLOW

### 1.1 End-to-End Request Flow

```
┌─────────────────────────┐
│  API: /description      │
│  (new endpoint)         │
└────────────┬────────────┘
             │
             │ Receives same parameters as /table_maker
             ▼
┌──────────────────────────────────────────────────┐
│  Backend: table_maker()                          │
│  ┌────────────────────────────────────────────┐  │
│  │  1. Execute full computation pipeline      │  │
│  │  2. Capture execution logs:                │  │
│  │     - loaded_surveys (pip_id vector)       │  │
│  │     - excluded_surveys (with reasons)      │  │
│  │     - filters_applied (resolved labels)    │  │
│  │     - measures_computed (resolved labels)  │  │
│  │     - poverty_line (if applicable)         │  │
│  │     - suppression_events (cells + flags)   │  │
│  │     - warnings (as character vector)       │  │
│  └────────────────────────────────────────────┘  │
│                                                   │
│  Returns:                                         │
│  list(                                            │
│    data = <existing data.table>,                 │
│    description_metadata = <new payload>          │
│  )                                                │
└──────────────────┬───────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│  Description Builder: build_description_model()  │
│  ┌────────────────────────────────────────────┐  │
│  │  Input: description_metadata + params      │  │
│  │  Output: Structured Description Data Model │  │
│  │                                             │  │
│  │  Sections (with visibility flags):         │  │
│  │  - overview                                 │  │
│  │  - provenance                               │  │
│  │  - surveys_selected                         │  │
│  │  - filters_applied (conditional)            │  │
│  │  - statistics_selected                      │  │
│  │  - layout_configuration (conditional)       │  │
│  │  - cell_definition (COMPLEX)                │  │
│  │  - execution_summary                        │  │
│  │  - warnings (conditional)                   │  │
│  └────────────────────────────────────────────┘  │
└──────────────────┬───────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│  Renderer: render_description_markdown()         │
│  (or render_description_html/json)               │
│  ┌────────────────────────────────────────────┐  │
│  │  Input: Description Data Model             │  │
│  │  Output: Formatted Markdown text           │  │
│  └────────────────────────────────────────────┘  │
└──────────────────┬───────────────────────────────┘
                   │
                   ▼
        ┌──────────────────┐
        │  API Response    │
        │  (Markdown text) │
        └──────────────────┘
```

### 1.2 Backward Compatibility Strategy

**Current `table_maker()` behavior**: Returns a `data.table` directly.

**Enhanced behavior** (opt-in):
- Add parameter `include_metadata = FALSE` (default)
- When `FALSE`: return `data.table` (current contract preserved)
- When `TRUE`: return `list(data = <data.table>, description_metadata = <list>)`

**For `/description` endpoint**: Always call with `include_metadata = TRUE`.

---

## II. DESCRIPTION METADATA SCHEMA

### 2.1 Required Metadata Structure

The `description_metadata` payload returned by enhanced `table_maker()`:

```r
list(
  # ── Input Parameters (echoed) ────────────────────────────────────
  params = list(
    pip_id              = character(),  # as requested
    analysis_var        = character(1), # scalar
    measures            = character(),  # vector
    poverty_line        = numeric(1) or NULL,
    ppp                 = integer(1),
    release             = character(1),
    by                  = character() or NULL,
    filter_base         = named list or NULL,
    pop_share_threshold = numeric(1) or NULL
  ),

  # ── Provenance ───────────────────────────────────────────────────
  provenance = list(
    release      = character(1),
    ppp_year     = integer(1),
    generated_at = character(1)  # ISO8601 timestamp
  ),

  # ── Surveys ──────────────────────────────────────────────────────
  surveys = list(
    loaded = data.table(
      pip_id       = character(),
      country_code = character(),
      country_name = character(),
      year         = integer(),
      welfare_type = character()
    ),
    excluded = data.table(
      pip_id  = character(),
      reason  = character()  # e.g. "missing dimension: gender"
    )
  ),

  # ── Resolved Labels ──────────────────────────────────────────────
  resolved_labels = list(
    analysis_var = list(
      varname  = character(1),
      ui_label = character(1),
      tm_type  = character(1)  # "continuous" | "binary" | "categorical"
    ),

    measures = data.table(
      measure    = character(),  # internal key
      ui_label   = character(),  # from tm_measure_spec.yaml
      stat_group = character()   # "summary_statistics" | "poverty" | etc.
    ),

    filters = data.table(
      varname       = character(),
      ui_label      = character(),
      selected_codes = list(),  # each element is integer vector
      selected_labels = list()  # each element is character vector
    ) or NULL,

    covariates = data.table(
      slot     = character(),  # "columns" | "rows" | "super_columns" | "super_rows"
      varname  = character() or NA_character_,
      ui_label = character() or NA_character_,
      n_categories = integer() or NA_integer_
    )
  ),

  # ── Execution Logs ───────────────────────────────────────────────
  execution = list(
    n_surveys_loaded   = integer(1),
    n_surveys_excluded = integer(1),
    n_filters_applied  = integer(1),
    n_measures_computed = integer(1),
    suppression = list(
      triggered = logical(1),
      threshold = numeric(1) or NULL,
      n_cells_suppressed = integer(1),
      suppressed_cells = data.table(  # only when triggered
        pip_id     = character(),
        [by_vars]  = mixed,
        pop_share  = numeric()
      ) or NULL
    ),
    warnings = character() or NULL  # collected cli_warn messages
  )
)
```

### 2.2 Data Sources for Metadata Assembly

| Metadata Field | Source |
|----------------|--------|
| `params.*` | Function arguments (direct echo) |
| `provenance.release` | `piptm_current_release()` or explicit `release` param |
| `provenance.ppp_year` | `ppp` param |
| `provenance.generated_at` | `Sys.time()` at execution |
| `surveys.loaded` | Join result table `pip_id` with manifest metadata |
| `surveys.excluded` | Captured from cli_warn() during dimension pre-filter (lines 366-382 in table_maker.R) |
| `resolved_labels.analysis_var` | `piptm_variable_registry()[[analysis_var]]` |
| `resolved_labels.measures` | Join `measures` with `piptm_stat_groups()` |
| `resolved_labels.filters` | Join `filter_base` keys with `piptm_filter_categories()` |
| `resolved_labels.covariates` | Join `by` with `piptm_layout_covariates()` |
| `execution.suppression.*` | Captured from pop_share_threshold logic (lines 496-566 in table_maker.R) |
| `execution.warnings` | Accumulated via custom warning handler |

---

## III. DESCRIPTION DATA MODEL SPECIFICATION

### 3.1 Structured Model with Conditional Visibility

```r
description_model <- list(

  # ── Section: Overview ────────────────────────────────────────────
  overview = list(
    visible = TRUE,  # always shown
    title   = "Table Overview",
    content = list(
      structure_description = character(1),
      # Example: "This table presents <N> statistics for each of <M> selected surveys,
      #           broken down by <covariates>. All calculations use survey sampling weights."
      weighting_note = "All calculations use survey sampling weights.",
      ppp_note = "Welfare values are expressed in <YYYY> PPP international dollars per day."
    )
  ),

  # ── Section: Provenance ──────────────────────────────────────────
  provenance = list(
    visible = TRUE,
    title   = "Data Provenance",
    content = list(
      release_id   = character(1),
      ppp_year     = integer(1),
      generated_at = character(1)
    )
  ),

  # ── Section: Surveys Selected ────────────────────────────────────
  surveys_selected = list(
    visible = TRUE,
    title   = "Surveys Selected",
    content = list(
      n_loaded   = integer(1),
      n_excluded = integer(1),
      loaded_list = data.table(
        country_name = character(),
        year         = integer(),
        welfare_type_label = character()  # "Income" | "Consumption"
      ),
      excluded_list = data.table(
        pip_id = character(),
        reason = character()
      ) or NULL  # NULL if none excluded
    )
  ),

  # ── Section: Filters Applied ─────────────────────────────────────
  filters_applied = list(
    visible = logical(1),  # TRUE if filter_base non-NULL
    title   = "Sample Base Filters",
    content = if (visible) {
      list(
        description = "The sample base has been filtered to include only the weighted population matching ALL of the following criteria:",
        filters = data.table(
          variable = character(),  # ui_label
          selected_categories = character()  # comma-separated labels
        )
      )
    } else NULL
  ),

  # ── Section: Statistics Selected ─────────────────────────────────
  statistics_selected = list(
    visible = TRUE,
    title   = "Statistics Computed",
    content = list(
      analysis_var_label = character(1),
      analysis_var_type  = character(1),
      measures = data.table(
        measure_label = character(),
        stat_group    = character()
      ),
      poverty_line = list(
        applicable = logical(1),
        value      = numeric(1) or NULL
      )
    )
  ),

  # ── Section: Layout Configuration ────────────────────────────────
  layout_configuration = list(
    visible = logical(1),  # TRUE if any `by` assigned
    title   = "Table Layout",
    content = if (visible) {
      list(
        covariates = data.table(
          slot         = character(),  # "Columns", "Rows", "Super Columns", "Super Rows"
          covariate    = character() or NA_character_,
          n_categories = integer() or NA_integer_
        )
      )
    } else {
      list(
        note = "No layout covariates assigned. Each survey produces aggregate national-level estimates."
      )
    }
  ),

  # ── Section: Cell Definition ─────────────────────────────────────
  cell_definition = list(
    visible = TRUE,  # always shown
    title   = "Cell Definition",
    content = list(
      population_scope = character(1),
      # Dynamically generated sentence; see Section IV
      measure_interpretation = character()  # vector of sentences, one per measure
    )
  ),

  # ── Section: Execution Summary ───────────────────────────────────
  execution_summary = list(
    visible = TRUE,
    title   = "Execution Details",
    content = list(
      n_surveys_loaded   = integer(1),
      n_surveys_excluded = integer(1),
      suppression = list(
        applied   = logical(1),
        threshold = numeric(1) or NULL,
        n_cells   = integer(1) or 0L
      )
    )
  ),

  # ── Section: Warnings ────────────────────────────────────────────
  warnings = list(
    visible  = logical(1),  # TRUE if execution.warnings non-empty
    title    = "Warnings",
    content  = if (visible) character() else NULL
  )
)
```

### 3.2 Conditional Visibility Rules

| Section | Visibility Condition |
|---------|---------------------|
| `overview` | Always visible |
| `provenance` | Always visible |
| `surveys_selected` | Always visible |
| `filters_applied` | `visible = !is.null(filter_base)` |
| `statistics_selected` | Always visible |
| `layout_configuration` | `visible = !is.null(by) && length(by) > 0` |
| `cell_definition` | Always visible |
| `execution_summary` | Always visible |
| `warnings` | `visible = length(execution.warnings) > 0` |

---

## IV. CELL DEFINITION ALGORITHM & MATRIX

**The Core Challenge:** Generate mathematically accurate natural-language descriptions of cell population scope that adapt to all permutations of:

- **Filter base** (`filter_base`): Restricts sample before analysis
- **Analysis variable** (`analysis_var`): What is being measured
- **Measure type** (`measures`): Especially critical for "shares" family
- **Layout covariates** (`by`): Further disaggregation

### 4.1 Population Scope Logic

**Base principle**: The cell population is defined by the **intersection** of:
1. Filter base conditions (if any)
2. Covariate group membership (if any)
3. Measure-specific targeting (for "shares" measures with binary `analysis_var`)

### 4.2 Algorithmic Decision Tree

```
START: Building cell definition sentence

1. ── Determine base population ────────────────────────────────────

   IF filter_base IS NULL:
       base_pop = "the total weighted population of the survey"
   ELSE:
       base_pop = "the weighted population that is {filter_conditions}"

       WHERE filter_conditions =
           JOIN(
               FOR EACH filter_var IN filter_base:
                   "{ui_label} in [{selected_labels_comma_sep}]"
               SEPARATOR = " AND "
           )

   Example outputs:
   - "the total weighted population of the survey"
   - "the weighted population that is Age group in [0 to 14, 15 to 24]
      AND Education level in [Primary]"

2. ── Layer covariate group membership ─────────────────────────────

   IF by IS NULL OR length(by) == 0:
       group_qualifier = ""  # no further restriction
   ELSE:
       group_qualifier = " within each {covariate_description} group"

       WHERE covariate_description =
           IF length(by) == 1:
               "{ui_label}"
           ELSE:
               "{ui_label_1} × {ui_label_2} [× ...]"

   combined_pop = base_pop + group_qualifier

   Example outputs:
   - "the total weighted population of the survey within each Gender group"
   - "the weighted population that is Age group in [0 to 14] within each
      Gender × Area group"

3. ── Apply measure-specific semantics ─────────────────────────────

   FOR EACH measure IN measures:

       measure_family = .MEASURE_REGISTRY[[measure]]

       SWITCH measure_family:

       CASE "summary_stats":
           cell_def = "The {measure_label} of {analysis_var_label} for {combined_pop}."
           Example: "The mean of Welfare for the total weighted population of the survey within each Gender group."

       CASE "inequality":
           cell_def = "The {measure_label} of {analysis_var_label} among {combined_pop}."
           Example: "The Gini index of Welfare among the total weighted population of the survey."

       CASE "poverty":
           cell_def = "The {measure_label} at ${poverty_line}/day (PPP {ppp_year}) for {combined_pop}."
           Example: "The Poverty headcount at $2.15/day (PPP 2021) for the weighted population that is Age group in [0 to 14] within each Area group."

       CASE "shares":

           SUBSWITCH measure:

           CASE "pop_share":
               cell_def = "The share of the total weighted survey population represented by {combined_pop}."
               Example: "The share of the total weighted survey population represented by the weighted population that is Education level in [Primary] within each Gender group."

           CASE "target_within_group_share":
               # Analysis_var MUST be binary for this measure
               # Target population = analysis_var == 1

               target_label = ui_label of analysis_var
               numerator_pop = base_pop + " for whom {target_label} is true" + group_qualifier
               denominator_pop = combined_pop

               cell_def = "The share of {denominator_pop} for whom {target_label} is true."

               Example:
               IF analysis_var = "imp_wat_rec" (Improved water source)
               AND filter_base = list(age_group = 1L)  # "0 to 14"
               AND by = c("gender")

               THEN:
               cell_def = "The share of the weighted population that is Age group in [0 to 14] within each Gender group for whom Improved water source is true."

               Interpretation: Among the weighted 0-14 year-old population of a given gender,
                              what fraction has access to improved water?

           CASE "target_survey_share":
               # Denominator is ALWAYS total weighted survey population (ignores by)

               target_label = ui_label of analysis_var
               numerator_pop = base_pop + " for whom {target_label} is true" + group_qualifier

               cell_def = "The share of the total weighted survey population represented by {numerator_pop}."

               Example:
               Same inputs as above:
               cell_def = "The share of the total weighted survey population represented by the weighted population that is Age group in [0 to 14] within each Gender group for whom Improved water source is true."

               Interpretation: What fraction of the ENTIRE weighted survey population consists of
                              0-14 year-old males with improved water access? (When by = gender,
                              this is computed separately for each gender group.)

4. ── Combine all measure definitions ──────────────────────────────

   cell_definition_content = list(
       population_scope = combined_pop,
       measure_interpretation = c(cell_def_measure1, cell_def_measure2, ...)
   )

END
```

### 4.3 Worked Examples

#### Example 1: Simple Aggregate Mean

**Inputs:**
- `analysis_var = "welfare"`
- `measures = c("mean")`
- `filter_base = NULL`
- `by = NULL`

**Output:**
```
Cell Definition:
The mean of Welfare for the total weighted population of the survey.
```

---

#### Example 2: Filtered Poverty Analysis with Covariates

**Inputs:**
- `analysis_var = "welfare"`
- `measures = c("headcount", "poverty_gap")`
- `poverty_line = 2.15`
- `ppp = 2021L`
- `filter_base = list(age_group = c(1L, 2L))`  # "0 to 14", "15 to 24"
- `by = c("gender", "area")`

**Filter Resolution:**
- `age_group` -> "Age group in [0 to 14, 15 to 24]"

**Output:**
```
Cell Definition:
Population scope: The weighted population that is Age group in [0 to 14, 15 to 24] within each Gender × Area group.

Each cell contains:
1. The Poverty headcount at $2.15/day (PPP 2021) for the weighted population that is Age group in [0 to 14, 15 to 24] within each Gender × Area group.
2. The Poverty gap index at $2.15/day (PPP 2021) for the weighted population that is Age group in [0 to 14, 15 to 24] within each Gender × Area group.
```

---

#### Example 3: Binary Analysis Variable with Target Shares

**Inputs:**
- `analysis_var = "imp_wat_rec"` (Improved water source — binary)
- `measures = c("target_within_group_share", "target_survey_share")`
- `filter_base = list(age_group = 1L)`  # "0 to 14"
- `by = c("area")`

**Filter Resolution:**
- `age_group` -> "Age group in [0 to 14]"

**Analysis Variable Resolution:**
- `imp_wat_rec` -> "Improved water source"

**Output:**
```
Cell Definition:
Population scope: The weighted population that is Age group in [0 to 14] within each Area group.

Each cell contains:
1. Target share within cell: The share of the weighted population that is Age group in [0 to 14] within each Area group for whom Improved water source is true.

   (Interpretation: Among the weighted 0-14 year-old population, what fraction has improved water? Computed separately for urban and rural.)

2. Target share in total survey: The share of the total weighted survey population represented by the weighted population that is Age group in [0 to 14] within each Area group for whom Improved water source is true.

   (Interpretation: What fraction of the ENTIRE weighted survey population consists of 0-14 year-old urban dwellers with improved water access? Computed separately for urban and rural.)
```

---

#### Example 4: Poverty Status as Covariate

**Inputs:**
- `analysis_var = "welfare"`
- `measures = c("mean", "gini")`
- `poverty_line = 6.85`
- `by = c("pov_status")`  # Derived covariate

**NOTE:** When `pov_status` is in `by`, it is computed as `as.integer(welfare < poverty_line)` before grouping (line 454 in table_maker.R).

**Output:**
```
Cell Definition:
Population scope: The total weighted population of the survey within each Poverty status group (below/above $6.85/day PPP 2021).

Each cell contains:
1. The mean of Welfare for the total weighted population of the survey within each Poverty status group.
2. The Gini index of Welfare among the total weighted population of the survey within each Poverty status group.

Note: Poverty status groups are defined using a threshold of $6.85/day (PPP 2021).
```

---

## V. REGISTRY & DEPENDENCY INVENTORY

### 5.1 Parameter-to-Label Mapping Requirements

| Parameter | Registry Function | Returns |
|-----------|------------------|---------|
| `analysis_var` | `piptm_variable_registry(release)[[analysis_var]]` | `list(varname, ui_label, tm_type, roles, stat_groups, categories)` |
| `measures` | `piptm_stat_groups(release)` | Nested list: `stat_groups[[group]]$measures[[measure]]$label` |
| `filter_base` keys | `piptm_filter_categories(release)` | `list(list(varname, label, subcategories))` |
| `filter_base` values (codes) | `piptm_filter_categories(release)[[var]]$subcategories` | `list(list(code, label))` |
| `by` | `piptm_layout_covariates(release)` | `list(list(varname, label, n_categories))` |
| `pip_id` | `piptm_manifest(release)` | `data.table(pip_id, country_code, country_name, year, welfare_type, ...)` |

### 5.2 Missing Registry Data: Action Plan

**Current gaps identified:**

1. **Measure definitions** (`tm_measure_spec.yaml`):
   - **Exists**: Contains `label` for each measure
   - **Missing**: `definition` field (brief definitional sentence)
   - **Action**: Enhance `tm_measure_spec.yaml` schema (optional for v1)

2. **Welfare type labels**:
   - Currently stored as `"INC"` / `"CON"` codes
   - **Action**: Add hardcoded mapping in description builder:
     ```r
     welfare_type_labels <- c(INC = "Income", CON = "Consumption")
     ```

3. **Layout slot labels**:
   - Currently: `"columns"`, `"rows"`, `"super_columns"`, `"super_rows"`
   - **Action**: Add hardcoded mapping:
     ```r
     slot_labels <- c(
       columns       = "Columns",
       rows          = "Rows",
       super_columns = "Super Columns",
       super_rows    = "Super Rows"
     )
     ```

---

## VI. IMPLEMENTATION ROADMAP

### Phase 1: Backend Enhancements (3-5 days)

**File:** `R/table_maker.R`

**Tasks:**

1. **Add `include_metadata` parameter** (default `FALSE`)
   - Location: Function signature, line 187
   - Validation: Assert `is.logical(include_metadata) && length(include_metadata) == 1L`

2. **Instrument execution log capture**
   - **Surveys loaded**: Already available via `entries$pip_id` (line 276)
   - **Surveys excluded**: Wrap dimension pre-filter warning (lines 366-382) to also populate a tracking list:
     ```r
     excluded_surveys <- data.table(
       pip_id = dropped_entries$pip_id,
       reason = dropped_info
     )
     ```
   - **Filters applied**: Resolve `filter_base` keys/values via `piptm_filter_categories()`
   - **Suppression events**: Capture `suppressed` data.table from pop_share_threshold block (line 534)
   - **Warnings**: Implement custom warning handler:
     ```r
     warning_log <- character()
     withCallingHandlers(
       { ... table_maker logic ... },
       warning = function(w) {
         warning_log <<- c(warning_log, conditionMessage(w))
         invokeRestart("muffleWarning")
       }
     )
     ```

3. **Build `description_metadata` structure**
   - Create helper function `.build_description_metadata()`
   - Populate all fields per Section II schema

4. **Modify return logic**
   ```r
   if (include_metadata) {
     return(list(
       data = result,
       description_metadata = .build_description_metadata(...)
     ))
   } else {
     return(result)
   }
   ```

**Testing:**
- Unit test: Call `table_maker(..., include_metadata = TRUE)` and validate structure
- Regression test: Ensure `table_maker(..., include_metadata = FALSE)` returns identical `data.table` as before

---

### Phase 2: Description Data Model Builder (4-6 days)

**File:** `R/description_builder.R` (new)

**Functions:**

1. **`build_description_model(metadata, params)`**
   - Input: `description_metadata` list + original `table_maker()` params
   - Output: Structured model per Section III schema
   - Logic:
     - Resolve all labels via registry functions
     - Apply conditional visibility rules
     - Call `build_cell_definition()` for complex logic

2. **`build_cell_definition(analysis_var, measures, filter_base, by, poverty_line, ppp, release)`**
   - Implements algorithm from Section IV.2
   - Returns `list(population_scope, measure_interpretation)`

3. **Helper: `resolve_filter_labels(filter_base, release)`**
   - Input: Named list of filter codes
   - Output: `data.table(varname, ui_label, selected_labels)`

4. **Helper: `resolve_measure_labels(measures, release)`**
   - Input: Character vector of measure keys
   - Output: `data.table(measure, ui_label, stat_group)`

5. **Helper: `format_covariate_description(by, release)`**
   - Input: Character vector of covariate varnames
   - Output: Human-readable string (e.g., "Gender × Area")

**Testing:**
- Unit tests for each helper function with mock registry data
- Integration test: Full model build with all sections enabled
- Edge case tests:
  - No filters
  - No covariates
  - Binary analysis variable + shares measures
  - Poverty status as both analysis variable and covariate

---

### Phase 3: Markdown Renderer (2-3 days)

**File:** `R/description_renderer.R` (new)

**Functions:**

1. **`render_description_markdown(model)`**
   - Input: Description Data Model
   - Output: Character scalar (Markdown text)
   - Logic:
     ```r
     sections <- character()

     for (section in names(model)) {
       if (!model[[section]]$visible) next

       sections <- c(sections,
         paste0("## ", model[[section]]$title),
         "",
         .render_section_content(model[[section]]$content),
         ""
       )
     }

     paste(sections, collapse = "\n")
     ```

2. **`.render_section_content(content)`**
   - Dispatches based on section structure
   - For data.tables: Format as Markdown tables
   - For lists: Format as bullet lists or paragraphs

**Testing:**
- Golden file tests: Compare rendered output against expected Markdown for canonical examples

---

### Phase 4: API Endpoint (1-2 days)

**File:** `R/api.R`

**Tasks:**

1. **Add `/description` route**
   ```r
   #* Generate table description
   #* @param pip_id:character
   #* @param analysis_var:character
   #* @param measures:character
   #* @param poverty_line:numeric
   #* @param by:character
   #* @param filter_base:object
   #* @param ppp:int
   #* @param release:character
   #* @param pop_share_threshold:numeric
   #* @get /description
   #* @serializer text
   api_description <- function(pip_id, analysis_var, measures, ...) {

     # Parse parameters (same logic as /table_maker)
     params <- .parse_api_params(...)

     # Call table_maker with metadata
     result <- table_maker(
       pip_id = pip_id,
       analysis_var = analysis_var,
       measures = measures,
       ...,
       include_metadata = TRUE
     )

     # Build description model
     model <- build_description_model(
       result$description_metadata,
       params
     )

     # Render to Markdown
     render_description_markdown(model)
   }
   ```

2. **Parameter parsing** (reuse existing logic from `/table_maker` endpoint)

**Testing:**
- Integration test: Full API call via `plumber` test harness
- Compare output with UI Step 3 expectations

---

### Phase 5: Documentation & Examples (2 days)

**Files:**
- `man/build_description_model.Rd`
- `man/render_description_markdown.Rd`
- `vignettes/description-endpoint.Rmd`

**Content:**
- Architectural overview
- Cell definition algorithm walkthrough
- Worked examples (all 4 from Section IV.3)
- Extensibility guide (adding HTML renderer)

---

## VII. ACCEPTANCE CRITERIA

### Functional Requirements

1. **Completeness**: Description covers all 9 input parameters and 4 execution log categories
2. **Accuracy**: Cell definitions mathematically correct for all measure families
3. **Dynamism**: Content adapts to all valid parameter permutations
4. **Factuality**: No interpretive or policy-prescriptive language
5. **Readability**: Human-readable labels resolved from registries (no raw codes)

### Technical Requirements

6. **Backward compatibility**: Existing `table_maker()` calls unaffected
7. **Performance**: Metadata assembly adds <5% overhead
8. **Modularity**: Description builder and renderer decoupled
9. **Extensibility**: Adding HTML/JSON renderers requires <50 LOC
10. **Test coverage**: >90% coverage for all new functions

### Edge Cases Handled

11. No filters applied (`filter_base = NULL`)
12. No covariates assigned (`by = NULL`)
13. All surveys excluded (should return descriptive error, not crash)
14. Binary analysis variable with shares measures
15. Poverty status as both analysis variable and covariate
16. Suppression triggered (include suppression details in execution summary)
17. Multiple warnings accumulated (all displayed in warnings section)

---

## VIII. OPEN QUESTIONS FOR IMPLEMENTATION TEAM

1. **Measure definition enhancement**: Should we extend `tm_measure_spec.yaml` to include `definition` fields, or keep cell interpretation logic purely algorithmic?

2. **Internationalization**: Are non-English descriptions in scope for v1, or defer to v2?

3. **UI integration**: Does Step 3 UI consume raw Markdown, or should we expose both Markdown and JSON renderers from day 1?

4. **Suppression detail level**: Should suppressed cell details include full dimension values, or just counts?

5. **Performance monitoring**: Should we instrument `description_metadata` assembly timing separately from `table_maker()` computation?

---

## IX. RISK MITIGATION

| Risk | Impact | Mitigation |
|------|--------|------------|
| Cell definition logic errors | High | Extensive unit tests with binary matrix of (filter x measure x covariate) combinations |
| Registry data inconsistencies | Medium | Schema validation tests for all registry JSONs |
| Backward compatibility break | High | Regression test suite comparing v1 and v2 `table_maker()` outputs |
| Performance degradation | Low | Benchmark suite; metadata assembly is opt-in |
| UI rendering issues | Medium | Golden file tests for Markdown output; coordinate with front-end team |

---

## X. SUCCESS METRICS

- **Developer adoption**: `include_metadata = TRUE` used in 100% of internal testing workflows within 2 sprints
- **User satisfaction**: Step 3 UI descriptions reduce support tickets related to table interpretation by >=30%
- **Code quality**: No P0/P1 bugs in description logic within first 3 months post-launch
- **Performance**: `/description` endpoint latency <1.5x `/table_maker` latency for equivalent queries
