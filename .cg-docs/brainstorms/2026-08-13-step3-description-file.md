---
date: 2026-08-13
title: "Step 3 Description File for Table Results"
status: decided
scope: "Standard"
chosen-approach: "Structured Markdown with R-side Generation (Approach 1)"
tags: [ui, api, documentation, step-3, metadata]
---

# Step 3 Description File for Table Results

## Context

Users reach Step 3 of the Table Maker wizard after making multiple decisions across Step 1 (survey selection) and Step 2 (sample filters, analysis variable, statistics, layout slots). The resulting table may be complex and difficult to interpret without context — especially when users select share measures, apply sample-base filters, or use multi-slot disaggregation.

The table displays numbers, but does not explain:
- What each number represents (which statistic, which population subset, which filters)
- How the table is structured (which dimensions are in rows vs. columns)
- What methodological choices were made (PPP year, suppression threshold, poverty line)
- What warnings or exclusions occurred during computation

**Problem:** Users need a structured, human-readable document that unambiguously defines what the table shows and how it was generated.

**Audience:** Policymakers, researchers, journalists, economists — users who need to cite results, share findings, or understand complex configurations months after creating the table.

**Primary use cases:**
- Sharing results with colleagues who didn't run the analysis
- Including tables in reports or presentations with proper documentation
- Long-term record-keeping ("what exactly was this table?")
- Understanding complex share measures and their interpretation

## Requirements

### Confirmed Content (Mandatory)

1. **Survey metadata** — country, year, welfare type, survey name (pip_id)
2. **Sample base definition** — filters applied in Decision 1 (or "full survey sample")
3. **Statistics computed** — analysis variable + selected measures from Decision 2
4. **Disaggregation structure** — layout slots from Decision 3 (rows, columns, super rows, super columns)
5. **Poverty line** — if applicable (when `analysis_var = "pov_status"` or poverty measures selected)
6. **Cell definition** — complete narrative sentence explaining what each cell represents

### Always Included

7. **PPP year** — which PPP conversion was used
8. **Suppression threshold** — if active, report threshold and whether cells were suppressed
9. **Caveats/warnings** — surveys excluded due to missing dimensions, filters, etc.

### Additional Elements (Selective)

10. **Interpretation notes** — factual only (e.g., "Higher Gini = more inequality", "Poverty rate = % below poverty line")
11. **Data provenance** — release ID and brief statement about data source

### Out of Scope (This Iteration)

- Statistical methodology documentation (FGT formulas, Gini calculation details)
- Survey documentation links or microdata sources
- Data quality notes beyond warnings (response rates, sampling design)
- Interactive elements (static text/PDF only)
- Multiple languages (English only for now)
- Version history or regeneration tracking
- Reproducibility code snippets (API calls, R code) — deferred to future iteration

## Approaches Considered

### Approach 1: Structured Markdown with R-side Generation ✅ CHOSEN

**How it works:**
- New R function `build_table_description()` in `piptm` package
- Takes same parameters as `table_maker()` (or accepts its output + original params)
- Returns a character string containing markdown with clearly defined sections
- New API endpoint `GET /description` calls this function and returns markdown in envelope
- UI receives markdown and either:
  - Renders it inline using a markdown viewer component
  - Converts to PDF via browser print-to-PDF or client-side tool

**Content structure (Option D: Contextual Inline Help):**
- Narrative flow with facts + just enough context to be clear
- Factual interpretation notes inline (e.g., "Higher Gini values indicate greater inequality")
- No separate "summary" vs. "details" sections — integrated narrative
- Target: 1 page max

**Example section:**
```markdown
## Table Structure

**Disaggregation:**
- **Rows:** Education level (4 categories: None, Primary, Secondary, Tertiary)
- **Columns:** Gender (2 categories: Female, Male)

Each cell in the table represents one unique combination of education level and gender. For example, the cell in row "Primary" and column "Female" contains statistics for women with primary education only.

**Cell Definition:** Each cell shows mean, Gini index, and median of welfare for the specified education level and gender group, weighted by survey sampling weights.
```

**Pros:**
- Simple to implement — string concatenation/templating in R
- Markdown is human-readable in raw form
- Easy to version-control examples and test fixtures
- No new heavy dependencies (rendering happens client-side)
- Consistent with existing PIP documentation style
- **Easy migration path to Approach 2** (1 day to upgrade to HTML+PDF if needed)

**Cons:**
- PDF conversion requires external step (not handled by R package)
- Markdown formatting may vary across renderers
- Limited control over PDF styling (fonts, page breaks, headers/footers)

**Effort:** Small (2-3 days)

**Migration path:** Approach 1 → Approach 2 is straightforward (1 day) if server-side PDF generation becomes required. Just wrap the same content in HTML tags and add `pagedown::chrome_print()`.

---

### Approach 2: HTML Template with Server-side PDF Generation

**How it works:**
- HTML template file (`inst/templates/description.html`) with placeholders
- `build_table_description()` fills template with actual values
- API endpoint accepts `format` parameter: `html` or `pdf`
- For PDF: R calls `pagedown::chrome_print()` or similar, returns binary
- For HTML: returns rendered HTML string

**Pros:**
- Full control over styling (CSS for fonts, colors, spacing, page breaks)
- PDF generation is deterministic and reproducible
- Can include World Bank branding/logos easily
- Single source of truth (one template, multiple formats)

**Cons:**
- Requires `chromote` or headless Chrome on server — complex deployment dependency
- Slower response time (Chrome rendering adds ~1-2 seconds)
- Heavier R package footprint
- PDF generation may fail in restricted server environments

**Effort:** Medium (4-5 days — includes template design, Chrome setup, testing)

**Recommended?** No — too heavy for MVP. Consider later if PDF quality becomes hard requirement.

---

### Approach 3: JSON Metadata + UI-side Rendering

**How it works:**
- R function `build_table_metadata()` returns structured list (auto-converts to JSON)
- API endpoint `GET /description` returns JSON
- UI handles all rendering (markdown, HTML, or PDF)

**Pros:**
- Clean separation of concerns (R does data, UI does presentation)
- Maximum UI flexibility — can change formatting without touching R
- No PDF generation complexity on server
- Structured data easier to test/validate

**Cons:**
- Requires UI developer to implement rendering logic
- More coordination needed between backend and frontend
- Harder to preview final output during R development
- JSON schema versioning required (breaking changes affect UI)

**Effort:** Medium (3-4 days — includes schema design, API contract negotiation)

**Recommended?** Maybe — architecturally cleaner, but adds coordination overhead. Only choose if UI team explicitly prefers full control.

---

## Decision

**Chosen Approach:** Approach 1 (Structured Markdown with R-side Generation)

**Rationale:**
1. **Fastest to implement** — can write and test `build_table_description()` in R immediately
2. **Easiest to validate** — can see exact text users will read without opening browser
3. **No deployment complexity** — markdown is just a string, no Chrome/Pandoc needed
4. **Progressive enhancement** — can upgrade to Approach 2 later without breaking API contract
5. **Addresses core problem** — especially critical for complex configurations (shares, multiple filters, 4-slot layouts) where table alone is cryptic

**Content style:** Option D (Contextual Inline Help) — narrative flow with factual interpretation notes, integrated sections, target 1 page.

**Delivery mechanism:** On-demand generation via new `/description` API endpoint. No session storage — description is generated fresh from table parameters on each request.

**Format:** Markdown initially. UI handles PDF conversion via browser print-to-PDF. Server-side PDF generation (Approach 2) can be added later if needed.

## Next Steps

### R Package (`piptm`)

1. **Create `build_table_description()` function** (`R/description.R`)
   - Accept same parameters as `table_maker()`: `pip_id`, `analysis_var`, `measures`, `poverty_line`, `by`, `filter_base`, `ppp`, `release`, `pop_share_threshold`
   - Alternatively: accept `table_maker()` output + original parameters (requires storing params with results)
   - Return character string (markdown)
   - Handle all conditional cases:
     - Poverty line present/absent
     - Multiple surveys vs. single survey
     - Disaggregation (0-4 slots)
     - Sample base filters applied vs. none
     - Suppression threshold active vs. not (mention threshold even if no cells suppressed)

2. **Implement content sections:**
   - Title + metadata (generated timestamp, release, PPP year)
   - Surveys analyzed (list or single sentence)
   - Sample base (filters or "full survey sample")
   - Statistics computed (analysis variable + measures, with brief factual notes)
   - Table structure (disaggregation slots + cell definition)
   - Data processing (poverty line if applicable, suppression threshold)
   - Warnings (surveys excluded, dimensions missing, etc.)

3. **Leverage existing registry functions:**
   - `piptm_manifest()` — get survey metadata (country, year, welfare type)
   - `piptm_variable_registry()` / `piptm_analysis_variables()` — get human-readable labels for variables
   - `piptm_stat_groups()` — get measure labels
   - `piptm_layout_covariates()` — get covariate labels and category counts
   - `piptm_filter_categories()` — get filter variable labels and subcategory labels

4. **Write unit tests:**
   - All conditional branches (poverty line, filters, disaggregation, suppression)
   - Edge cases (no surveys, all surveys excluded, by = NULL, etc.)
   - Output format validation (valid markdown, 1 page length check)

5. **Documentation:**
   - Roxygen for `build_table_description()`
   - Example usage in function docs
   - Add to package README

### API Layer (`inst/plumber/plumber.R`)

6. **New endpoint: `GET /description`**
   - Query parameters: same as `/table` endpoint (`pip_id`, `analysis_var`, `measures`, `poverty_line`, `by`, `filter_base`, `ppp`, `release`, `pop_share_threshold`)
   - Calls `build_table_description()` with validated parameters
   - Returns markdown string in standard envelope: `{ status, data: { description: "..." }, warnings, errors, meta }`
   - Reuse existing parameter validation logic from `/table` endpoint

7. **Update API documentation:**
   - Add `/description` to README endpoint table
   - Add to Insomnia collection (`insomnia-collection.json`)

### UI Integration (Deferred — requires coordination with UI team)

8. **UI changes needed:**
   - "View metadata" button in Step 3 results view
   - Modal or side panel to display rendered markdown
   - "Download PDF" button — uses browser print-to-PDF
   - Pass table configuration parameters to `/description` endpoint

### Testing & Validation

9. **Integration testing:**
   - Test `/description` endpoint with real table configurations from existing tests
   - Verify markdown renders correctly in browser
   - Test PDF conversion via browser print (manual QA)

10. **User acceptance:**
    - Generate description files for 3-5 realistic scenarios (simple, complex, shares, poverty)
    - Review with stakeholders to confirm content clarity and completeness
    - Adjust wording/structure based on feedback

### Future Enhancements (Out of Scope for This Iteration)

- Server-side PDF generation (Approach 2 migration)
- Reproducibility code snippets (API curl command, R code)
- Multiple languages (internationalization)
- Survey documentation links
- Detailed methodological notes
- Custom branding/styling for PDF output

## Validation Notes (Devil's Advocate)

**Effort-value confirmed:** Feature is especially valuable for complex configurations (shares, multi-slot layouts, filters) where the table alone is cryptic. Addresses a real user need for sharing, citing, and long-term record-keeping.

**Charter alignment:** Supports reproducibility by documenting all inputs. Executable code for reproduction can be added in future iteration.

**Simplicity check:** Simpler alternatives (CSV headers, URL encoding) don't provide the polished narrative document needed for reports/presentations. Markdown generation is lightweight and straightforward.
