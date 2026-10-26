---
date: 2026-10-26
title: "POST /table/download Approach C flattening and boundary validation"
category: "bugs"
language: "R"
tags: [plumber, api, approach-c, shape-a, json, schema-validation, data-table, error-envelope, performance]
root-cause: "The download endpoint consumed a nested /table response through a flat-row path and lacked boundary validation for malformed JSON-derived fields, while the flattener mixed registry-backed serialization with stateless export reshaping."
severity: "P1"
---

# POST /table/download Approach C flattening and boundary validation

## Problem

The `/table` endpoint changed from flat rows to Approach C payloads: each
survey contains header metadata, a four-slot `layout`, and dense `cells[]`
objects whose measure values are already formatted display strings. The
`POST /table/download` endpoint still expected flat rows and used
`rbindlist()` directly on `body$data`, causing nested payloads to fail. The
boundary also had several malformed-input hazards:

- `null` or empty scalar header fields could produce raw R indexing errors;
- malformed `surveyid_year` or `poverty_line` values could silently become
  `NA` through coercion;
- duplicate or colliding dimension/measure names could silently overwrite
  output columns;
- malformed active cell slots could be converted to missing labels;
- multi-line request bodies could trigger length errors in scalar `&&` checks;
- the validation and extraction implementation lived in the large,
  registry-backed `R/table_response.R` module.

## Root Cause

The endpoint boundary was treated as a trusted internal handoff even though
the client sends JSON back to a separate endpoint. JSON parsed with
`jsonlite::fromJSON(..., simplifyVector = FALSE)` preserves `null` as
present-but-`NULL`, so checking only key presence is insufficient before
`[[1L]]` indexing or numeric coercion.

The original implementation also coupled two distinct responsibilities:
registry-dependent `/table` serialization and registry-free `/table/download`
flattening. That made the stateless export path harder to audit and encouraged
validation and extraction work to be repeated across cell columns.

## Solution

`POST /table/download` now parses Approach C JSON and calls the exported
`piptm::flatten_table_response()` function. The flattener emits Shape A rows
with deterministic column order:

1. six survey headers;
2. active dimension labels in `superRows`, `rows`, `superColumns`, `columns`
   order;
3. formatted measure strings verbatim, including `"NR"` and JSON `null`.

The boundary validates scalar headers, layout/measure uniqueness, reserved
column collisions, active cell slot types, and cross-survey schema equality
before constructing the output. The request handler collapses multi-element
raw request bodies before scalar checks and maps malformed payloads to the
standard HTTP 400 JSON error envelope.

The stateless path was moved to `R/table_flatten.R`, leaving
`R/table_response.R` responsible for registry-backed response serialization.
Within `.flatten_single_survey()`, cell validation and extraction of all active
dimension labels and measure values occur in one pass:

```r
for (c_idx in seq_len(n_cells)) {
  cell <- cells[[c_idx]]
  validate_cell(cell)
  extract_dimension_labels(cell)
  extract_measure_values(cell)
}
```

This removes a redundant full validation traversal and repeated per-column
cell scans while preserving the same output schema.

Regression coverage includes collision rejection, malformed scalar headers,
formatted measure preservation, RDS/CSV round trips, malformed request
envelopes, legacy flat-row rejection, deterministic output, and multi-survey
stacking. The full gate passed with 1693 tests passing, 0 failures, and 7
known warnings.

## Prevention

- Treat every endpoint that consumes another endpoint's JSON as an external
  schema boundary; validate values, not just key presence.
- Never apply blanket numeric coercion to formatted API fields that may carry
  display scaling or disclosure sentinels such as `"NR"`.
- Reject duplicate and reserved-name collisions before assigning into named
  lists, because R list assignment silently overwrites existing entries.
- Keep registry-free request reshaping in a dedicated module so it cannot
  accidentally acquire data-loading or registry dependencies.
- For nested JSON arrays, combine structural validation with extraction where
  practical, but retain explicit regression tests for every malformed-input
  branch.
- Test both `pr$call()` behavior and the actual serializer/header contract for
  binary Plumber endpoints when parser or serializer directives change.

## Related

- `.cg-docs/solutions/bugs/2026-10-22-table-download-format-contract-leakage.md`
  — prior silent corruption from coercing formatted `/table` values.
- `.cg-docs/solutions/bugs/2026-10-16-table-download-parser-and-size-guard.md`
  — raw-body parsing and dual request-size enforcement.
- `.cg-docs/solutions/bugs/2026-10-16-table-download-serializer-and-swagger-body.md`
  — serializer/header and OpenAPI request-body hardening.
- `.cg-docs/solutions/data-quality/2026-08-27-json-roundtrip-defensive-coding.md`
  — broader JSON schema validation and error-path testing patterns.
- `docs/api-contract.md` — current `/table/download` Approach C request and
  Shape A export contract.
