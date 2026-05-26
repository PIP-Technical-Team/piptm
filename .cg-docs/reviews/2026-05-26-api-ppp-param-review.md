---
date: 2026-05-26
plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md
depth: thorough
findings:
  P0.1: fixed
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: open
  P1.5: fixed
  P1.6: fixed
  P1.7: fixed
  P1.8: fixed
  P2.1: open
  P2.2: fixed
  P2.3: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
  P3.5: open
---

## Review Report

**Review depth**: thorough
**Files reviewed**: 4 (`inst/plumber/helpers.R`, `inst/plumber/plumber.R`,
`tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`)
**Findings**: 16 (P0: 1, P1: 8, P2: 3, P3: 5)

**Commit reviewed**: `feat(api): expose ppp param on /table endpoint; update validate_table_input()`
**Branch**: `api-endpoints`

---

### P0 — BLOCKING (immediate remediation required)

- **[P0.1]** [cg-adversarial] `inst/plumber/helpers.R:~255` — **`ppp = TRUE` coerces silently to `1L`, bypassing validation as a "valid" PPP year**
  **Why**: `as.integer(TRUE)` → `1L`. Length is 1, no NA, and `1L > 0L`, so the validator returns `coerced_ppp = 1L` as valid. The request then reaches `load_surveys()` with `ppp = 1L` — a year that won't match any manifest entry, generating a 422 domain error instead of the correct 400 validation error. Same applies to `FALSE` → `0L` (which the `<= 0` guard catches) and to other non-numeric/non-character scalar types passed from direct R calls.
  **Fix**: Add a type guard at the top of the `if (!is.null(ppp))` block before the length check:
  ```r
  if (!is.character(ppp) && !is.numeric(ppp)) {
    errors <- c(
      errors,
      paste0(
        "`ppp` must be a numeric or character value; got type: ",
        class(ppp)[[1L]], "."
      )
    )
  } else if (length(ppp) != 1L) {
    ...
  ```

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-adversarial] `inst/plumber/helpers.R:~258` — **`ppp = "1e5"` (scientific notation) coerces silently to `100000L`**
  **Why**: `as.integer("1e5")` → `100000L` in R (the string is first parsed as double `1e5 = 100000.0`, then truncated to integer). This passes all checks and reaches `load_surveys()` as PPP year `100000` — a nonsensical but structurally valid value. Similarly `"1E3"`, `"2.017e3"`, etc.
  **Fix**: Replace the bare `as.integer(ppp)` coercion path (for character inputs) with a digit-only guard:
  ```r
  ppp_str <- trimws(as.character(ppp))
  if (!grepl("^[0-9]+$", ppp_str)) {
    errors <- c(errors, paste0(
      "`ppp` must be a plain integer year (digits only, e.g. 2017); got: ",
      substr(ppp_str, 1L, 40L), "."
    ))
    coerced_ppp <- NULL
  } else {
    coerced_ppp <- as.integer(ppp_str)
    if (coerced_ppp <= 0L) { ... }
  }
  ```
  This also subsumes P1.2 (whitespace), P1.3 (newlines), and the `"0x7FF"` hex-string case (all fail the digit-only check).

- **[P1.2]** [cg-adversarial] `inst/plumber/helpers.R:~258` — **`ppp = " 2017 "` (leading/trailing whitespace) coerces silently to `2017L`**
  **Why**: `as.integer(" 2017 ")` → `2017L`. Whitespace in query strings is URL-decoded and may reach the handler. This is benign for valid PPP years but inconsistent with the documented type.
  **Fix**: Subsumed by P1.1 fix (`trimws()` + digit-only guard).

- **[P1.3]** [cg-adversarial] `inst/plumber/helpers.R:~258` — **`ppp = "2017\n"` (embedded newline) silently coerces; newline echoed in error messages if other paths fail**
  **Why**: `as.integer("2017\n")` → `2017L` (R trims `\n`). If coercion fails for other reasons the raw `ppp` value is echoed in `paste0("got: ", ppp, ".")` — a newline in the string breaks log lines.
  **Fix**: Subsumed by P1.1 fix (digit-only guard rejects `"2017\n"` because `\n` is not in `[0-9]`).

- **[P1.4]** [cg-testing] `tests/testthat/test-api-endpoints.R:~589` — **The `ppp='2017'` endpoint test accepts both 200 and 422, masking regression risk**
  **Why**: The test comment explains the ambiguity comes from the fixture parquets not having a `welfare_ppp_2017` column. As written, the test would still pass if a bug caused the request to return 422 for an unrelated reason. The critical assertion ("validation did not reject ppp=2017 with a 400") is correct, but the test should be split to make each assertion clear.
  **Fix**: Rename the test to document its actual scope ("ppp='2017' passes validator — returns 200 or 422, never 400"), and add a second, sharper assertion: `expect_false(res$status == 400L)` as the primary check, with the `%in% c(200L, 422L)` as secondary documentation.

- **[P1.5]** [cg-testing] `tests/testthat/test-api-helpers.R` — **No test for `NA` ppp input**
  **Why**: `ppp = NA` → length 1, `as.integer(NA)` → `NA_integer_` → caught by `is.na()` check, error fires. Correct behavior but untested. The error message says `"got: NA"`, which is also somewhat opaque (no indication it's a missing value vs. the string "NA").

- **[P1.6]** [cg-testing] `tests/testthat/test-api-helpers.R` — **No test for `character(0)` ppp input**
  **Why**: `character(0)` → `length == 0`, `length != 1L` guard fires → error says "0 value(s) were supplied." Correct but untested.

- **[P1.7]** [cg-testing] `tests/testthat/test-api-helpers.R` — **No test for multi-param error accumulation (invalid `ppp` + other invalid param)**
  **Why**: All 7 new ppp tests use valid `pip_id` and `measures`. No test verifies that invalid `ppp` alongside an invalid `pip_id` accumulates both errors and returns `result$ppp = NULL`.

- **[P1.8]** [cg-testing] `tests/testthat/test-api-endpoints.R` — **No integration test for repeated `ppp` query param (`ppp=2017&ppp=2011`)**
  **Why**: Plumber delivers repeated query params as a vector. The unit test covers `c(2011L, 2017L)`, but the HTTP layer is untested. A regression in how plumber parses repeated `ppp` params would be invisible.

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-adversarial] `inst/plumber/helpers.R:~262` — **User-supplied `ppp` value echoed raw in error messages without length-bounding**
  **Why**: `paste0("... got: ", ppp, ".")` echoes the raw user string. A very long `ppp` (e.g. `strrep("x", 10000)`) creates an oversized error string in the JSON response; an embedded control character (`\n`, `\t`) can break log parsers.
  **Fix**: Truncate before echo: `substr(as.character(ppp), 1L, 40L)`. This is partially subsumed by the digit-only guard in P1.1 (which rejects before the echo), but the negative-value echo path (`coerced_ppp <= 0L`) also echoes `coerced_ppp` and should be bounded.

- **[P2.2]** [cg-testing] `tests/testthat/test-api-helpers.R` — **No test for decimal ppp string `"2017.5"` (currently rejected, but behaviour differs from decimal numeric)**
  **Why**: `as.integer("2017.5")` → `NA` in R (string with decimal point is not coerced). `as.integer(2017.5)` → `2017L` (numeric truncates). The two behaviours diverge. After P1.1 fix both should be rejected, but without a test the divergence is invisible and a future code change could accidentally re-introduce silent truncation.

- **[P2.3]** [cg-architecture] `inst/plumber/helpers.R` — **`validate_table_input()` return list has no pinned shape contract**
  **Why**: The return list started as 2 fields and is now 4. There is no test asserting `names(result) == c("valid", "errors", "poverty_lines", "ppp")`. A future contributor adding a 5th field (e.g. `measures_coerced`) would not know the shape is a contract until something silently breaks.
  **Fix**: Add one test:
  ```r
  test_that("validate_table_input() return list has the expected shape", {
    result <- validate_table_input(pip_id = "ARM_2012_ILCS_CON_ALL", measures = "mean")
    expect_named(result, c("valid", "errors", "poverty_lines", "ppp"), ignore.order = FALSE)
  })
  ```

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `inst/plumber/helpers.R:~243` — **`coerced_ppp` not explicitly reset to `NULL` in the `length != 1` branch for defensive clarity**
  **Why**: `coerced_ppp` is initialised to `NULL` and the `length != 1` branch doesn't assign to it — which is correct. But the other two error branches (`is.na` and `<= 0`) do explicitly set `coerced_ppp <- NULL`. The asymmetry could mislead a future editor into thinking the length branch is missing a reset.
  **Fix**: Add `coerced_ppp <- NULL` in the `length != 1` branch body for symmetry.

- **[P3.2]** [cg-documentation] `inst/plumber/plumber.R:~121` — **`@param ppp:integer` annotation lacks a description of rejection behavior**
  **Why**: The current description `"PPP reference year — e.g. 2017 (optional; defaults to manifest ppp_sort)"` is correct but doesn't tell API consumers what happens when a non-integer value is passed.
  **Fix**: Expand to:
  ```r
  #* @param ppp:integer PPP reference year (e.g. 2017; optional). Must be a
  #*   positive whole number. When omitted, the manifest ppp_sort default is
  #*   used. Non-integer or non-positive values return HTTP 400.
  ```

- **[P3.3]** [cg-adversarial] `inst/plumber/helpers.R` — **`ppp = "0x7FF"` (hex string) coerces to `2047L` — passes silently (confirmed bypass, subsumed by P1.1)**
  **Why**: Contrary to initial expectation, `as.integer("0x7FF")` = `2047L` in R (hex strings ARE parsed). This passes the validator and reaches `table_maker()` with `ppp = 2047L`. Tests added document current behavior. P1.1 digit-only guard will reject this.
  **Fix**: Subsumed by P1.1 fix. Tests already updated to document current behavior.
  **Status**: `fixed` (behavior documented; guard pending P1.1)

- **[P3.4]** [cg-adversarial] `inst/plumber/helpers.R` — **Integer overflow (`ppp = 2147483648`) — correctly rejected via NA path**
  **Why**: `suppressWarnings(as.integer(2147483648))` → `NA_integer_`. The `is.na()` guard catches it. The `suppressWarnings` is load-bearing here (the overflow warning is intentionally swallowed). A comment should document this.

- **[P3.5]** [cg-architecture] `inst/plumber/plumber.R` — **Double-validation of `ppp` is correct by design — advisory note**
  **Why**: The API layer checks structural validity (scalar, positive integer). `load_surveys()` checks domain validity (does a `welfare_ppp_<year>` column exist in the manifest). These are different concerns; the two-layer design is correct. No change needed but worth a one-line comment above `ppp <- check$ppp`.

---

### ✅ Passed

- **cg-code-quality**: No magic numbers; consistent naming (`coerced_ppp`, `coerced_pl`); NULL initialisation pattern correct; no hardcoded paths; no debug output.
- **cg-version-control**: No secrets, credentials, or tokens introduced. Commit message is well-formed conventional commit format.
- **cg-reproducibility**: No seeds, hardcoded paths, or non-deterministic operations introduced.
- **cg-performance**: `ppp` validation block is O(1), no allocation beyond one scalar. Zero performance concern on the per-request hot path.

---

*Parsed 16 finding IDs.*
