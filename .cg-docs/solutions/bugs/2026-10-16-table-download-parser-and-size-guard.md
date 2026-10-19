---
date: 2026-10-16
title: "POST /table/download malformed JSON 500s and size-guard bypass"
category: "bugs"
language: "R"
tags: [plumber, api, json, parser, content-length, download, regression-tests]
root-cause: "Endpoint relied on plumber auto-parsing and Content-Length alone, so malformed JSON could surface as 500 and oversized payloads could bypass limits when headers were missing or wrong"
severity: "P1"
---

# POST /table/download malformed JSON 500s and size-guard bypass

## Problem
A new binary download endpoint (`POST /table/download`) was added to convert already-computed `/table` rows into CSV or RDS.

Two reliability issues appeared during hardening:

1. Some malformed JSON request bodies could reach plumber/parser failure paths and return HTTP 500 instead of the endpoint's expected JSON error envelope with HTTP 400.
2. The size cap (`25 MB`) was enforced only via `Content-Length`, so requests with missing/invalid headers could still parse oversized bodies in memory.

Symptoms:
- Inconsistent error behavior for invalid payloads.
- Risk of memory pressure if a large body is accepted without a trustworthy header.

## Root Cause
1. **Parser contract mismatch**: relying on automatic JSON parsing in a binary endpoint made malformed-body behavior depend on parser/runtime edge cases, not only handler logic.
2. **Single-source size check**: trusting `Content-Length` alone is insufficient because clients/proxies can omit or misstate it.
3. **Coverage gap**: tests originally focused on happy paths and status-only checks, missing strict header/envelope and malformed-input branches.

## Solution
Implemented a deterministic raw-body parsing path and strengthened tests.

### 1) Manual body parse for deterministic 400 envelope behavior
`inst/plumber/plumber.R` (`/table/download`):

```r
#* @param body:object Request body (JSON object)
#* @parser text
#* @serializer null
function(req, body = NULL, res) {
  raw_body <- if (!is.null(body) && nzchar(body)) body else req$postBody
  body <- tryCatch(jsonlite::fromJSON(raw_body), error = function(e) NULL)
  if (is.null(body) || !is.list(body) || length(body) == 0L) {
    return(err("Request body must be a non-empty JSON object.", 400L))
  }
}
```

Why: `@parser text` keeps request capture stable while endpoint code owns JSON parsing and error handling. This avoids parser-level 500s for malformed JSON. `@serializer null` and `@param body:object` were added in a follow-up fix — see `.cg-docs/solutions/bugs/2026-10-16-table-download-serializer-and-swagger-body.md` for why.

### 2) Dual size enforcement
Before JSON parse, enforce both checks:
- Header-based check: `as.numeric(req$CONTENT_LENGTH) > max_bytes`
- Actual byte-length check: `nchar(raw_body, type = "bytes") > max_bytes`

Why: header check is cheap early rejection; byte-length check catches missing/incorrect headers.

### 3) Expanded regression tests
`tests/testthat/test-api-table-download.R` now verifies:
- Success headers for CSV/RDS (`Content-Type`, `Content-Disposition`)
- JSON error envelope shape for bad requests
- Malformed/non-object/missing-data body variants
- Default `format` fallback to CSV
- Oversized request rejection with HTTP 413

## Prevention
- For plumber endpoints that need strict custom error envelopes, parse raw request bodies in-handler and do not rely on framework defaults for malformed JSON behavior.
- Enforce payload limits with both header checks and measured byte checks.
- For new endpoints, include test coverage for:
  - success path headers
  - malformed JSON
  - non-object payloads
  - explicit size-limit behavior
- Keep endpoint inventory docs synchronized with router changes to reduce drift.

## Related
- `.cg-docs/solutions/bugs/2026-08-31-swagger-req-parameter-exposed.md`
- `.cg-docs/solutions/testing-patterns/2026-05-13-plumber-programmatic-testing-with-fixtures.md`
- `.cg-docs/reviews/2026-09-14-shares-cell-definition-structured-table-review.md`
- `.cg-docs/solutions/bugs/2026-10-16-table-download-serializer-and-swagger-body.md` — follow-up fix for a serializer-induced duplicate `Content-Type` header and a Swagger body-as-query-parameter bug found when manually verifying this endpoint against a live Swagger UI/OpenAPI spec.
