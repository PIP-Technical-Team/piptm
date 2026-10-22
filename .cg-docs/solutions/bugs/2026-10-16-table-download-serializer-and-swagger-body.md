---
date: 2026-10-16
title: "POST /table/download: serializer-induced duplicate Content-Type and Swagger body-as-query-param"
category: "bugs"
language: "R"
tags: [plumber, api, openapi, swagger, serializer, content-type, download, regression]
root-cause: "Route-level @serializer annotation re-set Content-Type after the handler already set it, and a bare `body` formal with no @param annotation was inferred by plumber's OpenAPI generator as a string query parameter instead of a JSON request body"
severity: "P1"
---

# POST /table/download: serializer-induced duplicate Content-Type and Swagger body-as-query-param

## Problem
While manually verifying `POST /table/download` against a live, running API (not just `pr$call()` unit tests), two defects surfaced that unit tests had not caught:

1. **Duplicate `Content-Type` header** on every response (success and error). Raw HTTP output looked like:
   ```
   Content-Type: text/csv; charset=utf-8
   Content-Disposition: attachment; filename="table.csv"
   Content-Type: application/octet-stream
   ```
   Two `Content-Type` headers in one response is technically invalid HTTP and risky for client parsers that read only the last/first value inconsistently.

2. **Swagger/OpenAPI misrepresented the request body.** `pr$getApiSpec()$paths[["/table/download"]][["post"]]` showed:
   ```r
   requestBody present: FALSE
   parameters count: 1   # name="body", in="query", type="string"
   ```
   Swagger UI would have rendered `body` as a string query-parameter text box instead of a JSON request-body editor — the same failure mode as the earlier `/description` Swagger bug.

An earlier attempted fix (removing `@serializer contentType` entirely to "fix" the duplicate header) made things catastrophically worse: with no serializer annotation, plumber falls back to its default JSON serializer, which wrapped the handler's raw byte-vector return value in a base64-encoded JSON array (`["eyJzdGF0dXMiOi..."]`) for *every* response, including successful CSV/RDS downloads. This was only caught by re-running the live curl checks, not by the existing `pr$call()`-based test suite — `pr$call()` round-trips objects differently and did not expose the corruption.

## Root Cause

1. **Duplicate header**: plumber's `serializer_headers()` (used internally by `serializer_content_type()`) unconditionally calls `res$setHeader("Content-Type", <route's fixed type>)` *after* the handler function returns — see `plumber:::serializer_headers`:
   ```r
   Map(names(headers), headers, f = function(header, header_val) {
     res$setHeader(header, header_val)
   })
   ```
   This runs regardless of whether the handler already called `res$setHeader("Content-Type", ...)` itself, and `res$setHeader()` appends rather than replaces, producing two header lines whenever the route-level fixed type differs from (or even matches) the handler's dynamic type.

2. **Removing the serializer annotation entirely is not a fix**: with no `@serializer` tag, plumber uses its default **JSON** serializer for the route. JSON-serializing a raw byte vector base64-encodes it and wraps it in an array — this silently corrupts every response body, not just error paths. The failure was only visible via real HTTP calls; `pr$call()`-based tests did not catch it because the test harness's object handling differs from strict JSON-over-HTTP serialization.

3. **Swagger body-as-query-param**: when a function formal (`body`) has no `@param` roxygen annotation, plumber's OpenAPI spec generator infers it from the formal's default value and emits a `query` parameter with `type: string`. There was no `@param body:object` annotation on this endpoint, so the generator could not recognize it as the JSON request body — identical root cause to the earlier `.cg-docs/solutions/bugs/2026-08-31-swagger-req-parameter-exposed.md` finding for `/description`.

## Solution

### 1) Use `@serializer null` instead of `@serializer contentType`
```r
#* @param body:object Request body (JSON object)
#* @parser text
#* @serializer null
#* @post /table/download
function(req, body = NULL, res) { ... }
```
`@serializer null` resolves to `plumber:::serializer_identity_`, which does **only** `res$body <- val; res$toResponse()` — it never touches headers. This lets the handler's own `res$setHeader("Content-Type", ctype)` / `res$setHeader("Content-Disposition", ...)` calls be the single source of truth, with no post-hoc header injection and no JSON-wrapping of raw bytes.

Verified via `plumber:::registered_serializers()` → `"null"` is a valid tag name, and `plumber:::get_registered_serializer("null")` resolves to `serializer_identity_`.

### 2) Restore `@param body:object` for correct OpenAPI generation
Re-added the annotation that documents `body` as a JSON object parameter (not a bare formal). Confirmed via direct spec inspection:
```r
pr <- plumber::plumb(path_to_plumber_R)
spec <- pr$getApiSpec()
op <- spec$paths[["/table/download"]][["post"]]
# before fix: requestBody = NULL, 1 query parameter named "body" (string)
# after fix:  requestBody present, 0 parameters
```

### 3) Verification method — do not trust `pr$call()` alone for serializer/header correctness
`pr$call()`-based `testthat` tests passed throughout, including during the broken intermediate state (JSON-wrapped base64 body). The regression and its fix were only confirmed by:
- Starting the real API as a background process (`piptm::run_api()`).
- Issuing real HTTP requests with `curl.exe -i` and inspecting raw header lines directly.
- Calling `pr$getApiSpec()` directly and inspecting `requestBody` / `parameters` on the live spec object, not just reading roxygen comments.

## Prevention

- **Never remove a route's `@serializer` annotation to "fix" a header problem.** If no custom header handling is needed, that's fine implicitly, but once a route returns a non-JSON object (raw bytes, HTML, etc.), it must always carry an explicit serializer; otherwise plumber's default JSON serializer will silently corrupt the body.
- **When a route sets its own response headers and also carries a `@serializer`, use `@serializer null` (identity)** so the two don't double-write `Content-Type`/other headers. Reserve `@serializer contentType list(type = ...)` for routes that do *not* set headers manually.
- **Every plumber endpoint with a JSON request body must have an explicit `@param <name>:object` annotation.** A bare function formal with a default value and no annotation is silently (mis)inferred as a query parameter by the OpenAPI generator — this is a recurring class of bug in this codebase (see Related).
- **After any plumber endpoint change touching serializers, headers, or parser directives, verify against a live running API with raw HTTP tooling (`curl -i`) and `pr$getApiSpec()` directly** — do not rely solely on `pr$call()`-based unit tests for these concerns, since the test harness does not reproduce real HTTP serialization behavior faithfully enough to catch this class of bug.

## Related
- `.cg-docs/solutions/bugs/2026-08-31-swagger-req-parameter-exposed.md` — identical root cause (missing `@param` annotation) for a different endpoint (`/description`).
- `.cg-docs/solutions/bugs/2026-10-16-table-download-parser-and-size-guard.md` — original hardening pass for this same endpoint; this entry's fix supersedes that entry's serializer choice (`@serializer contentType` → `@serializer null`).
- `.cg-docs/solutions/testing-patterns/2026-05-13-plumber-programmatic-testing-with-fixtures.md` — `pr$call()` testing patterns; note the limitation this entry adds (serializer/header bugs need live-server verification).
- `.cg-docs/solutions/bugs/2026-10-22-table-download-format-contract-leakage.md` — a third, independent bug class found later on this same endpoint: silent data corruption from blindly coercing `value` to numeric after a sibling endpoint's output contract changed.
