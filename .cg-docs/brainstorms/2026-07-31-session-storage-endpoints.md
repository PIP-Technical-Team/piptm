---
date: 2026-07-31
title: "Session Storage Endpoints for Survey Selection Persistence"
status: decided
scope: "Lightweight"
chosen-approach: "Approach A — In-Memory Environment Storage"
tags: [api, endpoints, session-storage, ui-integration]
---

# Session Storage Endpoints for Survey Selection Persistence

## Context

UI team requested endpoints to "store pip_ids" to support crash recovery / page refresh resilience. If a user is in Step 2 (variable selection) and the page refreshes or crashes, they would lose their Step 1 survey selections without some form of persistence.

This brainstorm evaluates the effort/cost of adding server-side session storage endpoints vs. recommending client-side localStorage.

## Requirements

1. **Session creation**: `POST /session/surveys` accepts `pip_id[]` array, returns a `session_id`
2. **Session retrieval**: `GET /session/:id/surveys` returns the stored `pip_id` array, or 404 if expired/missing
3. **No modification to existing endpoints**: `/categories`, `/covariates`, etc. continue accepting `pip_id[]` directly; UI fetches from session endpoint and passes values downstream
4. **Backwards compatibility**: Existing stateless API behavior unchanged
5. **Simple integration**: UI team decides how to use sessions (likely: fetch from `/session/:id`, then pass to other endpoints)

## Out of Scope

- Modifying existing endpoints (`/categories`, `/covariates`) to accept `session_id` parameter — they remain stateless
- Cross-device session sync (sessions are process-local)
- Long-term session persistence across API restarts
- Multi-worker session sharing (deferred until deployment actually uses multiple workers)

## Approaches Considered

### Approach A: In-Memory Environment Storage (Chosen)

Store sessions in a package-level environment (`.SESSION_STORE`) that lives in the plumber process's memory. Sessions expire after a configurable TTL (default 1 hour).

**Implementation pattern**:
```r
# In inst/plumber/helpers.R:
.SESSION_STORE <- new.env(parent = emptyenv())
.SESSION_TTL <- 3600L  # 1 hour

create_session <- function(pip_id) {
  session_id <- paste0(sample(c(letters, 0:9), 8, replace = TRUE), collapse = "")
  .SESSION_STORE[[session_id]] <- list(
    pip_id = pip_id,
    created_at = Sys.time()
  )
  session_id
}

get_session <- function(session_id) {
  if (!exists(session_id, envir = .SESSION_STORE)) return(NULL)
  session <- .SESSION_STORE[[session_id]]
  
  # Check TTL expiration
  if (difftime(Sys.time(), session$created_at, units = "secs") > .SESSION_TTL) {
    rm(list = session_id, envir = .SESSION_STORE)
    return(NULL)
  }
  
  session$pip_id
}

# In inst/plumber/plumber.R:
#* @post /session/surveys
function(pip_id = NULL, res) {
  if (is.null(pip_id) || length(pip_id) == 0L) {
    return(api_error("`pip_id` must be provided.", 400L, res))
  }
  out <- capture_with_warnings({
    session_id <- create_session(pip_id)
    list(session_id = session_id)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))
  api_response(out$result, warnings = out$warnings)
}

#* @get /session/<id>/surveys
function(id, res) {
  out <- capture_with_warnings({
    pip_id <- get_session(id)
    if (is.null(pip_id)) {
      stop("Session not found or expired.")
    }
    list(pip_id = pip_id)
  })
  if (!is.null(out$error)) return(api_error(out$error, 404L, res))
  api_response(out$result, warnings = out$warnings)
}
```

**Pros**:
- Fast to implement: ~2 hours (add helpers, add 2 endpoints, write tests)
- Zero dependencies: No Redis, no database, no external config
- Zero deployment overhead: Works immediately, no infrastructure changes
- Zero ongoing maintenance: No servers to monitor, no credentials to rotate

**Cons**:
- Process-local only: Sessions don't survive API restarts
- Multi-worker limitation: If deployed with multiple plumber processes, session created on worker A won't be visible to worker B
- Memory unbounded (no background cleanup): Expired sessions remain in memory until accessed again (acceptable for low-traffic v1)
- No persistence: Server restart clears all sessions

**When this breaks**:
- If API is deployed with multiple workers (`run_api(processes = 4)`)
- If API server restarts frequently (container orchestration, auto-scaling)
- If sessions need to persist longer than API uptime

**Migration path when limits hit**: 
Replace helpers with Redis-backed implementation (Approach B) — endpoint signatures remain identical, so no API contract change.

**Effort**: 2 hours (implementation + basic tests)

**Recommended?** Yes — for v1. Delivers feature quickly, avoids infrastructure complexity, easy to upgrade later if needed.

---

### Approach B: Redis-Backed Sessions (Production-Ready)

Use Redis as an external session store. All workers share the same Redis instance.

**Implementation**:
- Add `redux` dependency to DESCRIPTION
- Connect to Redis via environment variable (`REDIS_HOST`)
- Store sessions with automatic TTL expiration

**Pros**:
- Multi-worker safe
- Automatic cleanup via Redis EXPIRE
- Survives API restarts
- Scalable to millions of sessions

**Cons**:
- Infrastructure dependency (Redis server required in all environments)
- Deployment complexity (connection config, credentials, monitoring)
- New package dependency (`redux`)

**Effort**: 
- Initial: 4–6 hours (dependency, helpers, endpoints, local Redis testing, documentation)
- Deployment: 2–4 hours per environment (Redis installation, connection config)
- Ongoing: ~1 hour/month (monitoring, troubleshooting)

**Recommended?** No — over-engineering for current requirements. Revisit when/if multi-worker deployment becomes necessary.

---

### Approach C: SQLite-Backed Sessions (Middle Ground)

Use SQLite file as session store. Persistent across restarts, minimal setup.

**Pros**:
- Survives API restarts
- Minimal setup (just a file path)
- Low dependency (DBI + RSQLite)

**Cons**:
- File locking issues under high concurrency
- Manual cleanup of expired sessions (cron job needed)
- File management overhead (backups, rotation, disk monitoring)

**Effort**: 
- Initial: 3–4 hours
- Deployment: 1 hour
- Ongoing: ~30 minutes/month

**Recommended?** No — complexity falls between A and B without clear advantages over either.

## Decision

**Approach A** — In-Memory Environment Storage.

**Rationale**:
- Fastest to implement (2 hours) and delivers immediate value
- No infrastructure dependencies or deployment complexity
- Zero ongoing maintenance burden
- Limitations (process-local, no restart persistence) are acceptable for v1:
  - Current deployment is single-worker
  - API restarts are infrequent
  - Sessions are short-lived (user workflow is typically < 1 hour)
- Easy migration path to Redis (Approach B) if scaling requirements emerge — endpoint contracts remain unchanged

## Pushback Outcome

Considered recommending **client-side localStorage** instead (5 lines of JavaScript, zero server work). However, since Approach A is lightweight (2 hours) and UI team explicitly requested server-side endpoints, the implementation cost is low enough to proceed without pushback.

**Note for future**: If this feature sees low usage or if sessions frequently expire unused, revisit whether localStorage would have been sufficient.

## Edge Cases Handled

1. **Empty `pip_id` array**: Return 400 Bad Request
2. **Expired session**: Return 404 Not Found when TTL exceeded
3. **Non-existent session ID**: Return 404 Not Found
4. **Session ID collision**: Extremely unlikely (8-char alphanumeric = 2.8 trillion combinations), but `exists()` check prevents overwrite
5. **Memory growth**: No automatic background cleanup, but expired sessions are purged on access. For v1 traffic levels, memory impact is negligible.

## API Contract

### `POST /session/surveys`

**Description**: Create a new session storing a `pip_id` array.

**Request body** (JSON):
```json
{
  "pip_id": ["COL_2010_GEIH_INC_ALL", "BRA_2015_PNAD_CON_ALL"]
}
```

Or as query parameters:
```
POST /session/surveys?pip_id=COL_2010_GEIH_INC_ALL&pip_id=BRA_2015_PNAD_CON_ALL
```

**Success response** (200):
```json
{
  "status": "success",
  "data": {
    "session_id": "x7k9m2a4"
  },
  "warnings": [],
  "errors": [],
  "meta": {}
}
```

**Error responses**:
- 400 Bad Request: `pip_id` missing or empty
- 422 Unprocessable Entity: Unexpected error during session creation

**Session TTL**: 1 hour (3600 seconds) from creation

---

### `GET /session/:id/surveys`

**Description**: Retrieve the `pip_id` array for a given session.

**Path parameters**:
- `id`: Session identifier (e.g., `x7k9m2a4`)

**Success response** (200):
```json
{
  "status": "success",
  "data": {
    "pip_id": ["COL_2010_GEIH_INC_ALL", "BRA_2015_PNAD_CON_ALL"]
  },
  "warnings": [],
  "errors": [],
  "meta": {}
}
```

**Error responses**:
- 404 Not Found: Session doesn't exist or has expired

**Notes**:
- Each GET request refreshes the expiration check but does **not** extend the TTL (sessions expire 1 hour after creation, not last access)
- To extend TTL on access, modify `get_session()` to update `created_at` timestamp

## Next Steps

1. Add session helper functions to `inst/plumber/helpers.R`:
   - `.SESSION_STORE` environment initialization
   - `.SESSION_TTL` constant (3600L)
   - `create_session(pip_id)`
   - `get_session(session_id)`
2. Add two endpoints to `inst/plumber/plumber.R`:
   - `POST /session/surveys`
   - `GET /session/<id>/surveys`
3. Test with Insomnia:
   - Create session → verify `session_id` returned
   - Retrieve session → verify `pip_id` array matches
   - Retrieve expired session → verify 404
   - Retrieve non-existent session → verify 404
4. Document in API comments (roxygen already generates OpenAPI spec)
5. Communicate to UI team:
   - Endpoints are live
   - Sessions expire after 1 hour
   - `session_id` values are **not persistent across API restarts** (v1 limitation)
   - If multi-worker deployment is planned, flag this limitation early

## Future Considerations

### When to migrate to Redis (Approach B):

Trigger: Any of these conditions
- Deployment uses multiple plumber workers (`processes > 1`)
- API restart frequency increases (e.g., daily auto-deployments, container orchestration)
- Session expiration complaints from users ("my session disappeared!")
- Traffic scales beyond single-instance capacity

Migration effort: 2–3 hours (replace helper functions, update deployment config, test)

### Alternative: Client-Side localStorage

If session storage sees low usage or if UI team reports that sessions are frequently expiring unused, reconsider recommending **client-side localStorage**:

```javascript
// On Step 1 survey selection:
localStorage.setItem('selected_pip_ids', JSON.stringify(['COL_2010_...', 'BRA_2015_...']));

// On Step 2 page load (crash recovery):
const pipIds = JSON.parse(localStorage.getItem('selected_pip_ids') || '[]');
```

**Advantages over server-side sessions**:
- Zero server work
- Survives page refresh, tab close/reopen
- No expiration (persists until user clears browser data)
- No API restart dependency

**When localStorage isn't sufficient**:
- Cross-device sync required
- Compliance/security requires server-side audit trail
- localStorage quota exceeded (unlikely for pip_id arrays)

If UI team agrees localStorage meets their needs, deprecate session endpoints and remove in next major version.
