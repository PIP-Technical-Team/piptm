---
date: 2026-04-14
title: "Hardcoded network paths in .onLoad() break portability across machines"
category: "environment-issues"
type: "environment"
language: "R"
tags: [onLoad, zzz.R, Renviron, Sys.getenv, network-paths, portability, piptm, ci]
root-cause: ".onLoad() called set_arrow_root() and set_manifest_dir() with hardcoded //server/... paths, causing the package to fail to load on any machine where the drive letter or mount point differed"
severity: "P1"
test-written: "no"
fix-confirmed: "yes"
---

# Hardcoded Network Paths in `.onLoad()` Break Cross-Machine Portability

## Symptom

The `piptm` package loaded correctly on the original developer's machine
(where `Y:/` was mapped to the PIP network share) but failed silently or with
unexpected errors on:

- Team members with a different drive letter mapping (e.g. `Z:/`).
- CI/CD runners with no access to the network share.
- New developers who hadn't yet mapped the share.

The `.onLoad()` function called `set_arrow_root()` and `set_manifest_dir()`
with literal `Y:/PIP_ingestion_pipeline_v2/...` strings, so any deviation from
that exact path silently left the package in a broken state (paths set to a
nonexistent location).

## Root Cause

Hardcoded absolute paths were embedded directly in `R/zzz.R`:

```r
# Before — hardcoded, non-portable
.onLoad <- function(libname, pkgname) {
  set_arrow_root("Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow")
  set_manifest_dir("Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests")
  ...
}
```

These were the original developer's exact mount points. They were never meant
to be committed but were added for convenience during early development and
never replaced.

## Fix

Replace hardcoded paths with `Sys.getenv()` reads. Each team member sets the
variables once in `~/.Renviron` (via `usethis::edit_r_environ()`). When unset
(CI, new developer), the package starts in **dev/testing mode** with NULL slots
and gracefully waits for `set_arrow_root()` / `set_manifest_dir()` to be called
at runtime.

```r
# R/zzz.R — after fix
.onLoad <- function(libname, pkgname) {

  # Initialise slots to NULL
  .piptm_env$manifest_dir    <- NULL
  .piptm_env$arrow_root      <- NULL
  .piptm_env$manifests       <- list()
  .piptm_env$current_release <- NULL

  arrow_root_opt   <- Sys.getenv("PIPTM_ARROW_ROOT",   unset = "")
  manifest_dir_opt <- Sys.getenv("PIPTM_MANIFEST_DIR", unset = "")

  # Only assign arrow root if the path actually exists on this machine
  if (nzchar(arrow_root_opt) && dir.exists(arrow_root_opt)) {
    .piptm_env$arrow_root <- arrow_root_opt
  }

  # If manifest dir is unset, start in dev/testing mode
  if (!nzchar(manifest_dir_opt)) {
    return(invisible(NULL))
  }

  tryCatch(
    .load_manifests(manifest_dir_opt),
    error = function(e) {
      packageStartupMessage(
        "[piptm] Failed to load manifests: ", conditionMessage(e),
        "\n  Call piptm::set_manifest_dir() to retry after fixing the path."
      )
    }
  )

  invisible(NULL)
}
```

### `.Renviron` setup (each team member, once)

```
PIPTM_ARROW_ROOT=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow
PIPTM_MANIFEST_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests
```

Open via: `usethis::edit_r_environ()`

### Runtime fallback (for CI or one-off sessions)

```r
piptm::set_arrow_root("//w1wbgencifs01/pip/...")
piptm::set_manifest_dir("//w1wbgencifs01/pip/...")
```

### `dir.exists()` guard

The arrow root assignment includes `dir.exists()` so that setting
`PIPTM_ARROW_ROOT` to a valid string on a machine where the network share is
temporarily unmounted does not assign an inaccessible path:

```r
if (nzchar(arrow_root_opt) && dir.exists(arrow_root_opt)) {
  .piptm_env$arrow_root <- arrow_root_opt
}
```

## Prevention

**Rule**: Never commit absolute paths to `.onLoad()` or any package file.
Network paths and machine-specific mount points must always be externalised via
environment variables.

**Pattern to follow**:
- Read from `Sys.getenv("MY_PKG_ROOT", unset = "")`.
- Guard with `nzchar()` before using the value.
- For paths that must exist, add `dir.exists()` / `file.exists()` before
  assigning.
- Always provide a runtime override function (`set_arrow_root()`) so CI and
  one-off sessions can configure without `~/.Renviron`.
- Document both env vars and runtime overrides in `README.md`.

**Anti-pattern**:
```r
# NEVER do this in .onLoad()
set_arrow_root("C:/my/local/path")   # breaks on all other machines
set_arrow_root("Y:/network/path")    # breaks if drive letter differs
```

## Related

- `README.md` — documents `PIPTM_ARROW_ROOT` and `PIPTM_MANIFEST_DIR` env vars
  with full setup instructions
