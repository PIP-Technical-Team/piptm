# Changelog

<!-- cg:auto:version-history -->
## Version History

See `NEWS.md` in the package root for the authoritative, maintained version
history. This section mirrors high-level, user-facing changes as they are
captured through the compound-docs workflow.

Current state (as of this wiki's initialization): Phase 1 (Computation
Engine) is complete — the full `table_maker()` pipeline is implemented,
tested (414 passing at time of writing), and optimized (multi-survey batch
computation via compound `GRP()`, single batch call + keyed metadata join in
`table_maker()`, `collapse::set_collapse(nthreads = ...)` configured on
load).
<!-- cg:auto:end -->

← [Home](README.md)
