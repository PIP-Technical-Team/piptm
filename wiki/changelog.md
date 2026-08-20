# Changelog

## Version History

<!-- cg:auto:version-history -->

### v1.0.0 (unreleased)

Initial release of `{piptm}` — the Table Maker computation engine.

**Features:**
- 19 measures across poverty (5), inequality (2), and welfare (12) families
- Multi-survey batch processing with `collapse::GRP` up to 72% faster than per-survey iteration
- Arrow/Parquet I/O with column pruning (68% faster reads)
- Manifest-First lazy validation architecture
- Deterministic, reproducible computations

**Known limitations:**
- Weighted quantiles not supported as Arrow push-down; evaluated in R
- Gini requires pre-sorted welfare (enforced by upstream pipeline contract)

<!-- cg:auto:end -->

---

← [Home](README.md)
