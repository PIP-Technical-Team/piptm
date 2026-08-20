# Charter History — Table Maker

Deprecated sections migrated from `compound-gpid.md` on 2026-08-20.

---

## Architecture Notes (removed 2026-08-20)

The {piptm} package follows a **Manifest-First with Lazy Validation** architecture. On load, it reads all `manifest_*.json` files from `PIPTM_MANIFEST_DIR` into memory (keyed by release ID). No microdata is loaded at startup — all loading is on-demand per `table_maker()` call.

The data pipeline is:

```
Raw survey microdata
→ {pipdata} harmonization
→ Clean survey datasets (.qs2)
→ Arrow/Parquet partitions (partitioned by country_code / year / welfare_type)
→ Release manifest (reproducibility contract)
→ {piptm} computation engine
→ Structured cross-tabulated measure outputs
```

Key internal components:
- `load_survey_microdata()` — manifest lookup + lazy file validation + Arrow loading
- `compute_fgt()`, `compute_gini()`, `compute_mean_welfare()`, etc. — core measure functions
- `compute_measures()` — orchestrator across surveys and breakdown dimensions
- `table_maker()` — top-level API function

*Migrated to `compound-gpid.context.md`.*

---

## Roadmap (removed 2026-08-20)

- **Phase 0** — Arrow dataset generation: Master Arrow repository, partition pipeline in {pipdata}, release manifest generation (2–3 weeks)
- **Phase 1** — Computation engine: `.onLoad()` manifest system, measure functions (FGT, Gini, welfare stats), `compute_measures()` orchestrator (4–6 weeks)
- **Phase 2** — Data integration & release management: `load_survey_microdata()`, manifest inspection utilities, `table_maker()` top-level API (2–3 weeks)
- **Phase 3** — Validation & documentation: unit tests (≥90% coverage), performance benchmarks, developer and user docs

*Superseded by `roadmap.json` (use `@cg-roadmap` to manage).*

---

## Related Resources (removed 2026-08-20)

- `docs/project-context.md` — Detailed architecture, manifest schema, computation engine design
- `docs/roadmap.md` — Phased implementation plan
- `inst/schema/arrow-schema.json` — Arrow partition schema
- PIP platform: <https://pip.worldbank.org>

*Migrated to `compound-gpid.context.md`.*