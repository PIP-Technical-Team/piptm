# `/table` response format — mock payloads for UI review

Two complete, valid JSON responses in the **proposed** `/table` format.
Source of truth for the design: `../2026-10-19-table-endpoint-layout-slot-response-format.md`.

These are proposals, not live endpoint output. Please review and confirm
before implementation starts.

## The files

| File | Request | Slots used |
|---|---|---|
| `example-a-three-covariates.json` | `by=gender&by=educat4&by=area` | columns, rows, superColumns |
| `example-b-one-covariate.json` | `by=gender` | columns only |

Both describe the same survey, `COL_2008_GEIH_INC_ALL`, with `measures=mean`.

## The one thing to look at first

**The schema does not change with the number of covariates.**

Every cell always carries all four slot keys — `superRow`, `row`,
`superColumn`, `column` — and unused slots are `null`, in both `layout` and
every cell. Compare the two files: example B uses one covariate, so three
slots are `null`, but the key names and the structure are identical to
example A. One rendering path, no branching on covariate count.

## How slots are assigned

Slot assignment is **positional in the `by` parameter**:

| `by` position | Slot |
|---|---|
| 1 | columns |
| 2 | rows |
| 3 | super columns |
| 4 | super rows |

So `by=gender&by=educat4&by=area` means gender is columns, educat4 is rows,
area is super columns, and super rows is unused. Reorder `by` and the slot
assignment changes with it.

Covariates fill slots in order from the top; trailing slots stay empty.
**Gapped assignments are not possible** — a covariate cannot land in super
rows while columns is empty. This is intentional, and it means the UI's
Decision 3 should present an *ordered selection* of covariates rather than
four independently fillable drop targets.

## How to render

1. Read `layout`. Each non-null slot gives you `varname`, a display `label`,
   and an **ordered** `categories` array. Build your headers from this — do
   not discover categories by scanning the data.
2. Walk `cells`. Each cell gives you the `{code, label}` for each slot it
   occupies, so placement is a direct index lookup against `layout`.
3. Render `measures` inside the cell. `population` sits outside `measures`
   because it is cell metadata, not a user-selected statistic.

`cells` is **dense and deterministically ordered**: sorted by the index of
each category within `layout.superRows.categories`, then `layout.rows`,
then `layout.superColumns`, then `layout.columns`. Null slots are skipped in
the sort. Every cell you need to render is present.

## Notes

- **No dynamic keys.** The earlier sketch used variable-named keys
  (`"area": 0`), which forces you to read `varname` before you can read the
  code. Here `varname` lives once in `layout`; cells carry only
  `{code, label}`.
- **Multiple measures** nest inside `measures`, e.g.
  `"measures": { "mean": 10.47, "median": 8.91, "gini": 0.513 }`. Up to four
  are supported. The requested set and its order are echoed in the
  survey-level `measures` array, so you can render the in-cell stacked list
  in the user's chosen order.
- **Multi-survey requests** append more objects to `data`, one per `pip_id`,
  each with its own `layout` — mapping directly onto the per-survey tabs in
  Step 3.
- **`by` with no covariates** is legal: `layout` has all four slots `null`
  and `cells` holds exactly one cell with all four slot keys `null`.
- **NA categories are real cells.** `educat4: null` in example A is the
  missing-education group. It renders like any other category; the label is
  a placeholder here and will come from the registry.
- **Cells are never dropped.** You will always receive every cell. Empty or
  otherwise special cells carry a backend-defined value — you render what you
  receive and never interpret or fill in a value.
- **`varname` is omitted from cells** to avoid repeating it on every cell. If
  you would rather have it inline, say so — it is a cheap addition.

## Super rows

Neither file exercises `superRows`, because no four-covariate figures were
available to build a mock from without inventing numbers. It behaves
identically to the other three slots. With `by=gender&by=educat4&by=area&by=urbanrural`
the `layout` block would read:

```json
"layout": {
  "columns":      { "varname": "gender",  "label": "Gender",          "categories": [] },
  "rows":         { "varname": "educat4", "label": "Education level", "categories": [] },
  "superColumns": { "varname": "area",    "label": "Urban / Rural",   "categories": [] },
  "superRows":    { "varname": "region",  "label": "Region",          "categories": [] }
}
```

and every cell would carry a non-null `superRow` alongside the other three.
Structure only — `categories` elided.

## Data provenance

- **Example A** uses the exact values the current `/table` endpoint returns
  for this request. Only the structure differs from today's response.
- **Example B** values are *derived* from example A by population-weighted
  aggregation over the area and education cells. The aggregation identity is
  exact, but the inputs are rounded to 4 decimal places, so the derived means
  are accurate to roughly 4 dp rather than being byte-identical to what the
  endpoint would emit. Use them to validate structure, not to validate
  numerics.

## Questions we would like answered

1. Does the fixed four-key cell schema work for your renderer, or do you
   specifically need the nested tree shape from the original proposal?
2. Is `layout`-driven ordering sufficient, or do you need categories repeated
   somewhere else?
3. Do you want `varname` inline on each cell?
4. Does the ordered-selection model for Decision 3 cause any UX problem on
   your side?
