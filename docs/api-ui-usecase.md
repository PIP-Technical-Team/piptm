# Table Maker API — Swagger walkthrough

Open the QA Swagger. **Try it out**. For lists, add one value per row.

---

## 1. Check the API

`GET /health` — no params

`GET /releases` — no params

---

## 2. Step 1 — surveys

`GET /surveys-ui` — no params

`POST /session/surveys`

| Field | Value |
|-------|--------|
| `pip_id` | `COL_2019_GEIH_INC_ALL` |
| `pip_id` | `BRA_2004_PNAD_INC_ALL` |

---

## 3. Step 2, Decision 1 — sample base

`GET /categories`

| Field | Value |
|-------|--------|
| `pip_id` | `COL_2019_GEIH_INC_ALL` |
| `pip_id` | `BRA_2004_PNAD_INC_ALL` |

---

## 4. Step 2, Decision 2 — poverty

`GET /analysis-variables` — no params

`GET /statistics` — no params

---

## 5. Step 2, Decision 3 — slice

`GET /covariates`

| Field | Value |
|-------|--------|
| `pip_id` | `COL_2019_GEIH_INC_ALL` |
| `pip_id` | `BRA_2004_PNAD_INC_ALL` |

---

## 6. Generate the table

`GET /table` (or `POST /table`)

| Field | Value |
|-------|--------|
| `analysis_var` | `pov_status` |
| `pip_id` | `COL_2019_GEIH_INC_ALL` |
| `pip_id` | `BRA_2004_PNAD_INC_ALL` |
| `measures` | `headcount` |
| `measures` | `poverty_gap` |
| `poverty_line` | `3` |
| `by` | `gender` |
| `by` | `area` |
| `filter_base` | `{"educat4":[2,3]}` |
| `ppp` | `2021` |
