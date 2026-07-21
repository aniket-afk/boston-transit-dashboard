# 🚇 Boston Transit Performance Pipeline

An end-to-end analytics engineering project that measures MBTA service performance
by comparing **real-time predictions** against the **scheduled timetable**. Built as
a production-style ELT pipeline: a Python extractor lands raw MBTA API data in
Snowflake, dbt transforms it into a tested star schema and analytical marts, and a
Streamlit dashboard surfaces the insights.

**Stack:** Python · MBTA V3 API · Snowflake · dbt · Streamlit

---

## Architecture

```
MBTA V3 API
    │  (Python extractor: pagination, retry/backoff, JSON:API flattening)
    ▼
RAW.MBTA               ← landing zone (predictions, routes, stops, schedules)
    │  (dbt)
    ▼
Staging  (views)       ← cleaned, typed, deduplicated
    │
    ▼
Marts    (tables)      ← star schema: dim_routes, dim_stops, fct_predictions
    │                     + 5 analytical marts
    ▼
Streamlit dashboard    ← route perf, neighborhoods, accessibility, service, anomalies
```

---

## The data model (star schema)

**Fact — `fct_predictions`**
Grain: one row per `(prediction_id, loaded_at)`. Each row is a prediction *observed
in a snapshot*. Delay is computed by joining predictions to the schedule on
`(trip_id, stop_id)`:

```
delay_seconds = predicted_arrival − scheduled_arrival
```

**Dimensions**
- `dim_routes` — one row per route; MBTA `route_type` decoded, direction arrays unpacked
- `dim_stops` — one row per stop; accessibility and location codes decoded to labels

---

## Design decisions (and why)

| Decision | Reasoning |
|---|---|
| **Append predictions, overwrite dimensions** | Predictions are immutable time-series events (keep history); routes/stops/schedules are current-state dimensions (refresh wholesale). |
| **Composite key `(prediction_id, loaded_at)`** | The same prediction is re-observed across snapshots — the ID alone repeats by design. Uniqueness is on the combination, enforced with `dbt_utils.unique_combination_of_columns`. |
| **Dedupe in staging, not at ingestion** | Land everything; resolve grain downstream with `QUALIFY ROW_NUMBER()`. Never lose data at the boundary. |
| **`LEFT JOIN` predictions → schedules** | Unscheduled (`ADDED`) and `SKIPPED` trips have no scheduled time, so `delay_seconds` is null by design rather than dropping the rows silently. |
| **Views for staging, tables for marts** | Staging is zero-storage and always fresh; marts pre-compute for fast dashboard reads. |
| **`confidence_flag` macro** | Small-sample aggregates are flagged (`<30` rows → `low_sample`) so misleading averages aren't read as signal. Written once as a macro, reused across marts (DRY). |

---

## Testing

Trust is enforced by tests, not assumed:
- **Grain** — `unique_combination_of_columns` on the fact's composite key
- **Referential integrity** — `relationships` tests wire every fact FK to its dimension
  (this caught **181 orphaned stop_ids** — predictions referencing stops the dimension
  hadn't loaded — which were then recovered)
- **not_null / unique** on natural keys across staging and dimensions

---

## The 5 marts

1. **`mart_route_performance`** — avg delay, on-time rate, and volume per route
2. **`mart_delays_by_neighborhood`** — delay by municipality (joins fact to `dim_stops`)
3. **`mart_accessibility_gaps`** — service quality by wheelchair-accessibility status
4. **`mart_service_summary`** — hourly service volume & delay (time-series rollup via `date_trunc`)
5. **`mart_delay_anomalies`** — statistical outliers via delay z-score (`avg/stddev OVER ()`, |z| ≥ 2)

---

## Findings

- **Buses run early, not late.** Across matched trips, average delay is negative
  (~1–3 min early). On-time rate ~96%. Anomaly detection surfaces only *early*
  outliers, because the whole distribution skews early.
- **Data-quality catch:** `schedule_relationship` in this feed contains only `SKIPPED`
  and blank (normal) — no `SCHEDULED`/`ADDED`. All 1,724 `SKIPPED` rows carry null
  delay (a skipped stop has no scheduled arrival), so they're correctly excluded from
  delay metrics. Verified rather than assumed.
- **Accessibility:** bus stops in the sample are near-universally accessible; a "Not
  Accessible" category doesn't appear. Extending this analysis to rail requires
  solving the rail schedule-match gap first.

*Caveat:* results are from a limited polling window; the pipeline is built to accumulate
real history once the extractor is scheduled.

---

## Running it

```bash
# 1. Environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Extract (lands raw data in Snowflake)
python -m extract.run          # or the individual loaders

# 3. Transform + test
cd transform
export DBT_PROFILES_DIR=$(pwd)
dbt deps
dbt build                      # builds all models, runs all tests

# 4. Dashboard
# Deploy dashboard/streamlit_app.py as a Streamlit-in-Snowflake app
```

Secrets (`.env`, key-pair `.p8`) are gitignored; see `.env.example`.

---

## Repo layout

```
extract/                 Python MBTA extractor + Snowflake loader
transform/
  models/staging/mbta/   cleaned, typed source models
  models/marts/transit/  dimensions, fact, and 5 marts
  macros/                confidence_flag
dashboard/               Streamlit app
```
