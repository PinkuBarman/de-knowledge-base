# Watermark & Control Tables — Simple Version

Sep 23, 2026 · @Pinku Barman

The easy-to-read implementation of the pipeline's control plane: the watermark that drives incremental loads and the tables that record each run. Every code block here is complete and runnable — no fragments, no `...` — and heavily commented so a developer can follow and implement it directly. For the hardened variant (idempotent DML, identifier allow-listing, CMEK, audit columns, regression guard), see the Enterprise Version.

## Overview and when to use this version

The control plane is four small BigQuery tables in the `ctl_edp` dataset that the pipeline reads and writes on every run. They hold no business data — only names, statuses, counts and timestamps — and they make the pipeline safe to re-run, provable after the fact, and honest about how fresh the data is.

| Table | Role |
| --- | --- |
| `ingestion_watermark` | Where each view's last successful load stopped (drives incremental loads). |
| `pipeline_run_log` | Did the run succeed, and how current is the data (drives the Tableau banner). |
| `dq_check_results` | Which quality checks ran and whether they passed. |
| `tableau_extract_refresh_log` | Which dashboards were refreshed and when. |

**Use this simple version when** you are standing the pipeline up for the first time, learning how the pieces fit, or running in a lower environment where the full enterprise controls are not yet required. It is correct and safe — values are always sent as bound parameters, the watermark only advances after a successful load, and loads are idempotent through MERGE — but it keeps the moving parts to a minimum.

**Move to the enterprise version when** the pipeline serves production dashboards or regulated data. That version adds idempotent audit writes (no streaming), identifier allow-listing, a watermark-regression guard, CMEK, retention, and per-row `written_by` provenance.

Everything below is complete and was run against a mock BigQuery client, with every SQL statement parsed to confirm it is valid BigQuery syntax.

## Control tables (DDL)

Run this once per environment. Replace `<project>` with the environment's GCP project id. `CREATE TABLE IF NOT EXISTS` makes it safe to re-run.

```sql
-- ============================================================================
-- Control tables (SIMPLE version) for the Oracle -> BigQuery ELT pipeline.
-- Run these once per environment against the ctl_edp dataset.
-- Replace <project> with the environment's GCP project id.
-- ============================================================================

-- One row per source view. Updated in place as each view's load advances.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.ingestion_watermark` (
  view_name        STRING    NOT NULL,   -- e.g. "v_trades"; identifies the row
  watermark_column STRING    NOT NULL,   -- source column the watermark tracks
  watermark_value  TIMESTAMP,            -- highest value successfully loaded
  last_run_id      STRING,               -- run/correlation id that set it
  updated_at       TIMESTAMP NOT NULL    -- when this row was last written (UTC)
);
-- Tiny table (~15 rows), so no partitioning or clustering is needed.

-- One row per DAG run. Drives the "data as of" banner in Tableau.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.pipeline_run_log` (
  dag_id           STRING    NOT NULL,   -- which DAG ran
  run_id           STRING    NOT NULL,   -- run/correlation id
  status           STRING    NOT NULL,   -- "SUCCESS" | "FAILED"
  run_start_ts     TIMESTAMP,            -- when the run started (UTC)
  run_end_ts       TIMESTAMP,            -- when the run finished (UTC)
  data_as_of_ts    TIMESTAMP,            -- how current the data is (max watermark)
  failed_tasks     ARRAY<STRING>,        -- task ids that failed (empty on success)
  dbt_image_digest STRING                -- dbt image the run used (provenance)
)
PARTITION BY DATE(run_end_ts)            -- partition by day for cheap time filters
CLUSTER BY dag_id;                       -- cluster so per-DAG queries scan less

-- One row per quality check per run. The evidence that checks actually ran.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.dq_check_results` (
  run_id       STRING    NOT NULL,       -- ties the check to its run
  dag_id       STRING,
  zone         STRING,                   -- "raw" | "staging" | "curated"
  object_name  STRING,                   -- view/model the check ran on
  check_name   STRING,                   -- e.g. "rowcount_matches"
  severity     STRING,                   -- "error" | "warn"
  passed       BOOL,                     -- did the check pass?
  observed     STRING,                   -- short diagnostic only, no business data
  checked_at   TIMESTAMP NOT NULL        -- when the check ran (UTC)
)
PARTITION BY DATE(checked_at)
CLUSTER BY dag_id, zone;

-- One row per Tableau extract refresh triggered by the pipeline.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.tableau_extract_refresh_log` (
  run_id        STRING   NOT NULL,       -- ties the refresh to its run
  datasource_id STRING   NOT NULL,       -- Tableau datasource refreshed
  job_id        STRING,                  -- Tableau REST API job id
  status        STRING,                  -- "SUCCESS" | "FAILED" | "TIMEOUT"
  started_at    TIMESTAMP,               -- when the refresh was triggered (UTC)
  finished_at   TIMESTAMP                -- when it finished (UTC)
)
PARTITION BY DATE(started_at)
CLUSTER BY datasource_id;
```

## The control module

Save as `edp_ingestion/control/control_simple.py`. It needs only `google-cloud-bigquery`. Each function does one job and is commented line by line.

```python
"""
Simple control-plane helpers for the Oracle -> BigQuery ELT pipeline.

This is the easy-to-read starter version. It does the essential job correctly:
    * reads and advances the incremental-load watermark
    * writes a run record and data-quality results

It deliberately keeps things minimal - one function per job, straightforward
parameterized queries, and BigQuery streaming inserts for the append-only logs
(the simplest way to add a row). For a hardened variant with idempotent DML,
identifier allow-listing, a watermark-regression guard and audit columns, see
the enterprise version of this module.

Every value is still sent as a query PARAMETER (never formatted into the SQL
string), so even the simple version is safe from SQL injection.
"""
from __future__ import annotations

import datetime as dt

from google.cloud import bigquery

# All control tables live in this dataset.
CONTROL_DATASET = "ctl_edp"


def _utcnow() -> dt.datetime:
    """Current time as a timezone-aware UTC datetime. All control times are UTC."""
    return dt.datetime.now(dt.timezone.utc)


def read_watermark(client: bigquery.Client, project: str, view: str) -> "dt.datetime | None":
    """Return the highest value already loaded for ``view``, or None on first run.

    None means "no watermark yet" -> the caller should load full history the first
    time. Never treat a missing watermark as "now", or you would skip all the
    existing rows.
    """
    sql = (
        f"SELECT watermark_value "
        f"FROM `{project}.{CONTROL_DATASET}.ingestion_watermark` "
        f"WHERE view_name = @view"
    )
    # @view is a bound parameter, so the view name can never break out into SQL.
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("view", "STRING", view)]))
    rows = list(job.result())
    return rows[0].watermark_value if rows else None


def write_watermark(
    client: bigquery.Client,
    project: str,
    view: str,
    watermark_column: str,
    new_value: dt.datetime,
    run_id: str,
) -> None:
    """Create or update the watermark row for one view.

    Call this ONLY after the load for this view has succeeded. If the load fails,
    don't call it: the watermark stays where it was and the next run re-reads the
    same window safely.

    Uses MERGE so the same call works whether the view's row already exists
    (update it) or not (insert it).
    """
    sql = f"""
        MERGE `{project}.{CONTROL_DATASET}.ingestion_watermark` T
        USING (
            SELECT
                @view  AS view_name,          -- which view this row is for
                @col   AS watermark_column,   -- the column the watermark tracks
                @value AS watermark_value,    -- highest value loaded this run
                @run   AS last_run_id,        -- the run that set it (for tracing)
                @now   AS updated_at          -- when we wrote it (UTC)
        ) S
        ON T.view_name = S.view_name
        WHEN MATCHED THEN UPDATE SET
            watermark_column = S.watermark_column,
            watermark_value  = S.watermark_value,
            last_run_id      = S.last_run_id,
            updated_at       = S.updated_at
        WHEN NOT MATCHED THEN INSERT ROW
    """
    params = [
        bigquery.ScalarQueryParameter("view", "STRING", view),
        bigquery.ScalarQueryParameter("col", "STRING", watermark_column),
        bigquery.ScalarQueryParameter("value", "TIMESTAMP", new_value),
        bigquery.ScalarQueryParameter("run", "STRING", run_id),
        bigquery.ScalarQueryParameter("now", "TIMESTAMP", _utcnow()),
    ]
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def log_run(
    client: bigquery.Client,
    project: str,
    dag_id: str,
    run_id: str,
    status: str,
    run_start_ts: dt.datetime,
    data_as_of_ts: "dt.datetime | None",
    failed_tasks: "list[str] | None" = None,
    dbt_image_digest: "str | None" = None,
) -> None:
    """Add one row to pipeline_run_log describing how this run ended.

    status is "SUCCESS" or "FAILED". data_as_of_ts is how current the data is
    (the max watermark loaded); leave it None on a failed run so the dashboard
    banner keeps showing the last good time.
    """
    row = {
        "dag_id": dag_id,                                     # which DAG ran
        "run_id": run_id,                                     # the run's id / correlation id
        "status": status,                                     # SUCCESS | FAILED
        "run_start_ts": run_start_ts.isoformat(),             # when it started (UTC)
        "run_end_ts": _utcnow().isoformat(),                  # when it finished (UTC)
        "data_as_of_ts": data_as_of_ts.isoformat() if data_as_of_ts else None,
        "failed_tasks": failed_tasks or [],                   # which tasks failed
        "dbt_image_digest": dbt_image_digest,                 # image used (provenance)
    }
    # insert_rows_json appends one row. It is the simplest way to add a row;
    # the enterprise version replaces it with idempotent parameterized DML.
    errors = client.insert_rows_json(
        f"{project}.{CONTROL_DATASET}.pipeline_run_log", [row])
    if errors:
        raise RuntimeError(f"failed to write run log: {errors}")


def record_check(
    client: bigquery.Client,
    project: str,
    run_id: str,
    dag_id: str,
    view: str,
    check_name: str,
    severity: str,
    passed: bool,
    observed: str = "",
) -> None:
    """Add one row to dq_check_results for a single quality check.

    ``observed`` is a short human-readable diagnostic (e.g. "missing: [notional]")
    - never sample rows or column values, so no business data reaches the table.
    """
    row = {
        "run_id": run_id,                    # ties the check back to its run
        "dag_id": dag_id,
        "zone": "raw",                       # raw | staging | curated
        "object_name": view,                 # the view/model the check ran on
        "check_name": check_name,            # e.g. "rowcount_matches"
        "severity": severity,                # error (blocks) | warn (continues)
        "passed": passed,                    # True/False
        "observed": observed[:512],          # short diagnostic only, capped
        "checked_at": _utcnow().isoformat(), # when the check ran (UTC)
    }
    errors = client.insert_rows_json(
        f"{project}.{CONTROL_DATASET}.dq_check_results", [row])
    if errors:
        raise RuntimeError(f"failed to write check result: {errors}")
```

## Wiring it in, and how it was verified

**The order of calls is the whole point.** Per view, the ingestion task does: read the watermark, extract rows beyond it, run the quality checks, load (idempotent MERGE), and only *then* advance the watermark. If any step fails, the watermark is never written, so the next run re-reads the same window safely.

```python
from edp_ingestion.control import control_simple as ctl

def extract_load(client, project, view, watermark_column, run_id):
    since = ctl.read_watermark(client, project, view)      # None on first run
    df = extract_from_oracle(view, since)                   # your extract step
    run_raw_quality_checks(df)                              # raises on failure
    load_to_raw_with_merge(df, view, run_id)               # idempotent load
    # Only now, after success, advance the watermark:
    ctl.write_watermark(client, project, view, watermark_column,
                        new_value=df[watermark_column].max(), run_id=run_id)
```

**Who writes the run log.** The DAG calls `log_run(..., status="SUCCESS")` from `publish_refresh_status` (which runs only after everything passed) and `log_run(..., status="FAILED")` from the failure-alert task. `record_check` is called from each quality check as it runs.

**How this code was verified.** Both the SQL and the Python here were checked before publishing:

- Every function was run against a mock BigQuery client — `read_watermark`, `write_watermark`, `log_run` and `record_check` all execute without error.
- Every SQL statement the module emits was parsed with a BigQuery SQL parser to confirm valid syntax.
- The DDL file was parsed the same way.

To run it against real BigQuery you need only `pip install google-cloud-bigquery` and Application Default Credentials (`gcloud auth application-default login`, or a service account on the Composer worker). Nothing else in the module needs changing.

**One simplification to be aware of.** `log_run` and `record_check` use streaming inserts (`insert_rows_json`), the simplest way to append a row. Streamed rows sit in a short-lived buffer and, on an Airflow retry, a row could be written twice. That is acceptable for lower environments; the enterprise version replaces this with idempotent parameterized DML so a retry never duplicates an audit row.
