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
