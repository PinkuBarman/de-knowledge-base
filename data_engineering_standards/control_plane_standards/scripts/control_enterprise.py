"""
Enterprise control-plane helpers for the Oracle -> BigQuery ELT pipeline.

WHAT THIS MODULE OWNS
    Every WRITE to the control dataset (``ctl_edp``):
      * ``ingestion_watermark``          - one row per source view; drives incremental loads
      * ``pipeline_run_log``             - one row per DAG run; drives the Tableau banner
      * ``dq_check_results``             - one row per data-quality check per run (audit)
      * ``tableau_extract_refresh_log``  - one row per Tableau extract refresh (audit)
    It is imported by the ingestion package (watermark + DQ writes) and by the
    Airflow DAG (run-log + refresh-log writes).

SECURITY / WRITE STANDARDS ENFORCED HERE
    1. Parameterized DML only - values are bound, never formatted into SQL text,
       so there is no injection surface and no legacy streaming buffer.
    2. Identifiers (table/column names) are allow-listed before they can reach a
       SQL string - the one place a value could otherwise be concatenated in.
    3. Log writes are idempotent (MERGE ... WHEN NOT MATCHED on a natural key),
       so a retried Airflow task can never create a duplicate row.
    4. The watermark never moves backwards unless the caller explicitly opts in
       (deliberate backfill reset), guarding against accidental data loss.
    5. Every row records who wrote it (``written_by``) and the run it belongs to
       (``run_id``), so the audit trail answers "who, and in which run".
"""
from __future__ import annotations

import datetime as dt
from typing import Any

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants and allow-lists
# ---------------------------------------------------------------------------

# All control tables live in this one dataset.
CONTROL_DATASET = "ctl_edp"

# The ONLY tables this module may write to. A table name that is not in this set
# is rejected before it can be interpolated into a SQL string (rule 2 above).
ALLOWED_TABLES: set[str] = {
    "ingestion_watermark",
    "pipeline_run_log",
    "dq_check_results",
    "tableau_extract_refresh_log",
}

# The declared column -> BigQuery type for each control table. This map is the
# single source of truth used to (a) allow-list column names and (b) bind each
# value with the correct parameter type - which also lets us send a typed NULL
# (e.g. an empty data_as_of_ts on a failed run) that BigQuery will accept.
CONTROL_SCHEMAS: dict[str, dict[str, str]] = {
    "ingestion_watermark": {
        "view_name": "STRING",          # e.g. "v_trades" - identifies the view's row
        "watermark_column": "STRING",   # source column the high-watermark tracks
        "watermark_value": "TIMESTAMP", # highest value successfully loaded so far
        "last_run_id": "STRING",        # correlation_id of the load that set it
        "written_by": "STRING",         # service-account identity that wrote the row
        "updated_at": "TIMESTAMP",      # when this row was last written (UTC)
    },
    "pipeline_run_log": {
        "dag_id": "STRING",             # which DAG ran
        "run_id": "STRING",             # Airflow run_id, used as the correlation_id
        "status": "STRING",             # "SUCCESS" | "FAILED"
        "run_start_ts": "TIMESTAMP",    # when the run started (UTC)
        "run_end_ts": "TIMESTAMP",      # when the run finished (UTC)
        "data_as_of_ts": "TIMESTAMP",   # max watermark loaded this run; NULL on failure
        "failed_tasks": "ARRAY<STRING>",# task_ids that failed (empty on success)
        "dbt_image_digest": "STRING",   # exact dbt image the run used (provenance)
        "written_by": "STRING",         # service-account identity that wrote the row
    },
    "dq_check_results": {
        "run_id": "STRING",             # correlation_id, joins back to the run
        "dag_id": "STRING",
        "zone": "STRING",               # "raw" | "staging" | "curated"
        "object_name": "STRING",        # view or model the check ran on
        "check_name": "STRING",         # name of the check
        "severity": "STRING",           # "error" (blocks) | "warn" (continues)
        "passed": "BOOL",               # did the check pass?
        "observed": "STRING",           # short diagnostic, NEVER business data (<=512 chars)
        "written_by": "STRING",
        "checked_at": "TIMESTAMP",      # when the check ran (UTC)
    },
    "tableau_extract_refresh_log": {
        "run_id": "STRING",             # correlation_id, joins back to the run
        "datasource_id": "STRING",      # Tableau datasource that was refreshed
        "job_id": "STRING",             # Tableau REST API job id
        "status": "STRING",             # "SUCCESS" | "FAILED" | "TIMEOUT"
        "started_at": "TIMESTAMP",      # when the refresh was triggered (UTC)
        "finished_at": "TIMESTAMP",     # when it finished (UTC)
        "written_by": "STRING",
    },
}

# Natural key per table - the columns that make a row unique. The idempotent
# writer uses these so re-running a task never inserts the same logical row twice.
NATURAL_KEYS: dict[str, list[str]] = {
    "pipeline_run_log": ["dag_id", "run_id"],
    "dq_check_results": ["run_id", "object_name", "check_name"],
    "tableau_extract_refresh_log": ["run_id", "datasource_id"],
}


# ---------------------------------------------------------------------------
# Small internal helpers
# ---------------------------------------------------------------------------

def _utcnow() -> dt.datetime:
    """Current time as a timezone-aware UTC datetime (all control times are UTC)."""
    return dt.datetime.now(dt.timezone.utc)


def _sa_identity() -> str:
    """Return the service-account email running this code (for the written_by column).

    Read from Application Default Credentials. Falls back to "unknown" locally so
    the code still runs off-cloud (e.g. in unit tests).
    """
    try:
        import google.auth
        creds, _ = google.auth.default()
        return getattr(creds, "service_account_email", None) or "unknown"
    except Exception:
        return "unknown"


def _validate_identifiers(table: str, columns: list[str]) -> None:
    """Reject any table/column name not on the allow-list.

    Identifiers cannot be passed as query parameters, so this is the guard that
    stops a value ever being smuggled into SQL through a table or column name.
    """
    if table not in ALLOWED_TABLES:
        raise ValueError(f"table not allow-listed for control writes: {table!r}")
    allowed_cols = set(CONTROL_SCHEMAS[table])
    bad = [c for c in columns if c not in allowed_cols]
    if bad:
        raise ValueError(f"column(s) not allow-listed for {table}: {bad}")


def _param(name: str, bq_type: str, value: Any):
    """Build a typed BigQuery query parameter (scalar or array) for one value.

    ARRAY types (e.g. failed_tasks) use ArrayQueryParameter; everything else is a
    ScalarQueryParameter. A None value still carries its declared type, so a NULL
    lands in the right column type.
    """
    if bq_type.startswith("ARRAY<"):
        inner = bq_type[len("ARRAY<"):-1]          # "ARRAY<STRING>" -> "STRING"
        return bigquery.ArrayQueryParameter(name, inner, value or [])
    return bigquery.ScalarQueryParameter(name, bq_type, value)


def _append(client: bigquery.Client, project: str, table: str, row: dict) -> str:
    """Idempotently insert one row into a control table via parameterized DML.

    Uses MERGE ... WHEN NOT MATCHED on the table's natural key, so calling it
    twice for the same run (an Airflow retry) inserts the row at most once.
    DML is immediately consistent (unlike streaming), so the guard is reliable.

    Returns the SQL it executed (handy for logging and for tests).
    """
    keys = NATURAL_KEYS[table]
    _validate_identifiers(table, list(row) + keys)     # rule 2

    schema = CONTROL_SCHEMAS[table]
    cols = list(row)
    using_select = ", ".join(f"@{c} AS {c}" for c in cols)       # typed source row
    on_clause = " AND ".join(f"T.{k} = S.{k}" for k in keys)      # match on natural key
    col_list = ", ".join(cols)
    val_list = ", ".join(f"S.{c}" for c in cols)
    params = [_param(c, schema[c], row[c]) for c in cols]         # bind every value

    sql = (
        f"MERGE `{project}.{CONTROL_DATASET}.{table}` T\n"
        f"USING (SELECT {using_select}) S\n"
        f"ON {on_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})"
    )
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return sql


# ---------------------------------------------------------------------------
# Watermark: read and guarded upsert
# ---------------------------------------------------------------------------

def read_watermark(client: bigquery.Client, project: str, view: str) -> "dt.datetime | None":
    """Return the stored high-watermark for one view, or None if it has never run.

    A None result tells the caller this is a bootstrap (first) load and it should
    read the source with no lower bound. Do NOT default a missing watermark to
    "now" - that would silently skip all existing history.
    """
    sql = (
        f"SELECT watermark_value "
        f"FROM `{project}.{CONTROL_DATASET}.ingestion_watermark` "
        f"WHERE view_name = @view"
    )
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("view", "STRING", view)]))
    rows = list(job.result())
    return rows[0].watermark_value if rows else None


def upsert_watermark(
    client: bigquery.Client,
    project: str,
    view: str,
    watermark_column: str,
    new_value: "dt.datetime | None",
    run_id: str,
    allow_regression: bool = False,
) -> str:
    """Advance (or create) the watermark for one view - only ever forwards.

    Call this AFTER the load and its row-count check have succeeded. If the load
    failed, do not call it: the watermark stays put and the next run safely
    re-reads the same window.

    ``allow_regression`` must be set True to move the watermark backwards, which
    is only done for a deliberate backfill reset - never in normal operation.

    Returns the SQL it executed.
    """
    current = read_watermark(client, project, view)
    if (
        current is not None and new_value is not None
        and new_value < current and not allow_regression
    ):
        raise ValueError(
            f"watermark regression for {view!r}: {new_value} < {current}; "
            f"pass allow_regression=True for a deliberate backfill reset"
        )

    row = {
        "view_name": view,
        "watermark_column": watermark_column,
        "watermark_value": new_value,
        "last_run_id": run_id,
        "written_by": _sa_identity(),
        "updated_at": _utcnow(),
    }
    _validate_identifiers("ingestion_watermark", list(row))
    schema = CONTROL_SCHEMAS["ingestion_watermark"]
    cols = list(row)
    using_select = ", ".join(f"@{c} AS {c}" for c in cols)
    set_clause = ", ".join(f"{c} = S.{c}" for c in cols if c != "view_name")
    col_list = ", ".join(cols)
    val_list = ", ".join(f"S.{c}" for c in cols)
    params = [_param(c, schema[c], row[c]) for c in cols]

    # Upsert: update the view's row if it exists, otherwise insert it.
    sql = (
        f"MERGE `{project}.{CONTROL_DATASET}.ingestion_watermark` T\n"
        f"USING (SELECT {using_select}) S\n"
        f"ON T.view_name = S.view_name\n"
        f"WHEN MATCHED THEN UPDATE SET {set_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})"
    )
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return sql


# ---------------------------------------------------------------------------
# Public writers used by the DAG and the ingestion package
# ---------------------------------------------------------------------------

def log_run(
    client: bigquery.Client,
    project: str,
    dag_id: str,
    run_id: str,
    status: str,
    run_start_ts: dt.datetime,
    data_as_of_ts: "dt.datetime | None",
    failed_tasks: "list[str] | None",
    dbt_image_digest: "str | None",
) -> str:
    """Write one run record. status is "SUCCESS" (from publish_refresh_status)
    or "FAILED" (from alert_pipeline_failed). data_as_of_ts is NULL on failure so
    the banner keeps showing the last good time, not this broken run.
    """
    row = {
        "dag_id": dag_id,
        "run_id": run_id,
        "status": status,
        "run_start_ts": run_start_ts,
        "run_end_ts": _utcnow(),
        "data_as_of_ts": data_as_of_ts,
        "failed_tasks": failed_tasks or [],
        "dbt_image_digest": dbt_image_digest,
        "written_by": _sa_identity(),
    }
    return _append(client, project, "pipeline_run_log", row)


def record_check(
    client: bigquery.Client,
    project: str,
    run_id: str,
    dag_id: str,
    zone: str,
    object_name: str,
    check_name: str,
    severity: str,
    passed: bool,
    observed: str = "",
) -> str:
    """Record the result of one data-quality check. ``observed`` is a short
    diagnostic string only (e.g. "missing: [notional]") and is truncated so no
    business data can ever land in a control table.
    """
    row = {
        "run_id": run_id,
        "dag_id": dag_id,
        "zone": zone,
        "object_name": object_name,
        "check_name": check_name,
        "severity": severity,
        "passed": passed,
        "observed": observed[:512],
        "written_by": _sa_identity(),
        "checked_at": _utcnow(),
    }
    return _append(client, project, "dq_check_results", row)


def record_refresh(
    client: bigquery.Client,
    project: str,
    run_id: str,
    datasource_id: str,
    job_id: str,
    status: str,
    started_at: dt.datetime,
    finished_at: "dt.datetime | None",
) -> str:
    """Record one Tableau extract refresh. A failed refresh is logged and alerted
    but does not fail the pipeline run - the BigQuery data is still correct.
    """
    row = {
        "run_id": run_id,
        "datasource_id": datasource_id,
        "job_id": job_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "written_by": _sa_identity(),
    }
    return _append(client, project, "tableau_extract_refresh_log", row)
