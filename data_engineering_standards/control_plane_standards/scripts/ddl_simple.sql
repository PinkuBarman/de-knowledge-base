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
