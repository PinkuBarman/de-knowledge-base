-- ============================================================================
-- Control tables (ENTERPRISE version) for the Oracle -> BigQuery ELT pipeline.
-- Prefer creating these with Terraform so they are versioned and consistent; the
-- DDL below is the exact shape Terraform produces, shown for clarity and review.
--
-- Placeholders:
--   <project>  GCP project id for the environment (e.g. org-edp-prd)
--   <kms_key>  full CMEK resource name:
--              projects/<p>/locations/<r>/keyRings/<kr>/cryptoKeys/<k>
--
-- Enterprise hardening vs the simple version:
--   * CMEK on every table (customer-managed encryption key)
--   * partition_expiration_days for retention on the append-only logs
--   * dataset/table labels for cost and access reviews
--   * a written_by audit column on every table
--   * an append-only ingestion_watermark_history for full change history
-- ============================================================================

-- One row per source view; updated in place. Small, so no partitioning.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.ingestion_watermark` (
  view_name        STRING    NOT NULL,   -- e.g. "v_trades"; identifies the row
  watermark_column STRING    NOT NULL,   -- source column the watermark tracks
  watermark_value  TIMESTAMP,            -- highest value successfully loaded
  last_run_id      STRING,               -- run/correlation id that set it
  written_by       STRING,               -- service-account identity that wrote it
  updated_at       TIMESTAMP NOT NULL    -- when this row was last written (UTC)
)
OPTIONS (
  kms_key_name = "<kms_key>",            -- customer-managed encryption
  labels = [("domain", "edp"), ("layer", "control")]
);

-- Append-only history of every watermark change (audit). One row per change.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.ingestion_watermark_history` (
  view_name        STRING    NOT NULL,
  watermark_column STRING,
  watermark_value  TIMESTAMP,            -- the value set by this change
  last_run_id      STRING,               -- run that made the change
  written_by       STRING,
  changed_at       TIMESTAMP NOT NULL    -- when the change happened (UTC)
)
PARTITION BY DATE(changed_at)
OPTIONS (
  kms_key_name = "<kms_key>",
  partition_expiration_days = 400,       -- retention window; align with policy
  labels = [("domain", "edp"), ("layer", "control")]
);

-- One row per DAG run. Drives the Tableau banner and the freshness monitor.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.pipeline_run_log` (
  dag_id           STRING    NOT NULL,   -- which DAG ran
  run_id           STRING    NOT NULL,   -- run/correlation id
  status           STRING    NOT NULL,   -- "SUCCESS" | "FAILED"
  run_start_ts     TIMESTAMP,            -- when the run started (UTC)
  run_end_ts       TIMESTAMP,            -- when the run finished (UTC)
  data_as_of_ts    TIMESTAMP,            -- how current the data is (max watermark)
  failed_tasks     ARRAY<STRING>,        -- task ids that failed (empty on success)
  dbt_image_digest STRING,               -- dbt image the run used (provenance)
  written_by       STRING                -- service-account identity that wrote it
)
PARTITION BY DATE(run_end_ts)
CLUSTER BY dag_id
OPTIONS (
  kms_key_name = "<kms_key>",
  partition_expiration_days = 400,
  labels = [("domain", "edp"), ("layer", "control")]
);

-- One row per quality check per run. Evidence that checks ran and their outcome.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.dq_check_results` (
  run_id       STRING    NOT NULL,       -- ties the check to its run
  dag_id       STRING,
  zone         STRING,                   -- "raw" | "staging" | "curated"
  object_name  STRING,                   -- view/model the check ran on
  check_name   STRING,                   -- e.g. "rowcount_matches"
  severity     STRING,                   -- "error" | "warn"
  passed       BOOL,                     -- did the check pass?
  observed     STRING,                   -- short diagnostic only, no business data
  written_by   STRING,                   -- service-account identity that wrote it
  checked_at   TIMESTAMP NOT NULL        -- when the check ran (UTC)
)
PARTITION BY DATE(checked_at)
CLUSTER BY dag_id, zone
OPTIONS (
  kms_key_name = "<kms_key>",
  partition_expiration_days = 400,
  labels = [("domain", "edp"), ("layer", "control")]
);

-- One row per Tableau extract refresh triggered by the pipeline.
CREATE TABLE IF NOT EXISTS `<project>.ctl_edp.tableau_extract_refresh_log` (
  run_id        STRING   NOT NULL,       -- ties the refresh to its run
  datasource_id STRING   NOT NULL,       -- Tableau datasource refreshed
  job_id        STRING,                  -- Tableau REST API job id
  status        STRING,                  -- "SUCCESS" | "FAILED" | "TIMEOUT"
  started_at    TIMESTAMP,               -- when the refresh was triggered (UTC)
  finished_at   TIMESTAMP,               -- when it finished (UTC)
  written_by    STRING                   -- service-account identity that wrote it
)
PARTITION BY DATE(started_at)
CLUSTER BY datasource_id
OPTIONS (
  kms_key_name = "<kms_key>",
  partition_expiration_days = 400,
  labels = [("domain", "edp"), ("layer", "control")]
);

-- Consumer-facing view: the only object dashboards read. Exposes just the
-- freshness fields, never the raw control tables.
CREATE OR REPLACE VIEW `<project>.ctl_edp.v_refresh_status` AS
SELECT
  dag_id,
  MAX(IF(status = 'SUCCESS', data_as_of_ts, NULL)) AS data_as_of_ts,
  MAX(IF(status = 'SUCCESS', run_end_ts,    NULL)) AS last_success_ts,
  ARRAY_AGG(status ORDER BY run_end_ts DESC LIMIT 1)[OFFSET(0)] AS latest_status
FROM `<project>.ctl_edp.pipeline_run_log`
GROUP BY dag_id;
