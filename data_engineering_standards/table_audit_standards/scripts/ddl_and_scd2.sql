-- ============================================================================
-- Audit-field DDL and historization samples (BigQuery).
-- Replace <project> and business columns to fit each table.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. RAW table: business columns + the raw-zone audit columns.
--    (Terraform creates these; DDL shown for clarity.)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `<project>.raw_oracle_sys.v_trades` (
  -- business columns -------------------------------------------------------
  trade_id           INT64,
  last_updated_date  TIMESTAMP,
  counterparty       STRING,
  notional           NUMERIC,
  -- audit columns (leading underscore = technical metadata, sorts together) -
  _source_system     STRING,      -- origin system, e.g. "oracle_sys"
  _source_object     STRING,      -- source view name
  _run_id            STRING,      -- pipeline run / correlation id
  _loaded_at         TIMESTAMP,   -- when the row landed in raw (UTC)
  _loaded_by         STRING,      -- identity that wrote it (service account)
  _record_hash       STRING       -- hash of business columns (change detection)
)
PARTITION BY DATE(_loaded_at)     -- partition on load date for cheap pruning
CLUSTER BY trade_id;

-- Adding audit columns to a table that predates the standard:
ALTER TABLE `<project>.raw_oracle_sys.v_trades`
  ADD COLUMN IF NOT EXISTS _source_system STRING,
  ADD COLUMN IF NOT EXISTS _source_object STRING,
  ADD COLUMN IF NOT EXISTS _run_id        STRING,
  ADD COLUMN IF NOT EXISTS _loaded_at     TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _loaded_by     STRING,
  ADD COLUMN IF NOT EXISTS _record_hash   STRING;

-- ---------------------------------------------------------------------------
-- 2. CURATED dimension with SCD Type 2 historization columns.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `<project>.cur_markets.dim_counterparty` (
  -- business columns
  counterparty_id    INT64,
  counterparty_name  STRING,
  rating             STRING,
  -- SCD2 + audit columns
  _record_hash       STRING,      -- hash of the business columns above
  _valid_from        TIMESTAMP,   -- when this version became effective (UTC)
  _valid_to          TIMESTAMP,   -- when it was superseded; NULL while current
  _is_current        BOOL,        -- TRUE for the live version of each key
  _run_id            STRING,      -- run that wrote this version
  _dbt_loaded_at     TIMESTAMP    -- when the model built (UTC)
)
PARTITION BY DATE(_valid_from)
CLUSTER BY counterparty_id;

-- ---------------------------------------------------------------------------
-- 3. SCD Type 2 MERGE: close changed versions, insert new ones.
--    S is the staging model (one row per key, with a fresh _record_hash).
-- ---------------------------------------------------------------------------
MERGE `<project>.cur_markets.dim_counterparty` T
USING (
  SELECT
    counterparty_id, counterparty_name, rating,
    TO_HEX(MD5(TO_JSON_STRING(STRUCT(counterparty_name, rating)))) AS _record_hash
  FROM `<project>.stg_sys.stg_sys__counterparty`
) S
ON  T.counterparty_id = S.counterparty_id
AND T._is_current = TRUE
-- Close the current version only when the business data actually changed:
WHEN MATCHED AND T._record_hash != S._record_hash THEN UPDATE SET
  T._valid_to   = CURRENT_TIMESTAMP(),
  T._is_current = FALSE
-- Insert the first version of a brand-new key:
WHEN NOT MATCHED BY TARGET THEN INSERT (
  counterparty_id, counterparty_name, rating, _record_hash,
  _valid_from, _valid_to, _is_current, _run_id, _dbt_loaded_at
) VALUES (
  S.counterparty_id, S.counterparty_name, S.rating, S._record_hash,
  CURRENT_TIMESTAMP(), NULL, TRUE, @run_id, CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 4. Audit / lineage queries these columns enable.
-- ---------------------------------------------------------------------------

-- Which run last loaded each raw table, and how many rows it wrote:
SELECT _source_object, _run_id, MAX(_loaded_at) AS last_loaded_at, COUNT(*) AS rows
FROM `<project>.raw_oracle_sys.v_trades`
GROUP BY _source_object, _run_id
ORDER BY last_loaded_at DESC;

-- Full history of one dimension key (every version, newest first):
SELECT counterparty_id, counterparty_name, rating,
       _valid_from, _valid_to, _is_current
FROM `<project>.cur_markets.dim_counterparty`
WHERE counterparty_id = 42
ORDER BY _valid_from DESC;
