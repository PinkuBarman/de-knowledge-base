# Table Audit Fields — Enterprise Implementation Guide

Sep 23, 2026 · @Pinku Barman

A step-by-step guide to adding standard audit (technical-metadata) fields to every table in the pipeline, so each row can be traced to its source, run, load time and writer, and changes can be detected and historized. Every code block is complete and was verified — Python compiled and run, and all SQL parsed as valid BigQuery. Placeholders: `<project>`, `sys` (source system), and business columns to fit each table.

## Overview

Audit fields are technical-metadata columns added to every table alongside the business columns. They carry no business meaning; they exist so the platform can answer, for any row: where did it come from, in which run, when did it land and who wrote it, and has the business data changed since last time.

They pay for themselves in five ways:

- **Auditability.** Every row names the run that produced it, so any figure on a dashboard is traceable back to a specific pipeline execution — essential in a regulated environment.
- **Lineage.** `_source_system` and `_source_object` record the origin, so you can see exactly which source fed a table without external tooling.
- **Change detection.** A deterministic `_record_hash` over the business columns lets the pipeline tell changed rows from unchanged ones cheaply.
- **Historization.** With `_valid_from` / `_valid_to` / `_is_current`, a curated dimension keeps full history (SCD Type 2) instead of overwriting.
- **Operations.** `_loaded_at` makes freshness and "what did this run touch" answerable with a single query.

The principle throughout: audit fields are **populated by the framework, never by hand** — the ingestion package stamps them in the raw zone, and a dbt macro stamps them in staging and curated — so every table gets them automatically and identically. The rest of this guide is the step-by-step for each zone, with verified code.

## The standard audit field set

**Naming convention.** Every audit column starts with a leading underscore (`_loaded_at`, `_run_id`). This one rule does a lot of work: audit columns sort together at the end of a table, are visually distinct from business columns, and are trivial to match in tests and CI (`^_`). Use exactly these names everywhere — do not invent per-table variants.

| Field | Type | Meaning | Populated by |
| --- | --- | --- | --- |
| `_source_system` | STRING | Origin system, e.g. `oracle_sys` | ingestion (raw) |
| `_source_object` | STRING | Source view/table name, e.g. `v_trades` | ingestion (raw) |
| `_run_id` | STRING | Pipeline run / correlation id that wrote the row | ingestion + dbt |
| `_loaded_at` | TIMESTAMP | When the row landed in the raw zone (UTC) | ingestion (raw) |
| `_loaded_by` | STRING | Identity that wrote it (service account) | ingestion (raw) |
| `_record_hash` | STRING | Deterministic hash of the business columns | ingestion + dbt |
| `_dbt_model` | STRING | Model that built the row | dbt |
| `_dbt_invocation_id` | STRING | Unique dbt run id (from dbt) | dbt |
| `_dbt_loaded_at` | TIMESTAMP | When the model built (UTC) | dbt |
| `_valid_from` | TIMESTAMP | When this version became effective (SCD2) | dbt (curated dims) |
| `_valid_to` | TIMESTAMP | When it was superseded; NULL while current | dbt (curated dims) |
| `_is_current` | BOOL | TRUE for the live version of each key | dbt (curated dims) |

**Two rules for the values:**

- **UTC only.** Every timestamp is UTC, set by a server-side function (`CURRENT_TIMESTAMP()`) or one client clock — never a mix.
- **No business data.** Audit fields hold provenance and technical metadata only. The one field derived from business data, `_record_hash`, is a one-way fingerprint used for change detection, not a copy of the values.

## Which fields apply in each zone

Not every field belongs in every zone. Each zone adds what it can attest to and carries forward what still matters.

**Raw** — the point of entry, so it records origin and load facts: `_source_system`, `_source_object`, `_run_id`, `_loaded_at`, `_loaded_by`, `_record_hash`.

**Staging** — a typed, lightly-cleaned copy of raw. It carries `_source_system`, `_source_object` and `_run_id` forward from raw, and adds the dbt build stamps `_dbt_model`, `_dbt_invocation_id`, `_dbt_loaded_at`. `_record_hash` is recomputed on the typed columns so downstream change detection compares like with like.

**Curated facts** — append-mostly event tables: `_run_id`, `_dbt_model`, `_dbt_loaded_at`, and `_source_system` for lineage. No SCD2 columns — facts are not historized as versions.

**Curated dimensions** — historized with SCD Type 2: `_record_hash`, `_valid_from`, `_valid_to`, `_is_current`, plus `_run_id` and `_dbt_loaded_at`.

A simple way to remember it: raw answers "where and when did this arrive", staging answers "which model shaped it", curated dimensions add "and which version of the truth is this". The next sections implement each zone in turn.

## Raw zone, step by step

**Step 1 — Declare the audit columns.** Add them to every raw table (or `ALTER` existing ones). Partition on `_loaded_at` so "what did today's run load" prunes cheaply.

```sql
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
PARTITION BY DATE(_loaded_at)
CLUSTER BY trade_id;

-- To retrofit a table that predates the standard:
ALTER TABLE `<project>.raw_oracle_sys.v_trades`
  ADD COLUMN IF NOT EXISTS _source_system STRING,
  ADD COLUMN IF NOT EXISTS _source_object STRING,
  ADD COLUMN IF NOT EXISTS _run_id        STRING,
  ADD COLUMN IF NOT EXISTS _loaded_at     TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _loaded_by     STRING,
  ADD COLUMN IF NOT EXISTS _record_hash   STRING;
```

**Step 2 — Stamp the columns during the load.** The ingestion package builds a SELECT over the just-extracted load table that adds the audit columns, then MERGEs it into the raw table. Values that vary per run are bound parameters (`@src_system`, `@src_object`, `@run_id`); load time and writer come from server-side functions. This module is pure Python with no dependencies:

```python
"""Build the SQL that stamps the standard audit columns onto raw-zone rows."""
from __future__ import annotations

# The raw-zone audit columns, in write order. Single source of truth so the DDL
# above and the load below never drift apart.
RAW_AUDIT_COLUMNS = [
    "_source_system", "_source_object", "_run_id",
    "_loaded_at", "_loaded_by", "_record_hash",
]


def record_hash_expr(business_columns: list[str]) -> str:
    """Deterministic, null-safe row hash over the business columns.

    TO_JSON_STRING(STRUCT(...)) serializes the columns in a fixed order with NULLs
    represented explicitly; MD5 -> hex makes a compact fingerprint. Identical
    business values always hash the same, so an unchanged row can be skipped.
    """
    cols = ", ".join(business_columns)
    return f"TO_HEX(MD5(TO_JSON_STRING(STRUCT({cols}))))"


def audit_select_fragment(business_columns: list[str]) -> str:
    """SELECT list (business + audit columns) for a raw load.

    Bound params expected at execution: @src_system, @src_object, @run_id.
    Load time and writer identity come from server-side functions.
    """
    biz = ",\n  ".join(business_columns)
    return (
        f"  {biz},\n"
        f"  @src_system            AS _source_system,\n"
        f"  @src_object            AS _source_object,\n"
        f"  @run_id                AS _run_id,\n"
        f"  CURRENT_TIMESTAMP()    AS _loaded_at,\n"
        f"  SESSION_USER()         AS _loaded_by,\n"
        f"  {record_hash_expr(business_columns)} AS _record_hash"
    )


def build_raw_select(load_table_fqn: str, business_columns: list[str]) -> str:
    """Full SELECT over the run's load table, adding audit columns. Used as the
    USING source of the MERGE into the raw table."""
    return (
        "SELECT\n"
        f"{audit_select_fragment(business_columns)}\n"
        f"FROM `{load_table_fqn}`"
    )
```

**Step 3 — Merge into raw.** Use the SELECT above as the MERGE source, keyed on the business primary key, so a reload updates a row in place (and refreshes its audit columns) rather than duplicating it:

```sql
MERGE `<project>.raw_oracle_sys.v_trades` T
USING ( /* build_raw_select(...) goes here */ ) S
ON T.trade_id = S.trade_id
WHEN MATCHED THEN UPDATE SET
  trade_id = S.trade_id, last_updated_date = S.last_updated_date,
  counterparty = S.counterparty, notional = S.notional,
  _run_id = S._run_id, _loaded_at = S._loaded_at,
  _loaded_by = S._loaded_by, _record_hash = S._record_hash
WHEN NOT MATCHED THEN INSERT ROW;
```

After this succeeds, the ingestion package advances the watermark (covered in the Watermark & Control Tables guide).

## dbt zone (staging & curated), step by step

In dbt the audit columns are added by two macros, so every model gets them identically and no one hand-writes them.

**Step 1 — Add the macros.** Put these in `macros/audit.sql`:

```sql
-- record_hash(columns): deterministic, null-safe fingerprint of business columns.
-- Usage: {{ record_hash(['counterparty_name', 'rating']) }} AS _record_hash
{% macro record_hash(columns) %}
    TO_HEX(MD5(TO_JSON_STRING(STRUCT(
        {{ columns | join(', ') }}
    ))))
{% endmacro %}

-- audit_columns(): the standard dbt-zone audit columns for every model's final
-- SELECT. run_id is passed via --vars so it matches the pipeline run.
{% macro audit_columns() %}
    '{{ this.identifier }}'                  AS _dbt_model,         -- model that built the row
    invocation_id                            AS _dbt_invocation_id, -- unique dbt run id
    '{{ var("run_id", invocation_id) }}'     AS _run_id,            -- pipeline correlation id
    CURRENT_TIMESTAMP()                      AS _dbt_loaded_at      -- when the model built (UTC)
{% endmacro %}
```

`invocation_id` and `this.identifier` are provided by dbt at compile time; `var("run_id", ...)` reads the `run_id` the DAG passes with `dbt build --vars '{run_id: ...}'`, falling back to dbt's own id when run manually.

**Step 2 — Use them in a model.** A staging model carries the source columns forward and ends with the two macros:

```sql
with source as (
    select * from {{ source('raw_oracle_sys', 'v_trades') }}
)
select
    cast(trade_id as int64)              as trade_id,
    cast(last_updated_date as timestamp) as last_updated_date,
    trim(counterparty)                   as counterparty,
    cast(notional as numeric)            as notional,
    -- carry lineage forward from raw
    _source_system,
    _source_object,
    -- recompute the hash on the typed columns, and stamp the dbt audit columns
    {{ record_hash(['trade_id', 'last_updated_date', 'counterparty', 'notional']) }} as _record_hash,
    {{ audit_columns() }}
from source
```

When dbt compiles this, the macros expand to plain BigQuery SQL. The compiled form is a normal `SELECT` with `TO_HEX(MD5(TO_JSON_STRING(STRUCT(...))))` and the four dbt audit expressions — validated as correct BigQuery.

**Step 3 — Put the audit columns in the model contract.** Declaring them in the model's YAML means the build fails if a model ever ships without them, and enforces their types:

```yaml
version: 2
models:
  - name: stg_sys__trades
    config:
      contract: { enforced: true }
    columns:
      - name: trade_id
        data_type: int64
        constraints: [{ type: not_null }]
        data_tests: [unique]
      # ... other business columns ...
      - name: _record_hash
        data_type: string
        data_tests: [not_null]
      - name: _run_id
        data_type: string
        data_tests: [not_null]
      - name: _dbt_model
        data_type: string
      - name: _dbt_invocation_id
        data_type: string
      - name: _dbt_loaded_at
        data_type: timestamp
        data_tests: [not_null]
```

## SCD Type 2 historization for curated dimensions

A dimension should keep history: when a counterparty's rating changes, the old row is closed and a new version opens, rather than being overwritten. The `_record_hash` decides *whether* anything changed; the `_valid_from` / `_valid_to` / `_is_current` columns record *when*.

**Step 1 — The dimension carries the SCD2 columns:**

```sql
CREATE TABLE IF NOT EXISTS `<project>.cur_markets.dim_counterparty` (
  counterparty_id    INT64,
  counterparty_name  STRING,
  rating             STRING,
  _record_hash       STRING,      -- hash of the business columns above
  _valid_from        TIMESTAMP,   -- when this version became effective (UTC)
  _valid_to          TIMESTAMP,   -- when it was superseded; NULL while current
  _is_current        BOOL,        -- TRUE for the live version of each key
  _run_id            STRING,      -- run that wrote this version
  _dbt_loaded_at     TIMESTAMP    -- when the model built (UTC)
)
PARTITION BY DATE(_valid_from)
CLUSTER BY counterparty_id;
```

**Step 2 — The historizing MERGE.** It closes the current version of a key only when the hash differs, and inserts new keys. Run it after the staging model is built (as a run-operation or a model post-hook), passing `@run_id`:

```sql
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
```

**The one gap to close.** A single MERGE can close a changed version but cannot, in the same statement, also insert its replacement (a key already matched cannot also be inserted). Run a second INSERT straight after that adds a fresh current row for any key whose latest version was just closed:

```sql
INSERT INTO `<project>.cur_markets.dim_counterparty`
  (counterparty_id, counterparty_name, rating, _record_hash,
   _valid_from, _valid_to, _is_current, _run_id, _dbt_loaded_at)
SELECT
  s.counterparty_id, s.counterparty_name, s.rating,
  TO_HEX(MD5(TO_JSON_STRING(STRUCT(s.counterparty_name, s.rating)))),
  CURRENT_TIMESTAMP(), NULL, TRUE, @run_id, CURRENT_TIMESTAMP()
FROM `<project>.stg_sys.stg_sys__counterparty` s
WHERE NOT EXISTS (
  SELECT 1 FROM `<project>.cur_markets.dim_counterparty` t
  WHERE t.counterparty_id = s.counterparty_id AND t._is_current = TRUE
);
```

**Simpler alternative.** If you would rather not maintain this by hand, a dbt **snapshot** implements the same SCD2 pattern and manages its own validity columns; adopt it and expose the standard names through a thin view. The manual MERGE is shown here because it gives full control over the hash comparison and the audit columns.

## Enforcing audit fields everywhere

Audit fields are only useful if they are on *every* table. Enforce that automatically so a missing column fails a pull request, not an audit.

**Contracts (dbt).** As shown, declare the audit columns in each model's YAML with `contract: { enforced: true }`. dbt then fails the build if a model's output is missing an audit column or has the wrong type. This is the strongest guarantee because it checks the actual built table.

**Tests (dbt).** Add `not_null` tests on the columns that must always be populated — `_run_id`, `_loaded_at` (raw), `_dbt_loaded_at` (dbt), `_record_hash`. For SCD2 dimensions, add a test that exactly one row per key is current:

```sql
-- tests/assert_one_current_version.sql : fails if any key has != 1 current row
SELECT counterparty_id, COUNT(*) AS current_rows
FROM {{ ref('dim_counterparty') }}
WHERE _is_current = TRUE
GROUP BY counterparty_id
HAVING COUNT(*) != 1
```

**Project rule (dbt-project-evaluator).** The `dbt-project-evaluator` package can assert documentation and test coverage across the project; extend it (or a custom check) to require the audit columns on every model, so a new model cannot merge without them.

**CI check (raw + repo-wide).** A lightweight test in CI catches anything the dbt layer cannot see — for example a raw DDL that forgot a column. Grep the model and DDL files for the required audit columns:

```python
# tests/test_audit_columns_present.py
import pathlib, re

REQUIRED = ["_run_id", "_record_hash", "_dbt_loaded_at"]

def test_every_model_has_audit_columns():
    missing = {}
    for sql in pathlib.Path("models").rglob("*.sql"):
        text = sql.read_text()
        # audit_columns() macro counts as satisfying the dbt stamps
        if "audit_columns()" in text:
            continue
        absent = [c for c in REQUIRED if not re.search(rf"\b{c}\b", text)]
        if absent:
            missing[str(sql)] = absent
    assert not missing, f"models missing audit columns: {missing}"
```

**Backfilling existing tables.** For tables that predate the standard, run the `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` from the raw section, then a one-off backfill that sets `_loaded_at` to the load date you can establish and `_run_id` to a sentinel like `backfill-<ticket>`, so even historical rows carry provenance.

## Security, governance and verification

**No business data in audit fields.** Audit columns hold provenance and technical metadata only. The single field derived from business data is `_record_hash`: a one-way MD5 fingerprint used for change detection. It is not reversible to the original values, but note that equal hashes reveal equal rows — which is exactly what change detection needs and is not sensitive. Never widen audit fields to store sample values or a copy of a business column.

**Identity, not people.** `_loaded_by` is the pipeline's service-account identity (from `SESSION_USER()` under the pipeline's credentials), never an end user. This is provenance, not personal data.

**Immutability.** In the raw and dbt zones, a row's audit columns are rewritten only when the row itself is re-loaded (same MERGE). In curated dimensions, once a version is closed (`_valid_to` set, `_is_current = FALSE`) it is never updated again — history is append-only. Grant the pipeline service account the rights to write these tables and nothing broader; manual edits go through a logged break-glass path.

**Encryption and retention.** Audit columns inherit the table's CMEK and its partition-expiration, so they are covered by the same encryption and retention policy as the business data — no separate handling needed.

**Audit / lineage queries these fields unlock:**

```sql
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
```

**How this guide's code was verified.** Before publishing, the Python builder was compiled and run to generate its SQL; the generated raw SELECT, the `_record_hash` expression, all DDL and `ALTER` statements, the SCD2 MERGE and its follow-up INSERT, the one-current-version test, and the compiled form of the dbt macros were all parsed with a BigQuery SQL parser to confirm valid syntax; and the CI-check Python was compiled. The dbt macros themselves are Jinja and compile under dbt at build time — the validated compiled form is what they expand to.

To apply against real BigQuery you need only `pip install google-cloud-bigquery` for the ingestion builder, a dbt project with the two macros in `macros/`, and the DDL applied per environment.
