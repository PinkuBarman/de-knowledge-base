-- dbt macros that stamp audit columns on every model. Put in macros/audit.sql.
-- These are Jinja+SQL; dbt compiles them to plain BigQuery SQL at build time.

-- record_hash(columns): deterministic, null-safe fingerprint of business columns.
-- Usage: {{ record_hash(['counterparty_name', 'rating']) }} AS _record_hash
{% macro record_hash(columns) %}
    TO_HEX(MD5(TO_JSON_STRING(STRUCT(
        {{ columns | join(', ') }}
    ))))
{% endmacro %}

-- audit_columns(): the standard dbt-zone audit columns, added to every model's
-- final SELECT. run_id is passed in via --vars so it matches the pipeline run.
-- Usage (last lines of a model):
--    ...business columns...,
--    {{ audit_columns() }}
{% macro audit_columns() %}
    '{{ this.identifier }}'                 AS _dbt_model,        -- model that built the row
    invocation_id                            AS _dbt_invocation_id,-- unique dbt run id
    '{{ var("run_id", invocation_id) }}'     AS _run_id,          -- pipeline correlation id
    CURRENT_TIMESTAMP()                      AS _dbt_loaded_at     -- when the model built (UTC)
{% endmacro %}
