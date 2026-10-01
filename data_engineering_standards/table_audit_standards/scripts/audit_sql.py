"""
Build the SQL that stamps the standard audit columns onto raw-zone rows.

Audit (a.k.a. technical-metadata) columns answer, for every row in every table:
  * where did it come from      -> _source_system, _source_object
  * in which run                -> _run_id
  * when did it land, and who   -> _loaded_at, _loaded_by
  * has the business data changed since last time -> _record_hash

This module produces the SELECT fragment used when the ingestion package loads a
view into the raw zone. Business column *names* come from the view config
(allow-listed upstream); every run-varying *value* is a bound parameter or a
server-side function, so nothing is string-formatted into SQL.
"""
from __future__ import annotations

# The audit columns added in the RAW zone, in the order they are written.
# Kept here as the single source of truth so DDL and loads never drift apart.
RAW_AUDIT_COLUMNS = [
    "_source_system",   # STRING    - origin system, e.g. "oracle_sys"
    "_source_object",   # STRING    - source view name, e.g. "v_trades"
    "_run_id",          # STRING    - pipeline run / correlation id that wrote it
    "_loaded_at",       # TIMESTAMP - when the row landed in raw (UTC)
    "_loaded_by",       # STRING    - identity that wrote it (service account)
    "_record_hash",     # STRING    - hash of the business columns (change detection)
]


def record_hash_expr(business_columns: list[str]) -> str:
    """Return a deterministic, null-safe row-hash expression over the business
    columns.

    TO_JSON_STRING(STRUCT(...)) gives a stable, null-safe serialization (field
    order fixed by the STRUCT, NULLs represented explicitly), and MD5 -> hex turns
    it into a compact fingerprint. Two rows with identical business values get the
    same hash, so an unchanged row can be skipped by SCD2 / change detection.
    """
    cols = ", ".join(business_columns)
    return f"TO_HEX(MD5(TO_JSON_STRING(STRUCT({cols}))))"


def audit_select_fragment(business_columns: list[str]) -> str:
    """Return the SELECT list (business columns + audit columns) for a raw load.

    Bound parameters expected at execution: @src_system, @src_object, @run_id.
    Server-side functions supply the load time and the writer identity so they
    come from one trusted clock/identity, not the client.
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
    """Full SELECT that reads the just-extracted load table and adds audit columns.

    ``load_table_fqn`` is the fully-qualified staged/load table for this run; the
    result is used as the USING source of the MERGE into the raw table.
    """
    return (
        "SELECT\n"
        f"{audit_select_fragment(business_columns)}\n"
        f"FROM `{load_table_fqn}`"
    )
