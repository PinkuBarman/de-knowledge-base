"""Verify both control modules run end-to-end (against a mock BigQuery client)
and that every SQL statement they emit — plus both DDL files — is valid BigQuery
SQL. No live warehouse is required."""
import sys, types, datetime as dt
sys.path.insert(0, "simple")
sys.path.insert(0, "enterprise")

import sqlglot
from google.cloud import bigquery

DT = dt.datetime(2026, 1, 3, 0, 0, tzinfo=dt.timezone.utc)
collected_sql = []   # every SQL string executed via .query()


class FakeJob:
    def __init__(self, rows): self._rows = rows
    def result(self): return self._rows


class FakeClient:
    """Records queries/inserts; returns canned rows for the watermark read."""
    def __init__(self): self.read_rows = []; self.inserts = []
    def query(self, sql, job_config=None):
        collected_sql.append(sql)
        # sanity: every value must be a bound parameter, never absent
        assert job_config is not None and job_config.query_parameters is not None
        if sql.strip().startswith("SELECT watermark_value"):
            return FakeJob(list(self.read_rows))
        return FakeJob([])
    def insert_rows_json(self, table, rows):
        self.inserts.append((table, rows)); return []   # [] == no errors


def check_sql_valid(label):
    for s in collected_sql:
        sqlglot.parse(s, read="bigquery")   # raises on invalid SQL
    print(f"  {label}: {len(collected_sql)} SQL statement(s) parsed OK")
    collected_sql.clear()


# --------------------------------------------------------------------------
print("== ENTERPRISE module ==")
import control_enterprise as ent

c = FakeClient()
PROJECT = "org-edp-dev"

# 1. bootstrap read -> None
c.read_rows = []
assert ent.read_watermark(c, PROJECT, "v_trades") is None
print("  read_watermark bootstrap -> None: OK")

# 2. bootstrap upsert (no existing row)
ent.upsert_watermark(c, PROJECT, "v_trades", "last_updated_date", DT, "run-001")
print("  upsert_watermark bootstrap: OK")

# 3. regression is blocked
c.read_rows = [types.SimpleNamespace(watermark_value=DT)]
try:
    ent.upsert_watermark(c, PROJECT, "v_trades", "last_updated_date",
                         DT - dt.timedelta(days=1), "run-002")
    raise AssertionError("regression should have raised")
except ValueError:
    print("  watermark regression blocked: OK")

# 4. deliberate regression allowed with the override
ent.upsert_watermark(c, PROJECT, "v_trades", "last_updated_date",
                     DT - dt.timedelta(days=1), "run-003", allow_regression=True)
print("  watermark regression with override: OK")

# 5. run log SUCCESS and FAILED
ent.log_run(c, PROJECT, "edp_oracle_sys_elt_daily", "run-001", "SUCCESS",
            run_start_ts=DT, data_as_of_ts=DT, failed_tasks=[], dbt_image_digest="sha256:abc")
ent.log_run(c, PROJECT, "edp_oracle_sys_elt_daily", "run-004", "FAILED",
            run_start_ts=DT, data_as_of_ts=None, failed_tasks=["extract_load_v_trades"],
            dbt_image_digest=None)
print("  log_run SUCCESS + FAILED (NULL data_as_of + ARRAY param): OK")

# 6. check + refresh
ent.record_check(c, PROJECT, "run-001", "edp_oracle_sys_elt_daily", "raw",
                 "v_trades", "rowcount_matches", "error", True, "998 vs 998")
ent.record_refresh(c, PROJECT, "run-001", "ds-42", "job-9", "SUCCESS", DT, None)
print("  record_check + record_refresh: OK")

# idempotency guard is present in the emitted DML
assert any("WHEN NOT MATCHED THEN INSERT" in s for s in collected_sql)
check_sql_valid("enterprise emitted SQL")

# --------------------------------------------------------------------------
print("== SIMPLE module ==")
import control_simple as smp

c2 = FakeClient()
c2.read_rows = []
assert smp.read_watermark(c2, PROJECT, "v_trades") is None
smp.write_watermark(c2, PROJECT, "v_trades", "last_updated_date", DT, "run-001")
print("  read_watermark + write_watermark: OK")
smp.log_run(c2, PROJECT, "edp_oracle_sys_elt_daily", "run-001", "SUCCESS",
            run_start_ts=DT, data_as_of_ts=DT, failed_tasks=[], dbt_image_digest="sha256:abc")
smp.record_check(c2, PROJECT, "run-001", "edp_oracle_sys_elt_daily",
                 "v_trades", "rowcount_matches", "error", True, "998 vs 998")
assert len(c2.inserts) == 2   # run log + check both written
print("  log_run + record_check (streaming inserts): OK")
check_sql_valid("simple emitted SQL")

# --------------------------------------------------------------------------
print("== DDL files ==")
for path in ("simple/ddl_simple.sql", "enterprise/ddl_enterprise.sql"):
    sql = open(path).read()
    stmts = [s for s in sqlglot.parse(sql, read="bigquery") if s is not None]
    print(f"  {path}: {len(stmts)} statement(s) parsed OK")

print("\nALL CHECKS PASSED")
