"""
db.py — lightweight psycopg2 helpers for per-tenant mutual exclusion and
run-audit history, using the SAME Postgres instance that backs Airflow's
own metadata DB (not a new secret — matches the compose file's existing
postgresql+psycopg2://airflow:airflow@postgres/airflow, a trusted internal
service). Confirmed reachable directly from task-execution containers.

Two tables (created once via a manual `CREATE TABLE IF NOT EXISTS`, not
managed by this code — see the plan's Build Order step 1):
    dst_tenant_locks(tenant PK, locked_at, dag_run_id)
    dst_report_metadata(id, tenant, state_name, dag_run_id, mode, day,
                         status, step_failed, error, drive_link, created_time)

Why Postgres locks instead of Airflow Pools: dst_dynamic_campaigns is now a
single static DAG serving every tenant via dynamic dag_run.conf, so a
static per-task `pool=f"dst_pool_{tenant}"` (fixed at DAG-definition time)
can no longer express "only one tenant X run at a time" — which tenant is
running is only known at trigger time. This replaces scheduler.py's
original per-tenant threading.Lock with the same non-blocking-acquire
semantics, just across DAG runs instead of threads.
"""
import logging

import psycopg2

log = logging.getLogger(__name__)

_DSN = "host=postgres dbname=airflow user=airflow password=airflow"


def _connect():
    conn = psycopg2.connect(_DSN)
    conn.autocommit = True
    return conn


def acquire_tenant_lock(tenant: str, dag_run_id: str) -> bool:
    """Returns True if this run won the lock (no other run for this
    tenant is in progress), False if another run already holds it."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dst_tenant_locks (tenant, dag_run_id) "
            "VALUES (%s, %s) ON CONFLICT (tenant) DO NOTHING",
            (tenant, dag_run_id),
        )
        return cur.rowcount == 1


def release_tenant_lock(tenant: str, dag_run_id: str):
    """Only releases the lock if it's still held by THIS run's dag_run_id
    — a stray/duplicate release call from a different run must not clear
    another run's active lock."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM dst_tenant_locks WHERE tenant = %s AND dag_run_id = %s",
            (tenant, dag_run_id),
        )


def write_audit_row(*, tenant, state_name, dag_run_id, mode, status,
                     day=None, step_failed=None, error=None, drive_link=None):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dst_report_metadata "
            "(tenant, state_name, dag_run_id, mode, day, status, step_failed, error, drive_link) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (tenant, state_name, dag_run_id, mode, day, status, step_failed, error, drive_link),
        )
