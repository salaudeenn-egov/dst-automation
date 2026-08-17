"""Per-tenant run lock and campaign-run audit trail.

Both tables live in the Airflow metadata database, created idempotently on
first use. Airflow 3 forbids task code from using Airflow's ORM session, so
this module connects with its OWN SQLAlchemy engine using DST_AUDIT_DB_URL
(falling back to AIRFLOW__DATABASE__SQL_ALCHEMY_CONN, which the Docker
deployment already injects into every container). Timestamps are stored as
ISO-8601 UTC strings so the SQL is portable across Postgres and SQLite.

Lock semantics (mirrors the old scheduler's non-blocking threading.Lock):
  - claim_tenant_lock returns False when another run holds the tenant — the
    caller treats that as a routine no-op, never a failure.
  - Locks older than the TTL are considered orphaned (pod killed before
    finalize could release) and are stolen by the next claim.
  - release is scoped to (tenant, dag_run_id): a stray release from the wrong
    run can never clear another run's active lock.
"""
import logging
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError

log = logging.getLogger(__name__)

LOCK_TTL_MINUTES = int(os.getenv("DST_LOCK_TTL_MINUTES", "45"))

_CREATE_LOCKS = """
CREATE TABLE IF NOT EXISTS dst_tenant_locks (
    tenant     VARCHAR(64)  PRIMARY KEY,
    dag_run_id VARCHAR(250) NOT NULL,
    locked_at  VARCHAR(32)  NOT NULL
)"""

_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS dst_campaign_runs (
    dag_run_id VARCHAR(250) PRIMARY KEY,
    tenant     VARCHAR(64)  NOT NULL,
    state_name VARCHAR(128),
    mode       VARCHAR(16),
    slot_date  VARCHAR(10),
    slot_time  VARCHAR(5),
    status     VARCHAR(16)  NOT NULL,
    error      VARCHAR(500),
    drive_link VARCHAR(500),
    created_at VARCHAR(32)  NOT NULL
)"""


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


_engine_cache = None


def _resolve_db_url():
    """DST_AUDIT_DB_URL env, else the Airflow conn env, else the
    dst_audit_db_url Airflow Variable. The Airflow 3 task runner overwrites
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN with a sentinel inside tasks
    ("airflow-db-not-allowed:///") — treat that as absent."""
    for candidate in (os.getenv("DST_AUDIT_DB_URL"),
                      os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")):
        if candidate and not candidate.startswith("airflow-db-not-allowed"):
            return candidate
    from common.deployment_env import _get_airflow_variable
    return _get_airflow_variable("dst_audit_db_url")


def _engine():
    global _engine_cache
    if _engine_cache is None:
        url = _resolve_db_url()
        if not url:
            raise RuntimeError(
                "Tenant locks/audit need a metadata DB URL: set the "
                "dst_audit_db_url Airflow Variable or DST_AUDIT_DB_URL env")
        from sqlalchemy import create_engine
        _engine_cache = create_engine(url, pool_pre_ping=True)
    return _engine_cache


def _session():
    return _engine().begin()


def _ensure_tables():
    with _session() as session:
        session.execute(text(_CREATE_LOCKS))
        session.execute(text(_CREATE_RUNS))


def claim_tenant_lock(tenant, dag_run_id, ttl_minutes=LOCK_TTL_MINUTES):
    """Try to claim the tenant. Returns True on success, False when another
    live run holds it. Steals locks older than the TTL first."""
    _ensure_tables()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=ttl_minutes)).isoformat()

    with _session() as session:
        stale = session.execute(
            text("DELETE FROM dst_tenant_locks WHERE tenant = :t AND locked_at < :cutoff"),
            {"t": tenant, "cutoff": cutoff})
        if stale.rowcount:
            log.warning(f"[lock] stole stale lock for tenant={tenant} "
                        f"(older than {ttl_minutes} min — previous run died before finalize)")

    try:
        with _session() as session:
            session.execute(
                text("INSERT INTO dst_tenant_locks (tenant, dag_run_id, locked_at) "
                     "VALUES (:t, :r, :now)"),
                {"t": tenant, "r": dag_run_id, "now": _now_iso()})
        log.info(f"[lock] claimed tenant={tenant} run={dag_run_id}")
        return True
    except IntegrityError:
        log.info(f"[lock] tenant={tenant} already locked — routine no-op")
        return False


def release_tenant_lock(tenant, dag_run_id):
    """Release only this run's lock (scoped delete — safe against stray calls)."""
    with _session() as session:
        result = session.execute(
            text("DELETE FROM dst_tenant_locks WHERE tenant = :t AND dag_run_id = :r"),
            {"t": tenant, "r": dag_run_id})
        if result.rowcount:
            log.info(f"[lock] released tenant={tenant}")


def record_campaign_run(dag_run_id, tenant, state_name, mode,
                        slot_date, slot_time, status, error="", drive_link=""):
    """Write one audit row per real report attempt (routine no-ops are skipped
    by the caller). Idempotent on retry: delete-then-insert by dag_run_id."""
    _ensure_tables()
    with _session() as session:
        session.execute(text("DELETE FROM dst_campaign_runs WHERE dag_run_id = :r"),
                        {"r": dag_run_id})
        session.execute(
            text("INSERT INTO dst_campaign_runs "
                 "(dag_run_id, tenant, state_name, mode, slot_date, slot_time,"
                 " status, error, drive_link, created_at) "
                 "VALUES (:r, :t, :s, :m, :sd, :st, :status, :e, :d, :now)"),
            {"r": dag_run_id, "t": tenant, "s": state_name, "m": mode,
             "sd": slot_date, "st": slot_time, "status": status,
             "e": str(error)[:500], "d": drive_link[:500], "now": _now_iso()})
    log.info(f"[audit] {tenant} {slot_date} {slot_time} -> {status}")


def has_successful_run_since(tenant, mode, slot_dt):
    """Retime guard for the scheduler: True when this campaign already produced
    a successful report today at or after slot_dt — so a slot retimed into the
    past must not fire again. A "both" run covers internal and partner slots.
    Failed runs do NOT count: a retimed slot may replace a failed report."""
    _ensure_tables()
    modes = (mode, "both") if mode != "both" else ("both",)
    with _session() as session:
        query = text(
            "SELECT 1 FROM dst_campaign_runs "
            "WHERE tenant = :t AND slot_date = :d AND status = 'success' "
            "AND mode IN :modes AND slot_time >= :time LIMIT 1"
        ).bindparams(bindparam("modes", expanding=True))
        found = session.execute(
            query,
            {"t": tenant, "d": slot_dt.date().isoformat(),
             "modes": list(modes), "time": slot_dt.strftime("%H:%M")}).first()
    return found is not None
