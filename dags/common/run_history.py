"""Sheet-backed retime guard for sheet mode — no database anywhere.

There is deliberately NO run lock in this system: duplicate fires of the same
slot are impossible via deterministic trigger run-ids, and the rare overlap of
two different slots for one tenant is accepted (worst case: one duplicate
Slack post), the same trade the platform's production report system makes.

The guard must be built inside group_environment (it reads the group's sheet).
mdms mode skips the guard entirely (zero sheet/DB access on the scheduling
path there).
"""
import logging

from pipeline.run_log import append_run_log, fetch_today_runs

log = logging.getLogger(__name__)


def build_retime_guard():
    """One sheet read, then a pure closure for find_due_slots.

    has_report_since(tenant, mode, slot_dt) -> True when a SUCCESS row for this
    tenant already exists today for a slot at or after this one, with a
    covering mode ("both" covers internal and partner). FAILED runs do not
    count, so a retimed slot may replace a failed report.
    """
    today_runs = fetch_today_runs()
    log.info(f"[retime-guard] {len(today_runs)} run(s) recorded today")

    def has_report_since(tenant, mode, slot_dt, state_name=""):
        """Keyed on TENANT, which is what find_due_slots passes. It previously
        compared against the Run Log's State column while the caller passed the
        lowercase tenant, so the guard never matched anything and a retimed
        slot re-fired. Compares the recorded SLOT time, not the write time."""
        covering = {mode, "both"}
        slot_hhmm = slot_dt.strftime("%H:%M")
        def same_campaign(r):
            # rows written before the Tenant column existed (and every row
            # run.py writes) carry only State — fall back to it rather than
            # treating them as belonging to no campaign at all
            return r["tenant"] == tenant or (not r["tenant"]
                                             and state_name
                                             and r["state"] == state_name)
        return any(same_campaign(r) and r["status"] == "SUCCESS"
                   and r["mode"] in covering and r["slot_time"] >= slot_hhmm
                   for r in today_runs)

    return has_report_since


def record_outcome(conf, dag_run_id, marker, use_mdms, group_environment):
    """Record one run's outcome on the channel the deployment flag selects.

    Kept out of the DAG file so it can be exercised without an Airflow
    scheduler. Returns a summary of what was written:
        {"recorded": "kafka"|"sheet"|"none", "failed": bool,
         "step_failed": str, "drive_folder_url": str}

    A marker of None means the execute task genuinely failed. Sheet mode writes
    the Run Log tab; mdms mode publishes to Kafka and FALLS BACK to the Run Log
    tab if the publish does not land, so a broker outage cannot silently erase
    the audit trail.
    """
    from common.dst_kafka_status import push_run_event

    row = conf.get("row") or {}
    group = conf.get("group") or {"name": "default", "env": {}}
    mode = conf.get("mode", "both")

    failed = marker is None
    stages = {} if failed else marker.get("stages", {})
    degraded = any(str(v).startswith("degraded") for v in stages.values())
    step_failed = next((name for name, outcome in stages.items()
                        if str(outcome).startswith("failed")), "")
    if failed and not step_failed:
        step_failed = "execute_campaign_pipeline"
    error = ("execute_campaign_pipeline failed — see task log" if failed
             else "cdd_sync degraded" if degraded else "")
    drive_folder_url = "" if failed else marker.get("drive_folder_url", "")
    drive_link = "" if failed else marker.get("drive_link", "")
    day = "" if failed else marker.get("day", "")

    published = False
    if use_mdms:
        published = push_run_event(
            "REPORT_FAILED" if failed else "REPORT_COMPLETED",
            conf, dag_run_id, step_failed=step_failed,
            drive_folder_url=drive_folder_url, day=day)
        if not published:
            log.warning("[finalize] Kafka publish did not land — falling back "
                        "to the Run Log tab so the outcome is not lost")

    recorded = "kafka" if published else "none"
    if not published:
        with group_environment(group):
            ok = append_run_log(
                conf.get("state_name", ""), row.get("campaign_name", ""), day,
                "FAILED" if failed else "SUCCESS",
                step_failed=step_failed, error=error,
                drive_link=drive_folder_url or drive_link, mode=mode,
                tenant=conf.get("tenant", ""),
                cycle_index=row.get("cycle_index", ""),
                slot_date=conf.get("slot_date", ""),
                slot_time=conf.get("slot_time", ""),
                dag_run_id=dag_run_id)
        recorded = "sheet" if ok else "none"

    return {"recorded": recorded, "failed": failed,
            "step_failed": step_failed, "drive_folder_url": drive_folder_url}
