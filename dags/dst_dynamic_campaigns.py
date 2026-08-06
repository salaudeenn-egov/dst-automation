"""
dst_dynamic_campaigns.py — Processor DAG.

Trigger-only (schedule=None): dst_campaign_scheduler.py dynamically
triggers one run of this DAG per due campaign trigger-slot, passing
{"group": ..., "row": ..., "mode": ...} as dag_run.conf. Every task in the
chain reads that conf directly (see common/tasks.py's module docstring for
why) rather than having group/row/mode threaded through as XCom arguments.

Replaces the earlier per-(tenant,time,mode) DAG-factory design (one DAG per
trigger slot) with a single static DAG definition, mirroring the reference
architecture doc's "Scheduler DAG decides what's due, Processor DAG does
the work" split — see D:\\DST\\.claude\\plans (or ask Salaudeen) for the
migration rationale.
"""
from datetime import datetime, timedelta

from airflow import DAG

from common.tasks import (
    check_active,
    run_analyze,
    run_cdd_sync,
    run_report,
    run_notify,
    finalize,
    dst_failure_slack_alert,
)

_OWNER = "dst-automation"

default_args = {
    "owner": _OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="dst_dynamic_campaigns",
    description="Processor DAG — runs the check_active -> analyze -> cdd_sync -> report -> notify chain for one due campaign trigger-slot",
    doc_md=(
        "### DST Processor DAG\n"
        "Triggered dynamically by `dst_campaign_scheduler` — never runs on "
        "its own schedule. Each run's `dag_run.conf` carries "
        "`{group, row, mode}` for exactly one campaign trigger-slot "
        "(mode = `both`/`internal`/`partner`, matching "
        "`pipeline/schedule_utils.compute_trigger_slots`).\n\n"
        "Per-tenant mutual exclusion is enforced via a Postgres row lock "
        "(`dst_tenant_locks`, see `common/db.py`) rather than an Airflow "
        "Pool, since this single DAG serves every tenant dynamically — a "
        "static per-task `pool=` can't vary by which tenant a given run's "
        "conf names.\n\n"
        "`finalize` always runs last: releases the tenant lock, writes a "
        "`dst_report_metadata` audit row for every real (non-no-op) "
        "attempt, and re-raises to surface a genuine cdd_sync failure as a "
        "visible DAG failure + Slack alert even though run_report/"
        "run_notify tolerate it and complete anyway."
    ),
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=16,
    tags=["dst", "processor"],
    on_failure_callback=dst_failure_slack_alert,
) as dag:
    active_ctx = check_active()
    analyze_result = run_analyze(active_ctx)
    cdd_result = run_cdd_sync(active_ctx, analyze_result)
    report_result = run_report(active_ctx)
    notify_result = run_notify(active_ctx, report_result)
    done = finalize()

    # run_report's trigger_rule=ALL_DONE means it isn't gated on cdd_result
    # via a normal argument-passing edge (see common/tasks.py) — this is
    # what actually makes it wait for cdd_sync to finish (success or fail)
    # before starting.
    cdd_result >> report_result
    # finalize (trigger_rule=ALL_DONE) inspects run_report/run_cdd_sync/
    # run_notify's outcomes via ti.xcom_pull — explicit edges from all
    # three (not just relying on notify's transitive dependency on report)
    # make the "wait for everything to be done first" requirement obvious
    # rather than implicit.
    report_result >> done
    cdd_result >> done
    notify_result >> done
