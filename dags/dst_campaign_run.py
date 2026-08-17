"""dst_campaign_run — executes one campaign report end to end. Trigger-only.

Triggered by dst_campaign_scheduler with the campaign's sheet row in
dag_run.conf, so this DAG never reads the Google Sheet itself. The whole
pipeline chain (analyze -> cdd_sync -> report -> notify) runs inside ONE task:
intermediate files stay on that task's local disk (pod-local on Kubernetes),
and every durable artifact — reports, Excels, checkpoints — is published to
Google Drive before the task ends. Checkpoints upload even on failure, so a
dead run stays debuggable from any machine (see pipeline/README.md).

Task chain:
  claim_tenant_lock          one live run per tenant; a held lock is a routine
                             no-op (marker ok=None), never a failure
  execute_campaign_pipeline  the pipeline chain; data errors fail fast with no
                             retry, infrastructure errors retry
  finalize_run               ALL_DONE: always releases the lock, writes the
                             audit row (skipped for routine no-ops)
"""
import logging
from datetime import datetime, timedelta, timezone

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from common.alerts import notify_slack_on_failure
from common.campaign_runner import execute_campaign
from common.deployment_env import group_environment
from common.tenant_lock import (claim_tenant_lock, record_campaign_run,
                                release_tenant_lock)

log = logging.getLogger(__name__)

EXECUTE_TASK_ID = "execute_campaign_pipeline"


@dag(
    dag_id="dst_campaign_run",
    description="Runs one campaign report: ES extract, Excels, Word docs, Drive upload, Slack post",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=16,
    max_consecutive_failed_dag_runs=3,
    dagrun_timeout=timedelta(minutes=90),
    tags=["dst", "reporting"],
    default_args={"on_failure_callback": notify_slack_on_failure},
    doc_md=__doc__,
)
def dst_campaign_run():

    @task
    def claim_tenant_lock_for_run(dag_run=None):
        """Claim the per-tenant lock so two runs of one tenant can never
        overlap (a delayed run vs the next slot, or a retimed catch-up).
        Losing the lock is a routine no-op, not a failure."""
        conf = dag_run.conf or {}
        tenant = conf.get("tenant", "")
        if not tenant:
            raise ValueError("dag_run.conf is missing 'tenant' — this DAG must be "
                             "triggered by dst_campaign_scheduler, not manually "
                             "without conf")
        if claim_tenant_lock(tenant, dag_run.run_id):
            return {"ok": True, "tenant": tenant}
        return {"ok": None, "tenant": tenant,
                "reason": "another run already in progress for this tenant"}

    @task(retries=2, retry_delay=timedelta(minutes=3),
          execution_timeout=timedelta(minutes=60))
    def execute_campaign_pipeline(lock, dag_run=None):
        """Run the whole pipeline chain for this campaign row (see
        common/campaign_runner.py for stage and error semantics)."""
        if lock["ok"] is None:
            log.info(f"skipping: {lock['reason']}")
            return {"ok": None, "reason": lock["reason"]}

        conf = dag_run.conf or {}
        with group_environment(conf.get("group") or {"name": "default", "env": {}}):
            return execute_campaign(conf["row"], conf.get("mode", "both"))

    @task(trigger_rule="all_done")
    def finalize_run(lock, dag_run=None, ti=None):
        """Always runs: release the tenant lock, then record every REAL report
        attempt twice — an audit row in Postgres (source of truth, drives the
        retime guard) and a row in the sheet's Run Log tab (the human-visible
        history, same format run.py writes). Routine no-ops record nothing.
        A raw None from xcom_pull means the execute task genuinely failed."""
        from pipeline.run_log import append_run_log

        conf = dag_run.conf or {}
        tenant = conf.get("tenant", "?")
        row = conf.get("row") or {}
        group = conf.get("group") or {"name": "default", "env": {}}

        def _record(status, step_failed="", error="", drive_link="", day=""):
            record_campaign_run(
                dag_run.run_id, tenant, conf.get("state_name", ""),
                conf.get("mode", ""), conf.get("slot_date", ""),
                conf.get("slot_time", ""), status=status,
                error=error, drive_link=drive_link)
            with group_environment(group):
                append_run_log(conf.get("state_name", ""),
                               row.get("campaign_name", ""), day,
                               status.upper(), step_failed=step_failed,
                               error=error, drive_link=drive_link)

        try:
            marker = ti.xcom_pull(task_ids=EXECUTE_TASK_ID)
            if lock.get("ok") is None:
                log.info("routine no-op (lock held elsewhere) — nothing recorded")
            elif marker is None:
                _record("failed", step_failed=EXECUTE_TASK_ID,
                        error="execute_campaign_pipeline failed — see task log")
            elif marker.get("ok") is None:
                log.info(f"routine no-op ({marker.get('reason')}) — nothing recorded")
            else:
                degraded = any(str(v).startswith("degraded")
                               for v in marker.get("stages", {}).values())
                _record("success",
                        error="cdd_sync degraded" if degraded else "",
                        drive_link=marker.get("drive_link", ""),
                        day=marker.get("day", ""))
        finally:
            if lock.get("ok"):
                release_tenant_lock(tenant, dag_run.run_id)

    lock = claim_tenant_lock_for_run()
    pipeline_result = execute_campaign_pipeline(lock)
    pipeline_result >> finalize_run(lock)


dst_campaign_run()
