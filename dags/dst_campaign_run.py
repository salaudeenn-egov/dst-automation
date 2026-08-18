"""dst_campaign_run — executes one campaign report end to end. Trigger-only.

Triggered by dst_campaign_scheduler with the campaign's sheet row in
dag_run.conf, so this DAG never reads config itself. The whole pipeline chain
(analyze -> cdd_sync -> report -> notify) runs inside ONE task: intermediate
files stay on that task's local disk (pod-local on Kubernetes), and every
durable artifact — reports, Excels, checkpoints — is published to Google Drive
before the task ends. Checkpoints upload even on failure, so a dead run stays
debuggable from any machine (see pipeline/README.md).

No database anywhere, in any mode. No run lock either: duplicate fires of the
same slot are impossible via deterministic trigger run-ids, and the rare
overlap of two different slots for one tenant is accepted — the same trade
the platform's production report system makes.

Run history follows the universal DST_MODE flag:
  sheet mode — one row on the sheet's Run Log tab
  mdms mode  — one Kafka lifecycle event (platform persister owns the DB write)

Task chain:
  execute_campaign_pipeline  the pipeline chain; data errors fail fast with no
                             retry, infrastructure errors retry
  finalize_run               ALL_DONE: records the outcome, then re-raises on
                             failure so the DAG run itself is marked failed
"""
import logging
from datetime import datetime, timedelta, timezone

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from common.alerts import notify_slack_on_failure
from common.campaign_runner import execute_campaign
from common.deployment_env import group_environment, resolve_dst_mode
from common.dst_kafka_status import push_run_event

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

    @task(retries=2, retry_delay=timedelta(minutes=3),
          execution_timeout=timedelta(minutes=60))
    def execute_campaign_pipeline(dag_run=None):
        """Run the whole pipeline chain for this campaign row (see
        common/campaign_runner.py for stage and error semantics)."""
        conf = dag_run.conf or {}
        if not conf.get("row"):
            raise ValueError("dag_run.conf is missing 'row' — this DAG must be "
                             "triggered by dst_campaign_scheduler, or manually "
                             "with a full conf payload")
        with group_environment(conf.get("group") or {"name": "default", "env": {}}):
            return execute_campaign(conf["row"], conf.get("mode", "both"))

    @task(trigger_rule="all_done")
    def finalize_run(dag_run=None, ti=None):
        """Always runs. Records every REAL report attempt on the channel the
        universal DST_MODE flag selects (Run Log tab in sheet mode, Kafka event
        in mdms mode). Routine no-ops record nothing. A raw None from
        xcom_pull means the execute task genuinely failed — recorded, then
        re-raised so the DAG run is marked failed (finalize is the leaf task;
        swallowing the failure would blind max_consecutive_failed_dag_runs)."""
        from pipeline.run_log import append_run_log

        conf = dag_run.conf or {}
        row = conf.get("row") or {}
        group = conf.get("group") or {"name": "default", "env": {}}
        mode = conf.get("mode", "both")
        dst_mode = resolve_dst_mode()

        marker = ti.xcom_pull(task_ids=EXECUTE_TASK_ID)
        if marker is not None and marker.get("ok") is None:
            log.info(f"routine no-op ({marker.get('reason')}) — nothing recorded")
            return

        failed = marker is None
        degraded = (not failed and any(str(v).startswith("degraded")
                                       for v in marker.get("stages", {}).values()))
        error = ("execute_campaign_pipeline failed — see task log" if failed
                 else "cdd_sync degraded" if degraded else "")
        drive_link = "" if failed else marker.get("drive_link", "")
        day = "" if failed else marker.get("day", "")

        if dst_mode == "mdms":
            push_run_event("REPORT_FAILED" if failed else "REPORT_COMPLETED",
                           conf, dag_run.run_id, error_message=error or None,
                           drive_link=drive_link, day=day)
        else:
            with group_environment(group):
                append_run_log(conf.get("state_name", ""),
                               row.get("campaign_name", ""), day,
                               "FAILED" if failed else "SUCCESS",
                               step_failed=EXECUTE_TASK_ID if failed else "",
                               error=error, drive_link=drive_link, mode=mode)

        if failed:
            raise RuntimeError("execute_campaign_pipeline failed — outcome "
                               "recorded; re-raising so the DAG run is marked failed")

    execute_campaign_pipeline() >> finalize_run()


dst_campaign_run()
