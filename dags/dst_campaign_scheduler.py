"""dst_campaign_scheduler — decides, every 5 minutes, which campaign reports are due.

Stateless by design: each tick re-reads the Google Sheet, so a sheet edit is
live within 5 minutes and there is no cached schedule to invalidate. The sheet
is only ever read INSIDE tasks — never at DAG-parse time (the dag-processor
re-executes this file's top level every ~30 seconds).

Tick flow:
  list_deployment_groups     Airflow Variable dst_groups (or env default)
  find_due_campaigns (xN)    read the group's sheet tab, match slots against
                             the wall-clock lookback window, apply the
                             retime guard
  collect_due_campaigns      merge every group's due list
  trigger_campaign_run (xM)  one dst_campaign_run per due slot, campaign row
                             in conf, deterministic run id (duplicate slot
                             fires are skipped, never doubled)
"""
import logging
import os
from datetime import datetime, timedelta, timezone

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task
try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common.alerts import notify_slack_on_failure
from common.deployment_env import group_environment, load_deployment_groups
from common.slots import find_due_slots
from common.tenant_lock import has_successful_run_since

log = logging.getLogger(__name__)

LOOKBACK_MINUTES = int(os.getenv("DST_LOOKBACK_MINUTES", "60"))


@dag(
    dag_id="dst_campaign_scheduler",
    description="Reads the campaign config sheet every 5 minutes and triggers due report runs",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["dst", "reporting"],
    default_args={"retries": 1,
                  "retry_delay": timedelta(minutes=1),
                  "on_failure_callback": notify_slack_on_failure},
    doc_md=__doc__,
)
def dst_campaign_scheduler():

    @task
    def list_deployment_groups():
        groups = load_deployment_groups()
        log.info(f"deployment groups: {[g['name'] for g in groups]}")
        return groups

    @task(execution_timeout=timedelta(minutes=4))
    def find_due_campaigns(group):
        """Read one group's sheet tab and return the slots due right now.

        Wall-clock window (now - lookback, now]: Airflow 3's CronTriggerTimetable
        has a zero-width data interval, so the tick's own timestamps are useless
        for windowing. The lookback also gives downtime catch-up.
        """
        from pipeline import config

        with group_environment(group):
            rows = config.get_active_rows()

        now = datetime.now(timezone.utc)
        due = find_due_slots(group, rows, now, LOOKBACK_MINUTES,
                             has_report_since=has_successful_run_since)
        log.info(f"[{group['name']}] {len(rows)} rows -> {len(due)} due slot(s) "
                 f"in the last {LOOKBACK_MINUTES} min")
        for item in due:
            log.info(f"  due: {item['trigger_run_id']}")
        return due

    @task
    def collect_due_campaigns(per_group_due):
        return [item for group_due in per_group_due for item in group_due]

    groups = list_deployment_groups()
    due_per_group = find_due_campaigns.expand(group=groups)
    all_due = collect_due_campaigns(due_per_group)

    TriggerDagRunOperator.partial(
        task_id="trigger_campaign_run",
        trigger_dag_id="dst_campaign_run",
        skip_when_already_exists=True,
        reset_dag_run=False,
    ).expand_kwargs(all_due)


dst_campaign_scheduler()
