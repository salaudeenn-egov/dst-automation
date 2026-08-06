"""
dst_campaign_scheduler.py — Scheduler DAG.

Runs every 5 minutes. One task per active deployment group (never a loop
over groups inside one task — see common/env.py's tenant_env docstring for
why that matters for credential isolation) reads that group's Google Sheet
tab and finds every (tenant, time, mode) trigger-slot whose scheduled time
falls within the current 5-minute tick window. All groups' matches are
flattened into one list and used to dynamically trigger dst_dynamic_campaigns
— one run per due slot, via dynamic task mapping over TriggerDagRunOperator
(no fixed number of triggers is known at DAG-parse time, since it depends
on how many campaigns are due this tick).

Mirrors the reference architecture doc's "Scheduler DAG decides what's due,
Processor DAG does the work" split, adapted for exact HH:MM trigger times
(pipeline/schedule_utils.compute_trigger_slots) rather than their hourly
triggerTime-window matching.
"""
from datetime import datetime, timedelta, time as dtime, timezone as tz

from airflow import DAG
from airflow.sdk import task, Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from common.env import tenant_env, ACTIVE_VALUES

_OWNER = "dst-automation"


def _active_groups():
    try:
        return Variable.get("dst_active_groups", deserialize_json=True)
    except Exception:
        return []


def _slug(value: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value).strip()).strip("_") or "unknown"


with DAG(
    dag_id="dst_campaign_scheduler",
    description="Finds campaigns due for a report in the current 5-minute window and triggers dst_dynamic_campaigns for each",
    doc_md=(
        "### DST Scheduler DAG\n"
        "One task per group in the `dst_active_groups` Variable — each "
        "reads its own Google Sheet tab (via `dst_secrets_<group>`) and "
        "matches active rows' `report_times`/`partner_report_times` "
        "against the current `[data_interval_start, data_interval_end)` "
        "window. Matches are flattened and used to dynamically trigger "
        "one `dst_dynamic_campaigns` run per due `(tenant, time, mode)` "
        "slot — the actual work (analyze/cdd_sync/report/notify) lives "
        "entirely in that Processor DAG, not here."
    ),
    default_args={"owner": _OWNER, "retries": 1, "retry_delay": timedelta(minutes=1)},
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=4),
    tags=["dst", "scheduler"],
) as dag:

    @task(execution_timeout=timedelta(minutes=2))
    def find_due(group: str):
        """Returns a list of {"conf": {...}, "trigger_run_id": ...} dicts
        — one per (tenant, time, mode) slot due in this tick's window.

        Uses wall-clock now() for the window (matching the reference
        doc's own "window_start = now_utc - 1 hour + 1 minute" approach),
        not dag_run.data_interval_start/end — a plain cron-string
        `schedule` on Airflow 3 defaults to CronTriggerTimetable, which
        gives a zero-width data_interval (start == end == trigger time),
        not the [previous_tick, this_tick) interval Airflow 2's
        CronDataIntervalTimetable produced. Confirmed by direct query
        against a real scheduled run before switching to this approach."""
        window_end = datetime.now(tz.utc)
        window_start = window_end - timedelta(minutes=5)

        with tenant_env(group):
            from pipeline import config
            from pipeline.schedule_utils import compute_trigger_slots
            rows = config.get_active_rows()

        active_rows = [r for r in rows if str(r.get("active", "")).strip().upper() in ACTIVE_VALUES]

        due = []
        for row in active_rows:
            tenant = row.get("tenant", "?")
            for time_str, mode in compute_trigger_slots(row):
                try:
                    hh, mm = (int(p) for p in time_str.strip().split(":"))
                except ValueError:
                    continue
                # Check both the window-start and window-end dates so a
                # slot time right at a midnight-UTC boundary still matches
                # correctly (the window is only 5 minutes wide, so at most
                # one of the two dates is ever actually relevant).
                for base_date in {window_start.date(), window_end.date()}:
                    candidate = datetime.combine(base_date, dtime(hh, mm), tzinfo=tz.utc)
                    if window_start <= candidate < window_end:
                        due.append({
                            "conf": {"group": group, "row": row, "mode": mode},
                            "trigger_run_id": (
                                f"{_slug(group)}_{_slug(tenant)}_{mode}_"
                                f"{base_date.isoformat()}_{time_str.replace(':', '')}"
                            ),
                        })
        return due

    @task
    def flatten(lists_of_lists):
        """Pure data reshaping (no credentials touched) — safe to combine
        results from every group in one task, unlike the per-group Sheet
        reads above."""
        return [item for sub in lists_of_lists for item in sub]

    due_per_group = [find_due.override(task_id=f"find_due_{g}")(g) for g in _active_groups()]
    all_due = flatten(due_per_group)

    # Dynamic task mapping: the number of triggers isn't known until
    # find_due/flatten actually run, so this expands over all_due at
    # execution time rather than being a fixed operator in the graph.
    # skip_when_already_exists + the deterministic trigger_run_id from
    # find_due together deduplicate an overlapping tick re-matching the
    # same (tenant, mode, date, time) slot.
    TriggerDagRunOperator.partial(
        task_id="trigger_processor",
        trigger_dag_id="dst_dynamic_campaigns",
        skip_when_already_exists=True,
    ).expand_kwargs(all_due)
