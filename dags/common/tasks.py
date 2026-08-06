"""
common/tasks.py — TaskFlow callables for dst_dynamic_campaigns (the
Processor DAG). Unlike the earlier per-(tenant,time,mode) DAG-factory
design, this is now a SINGLE static DAG serving every tenant, so `group`,
`row`, and `mode` are no longer passed as XCom-threaded arguments between
tasks — every task reads them straight from `dag_run.conf` (via the
`dag_run` parameter, a known TaskFlow context key Airflow auto-injects).
dst_campaign_scheduler.py sets that conf when it dynamically triggers a
run: {"group": ..., "row": ..., "mode": ...}.

Every task still wraps its pipeline/*.py business-logic calls in
common.env.tenant_env(group) so credentials for one deployment group never
leak into another task's execution. pipeline/*.py itself is never modified.

Marker-dict protocol (unchanged from the previous design):
    raw None            -> the upstream task genuinely raised (no XCom
                           was ever pushed) -> a real failure to propagate
    {"ok": None, ...}   -> routine no-op (tenant inactive / outside
                           campaign window / another run already holds
                           this tenant's lock) -> not an error, do nothing
    {"ok": True, ...}   -> real success -> proceed

Default trigger_rule (ALL_SUCCESS) edges (check_active->run_analyze,
run_analyze->run_cdd_sync, run_report->run_notify) rely on Airflow's own
cascade to guarantee a task's body only runs if its upstream genuinely
succeeded — so those just check the upstream's marker dict directly, no
manual ti.xcom_pull() needed. Only the two trigger_rule=ALL_DONE edges
(run_cdd_sync->run_report, and the finalize task) need an explicit
`ti.xcom_pull(task_ids=...)` gate, since ALL_DONE deliberately bypasses
that cascade. There are no TaskGroups in this design (a prior TaskGroup
refactor pass silently broke these exact string-based task_id lookups by
renaming tasks — see git history — so this design keeps a flat task list
specifically to avoid that failure mode recurring).
"""
import importlib
import logging
from contextlib import contextmanager
from datetime import timedelta

from airflow.sdk import task
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.task.trigger_rule import TriggerRule

from common.env import tenant_env, ACTIVE_VALUES
from common.db import acquire_tenant_lock, release_tenant_lock, write_audit_row

log = logging.getLogger(__name__)

_ITN_DRUG_TYPES = {"ITN", "LLIN"}

_OK = {"ok": True}
_NOOP = {"ok": None}

# Data/logic errors (malformed Sheet row, missing expected field, a schema
# mismatch) aren't fixed by retrying — they'll fail identically every time.
# Only genuine infrastructure issues (network, service unavailable) should
# burn a task's retries, per the reference doc's retry policy ("retry only
# for infrastructure issues... no retries for script logic errors").
_NON_RETRYABLE_EXCEPTIONS = (KeyError, ValueError, TypeError, AttributeError)

_TIMEOUT_CHECK = timedelta(minutes=2)
_TIMEOUT_ANALYZE = timedelta(minutes=15)
_TIMEOUT_CDD_SYNC = timedelta(minutes=10)
_TIMEOUT_REPORT = timedelta(minutes=15)
_TIMEOUT_NOTIFY = timedelta(minutes=10)
_TIMEOUT_FINALIZE = timedelta(minutes=2)


def _pull(ti, upstream_task_id):
    """Returns (aborted, noop, value) for a marker-dict XCom from
    `upstream_task_id`. Only used for trigger_rule=ALL_DONE edges — see
    module docstring for why default-trigger_rule edges don't need this."""
    result = ti.xcom_pull(task_ids=upstream_task_id)
    if result is None:
        return True, False, None
    if result.get("ok") is None:
        return False, True, result
    return False, False, result


@contextmanager
def _cfg_scope(group: str, row: dict):
    """Applies the group's secrets for the duration of the block and
    yields a freshly-built cfg dict. Business logic (module.run(cfg))
    must execute INSIDE this scope, not after it — several pipeline
    modules (report.py's GROQ_API_KEY, notify.py's SLACK_TOKEN) read
    os.environ directly at call time rather than from cfg."""
    with tenant_env(group):
        from pipeline import config
        yield config.build(row)


def _run_stage(group: str, row: dict, default_mod_path: str, itn_mod_path: str):
    """Shared SPAQ/AZM-vs-ITN/LLIN module dispatch for the stages that
    just take cfg and return whatever mod.run(cfg) returns (analyze,
    cdd_sync, report) — avoids repeating the same import/dispatch/tenant_env
    boilerplate three times."""
    with _cfg_scope(group, row) as cfg:
        default_mod = importlib.import_module(default_mod_path)
        itn_mod = importlib.import_module(itn_mod_path)
        mod = itn_mod if cfg.get("drug_type") in _ITN_DRUG_TYPES else default_mod
        try:
            return mod.run(cfg)
        except _NON_RETRYABLE_EXCEPTIONS as e:
            raise AirflowFailException(f"non-retryable logic error in {mod.__name__}: {e}") from e


@task(execution_timeout=_TIMEOUT_CHECK)
def check_active(dag_run=None):
    """Re-fetch this row fresh from the Sheet so a last-minute active=FALSE
    edit is honored without waiting for the next scheduler tick. Also
    acquires this tenant's Postgres lock (see common/db.py) — if another
    run already holds it, this is a routine no-op, not a failure. Never
    raises for the routine "inactive today" case either — returns a no-op
    marker instead, so downstream tasks short-circuit cleanly rather than
    Airflow reporting a routine skip as a pipeline failure."""
    group = dag_run.conf["group"]
    row = dag_run.conf["row"]
    tenant = row.get("tenant", "?")
    state = row.get("state_name", "?")

    if not acquire_tenant_lock(tenant, dag_run.run_id):
        log.info(f"[{state}] another run already in progress for tenant={tenant} — routine no-op")
        return {**_NOOP, "row": None}

    with tenant_env(group):
        from pipeline import config
        fresh_rows = config.get_active_rows()
        match = next(
            (r for r in fresh_rows
             if r.get("state_name", "").strip().lower() == state.strip().lower()),
            None,
        )
        if match is None:
            log.info(f"[{state}] not found in sheet — routine no-op")
            return {**_NOOP, "row": None}
        if str(match.get("active", "")).strip().upper() not in ACTIVE_VALUES:
            log.info(f"[{state}] inactive — routine no-op")
            return {**_NOOP, "row": None}
        cfg = config.build(match)
        if not cfg["in_campaign_window"]:
            log.info(f"[{state}] outside campaign window — routine no-op")
            return {**_NOOP, "row": None}
    return {**_OK, "row": match}


@task(execution_timeout=_TIMEOUT_ANALYZE)
def run_analyze(active_ctx: dict, dag_run=None):
    """Reached only if check_active succeeded — default trigger_rule
    (ALL_SUCCESS) already marks this task upstream_failed without
    invoking the body if check_active genuinely failed, so active_ctx
    here is always check_active's real returned dict."""
    if active_ctx.get("ok") is None:
        return _NOOP
    group = dag_run.conf["group"]
    _run_stage(group, active_ctx["row"], "pipeline.analyze", "pipeline.analyze_itn")
    return _OK


@task(execution_timeout=_TIMEOUT_CDD_SYNC)
def run_cdd_sync(active_ctx: dict, analyze_result: dict, dag_run=None):
    """Allowed to genuinely fail + retry — unlike scheduler.py's swallow-
    and-log, this gets real Airflow-level visibility. run_report still
    proceeds regardless of a genuine cdd_sync failure (see its trigger_rule
    + explicit gate below). Reached only if run_analyze succeeded (same
    default-trigger_rule reasoning as run_analyze above)."""
    if analyze_result.get("ok") is None:
        return _NOOP
    group = dag_run.conf["group"]
    _run_stage(group, active_ctx["row"], "pipeline.cdd_sync", "pipeline.cdd_sync_itn")
    return _OK


@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=_TIMEOUT_REPORT)
def run_report(active_ctx: dict, dag_run=None, ti=None):
    """trigger_rule=ALL_DONE lets a genuine cdd_sync failure through, but
    analyze failing (or the routine inactive no-op) must still short-
    circuit — enforced by the explicit marker-dict gate below, since
    ALL_DONE deliberately bypasses the default-trigger_rule cascade that
    protects the other tasks (see module docstring)."""
    analyze_aborted, analyze_noop, _ = _pull(ti, "run_analyze")
    if analyze_aborted:
        raise AirflowException("run_analyze did not succeed — aborting report")
    if analyze_noop:
        return _NOOP

    cdd_aborted, _, _ = _pull(ti, "run_cdd_sync")
    if cdd_aborted:
        log.warning("cdd_sync failed — continuing to report anyway (non-fatal, matches prior behavior)")

    group = dag_run.conf["group"]
    docx, partner_docx, slack_text = _run_stage(
        group, active_ctx["row"], "pipeline.report", "pipeline.report_itn"
    )
    return {**_OK, "docx": docx, "partner_docx": partner_docx, "slack_text": slack_text}


@task(execution_timeout=_TIMEOUT_NOTIFY)
def run_notify(active_ctx: dict, report_result: dict, dag_run=None):
    """Reached only if run_report succeeded (default trigger_rule)."""
    if report_result.get("ok") is None:
        return _NOOP

    group = dag_run.conf["group"]
    mode = dag_run.conf["mode"]
    with _cfg_scope(group, active_ctx["row"]) as cfg:
        from pipeline import notify
        notify.run(
            cfg,
            report_result["docx"],
            report_result["slack_text"],
            partner_docx_path=report_result.get("partner_docx"),
            mode=mode,
        )
    return _OK


@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=_TIMEOUT_FINALIZE)
def finalize(dag_run=None, ti=None):
    """Always runs last regardless of upstream outcome:
      1. Releases this run's tenant lock (in a finally — must happen even
         if audit-writing or the cdd_sync-failure re-raise below blows up).
      2. Writes one dst_report_metadata audit row per REAL report attempt
         (skipped for the routine inactive-day no-op, matching the
         reference doc's "one record per generated report" semantics).
      3. Re-raises to surface a visible DAG-run failure + Slack alert
         (via on_failure_callback) for a genuine cdd_sync failure, even
         though run_report/run_notify tolerated it and completed anyway.
    """
    row = dag_run.conf["row"]
    mode = dag_run.conf["mode"]
    tenant = row.get("tenant", "?")
    state_name = row.get("state_name", "?")

    try:
        report_aborted, report_noop, report_result = _pull(ti, "run_report")
        cdd_aborted, _, _ = _pull(ti, "run_cdd_sync")

        if report_noop:
            return  # routine inactive day — nothing to audit

        if report_aborted:
            status, step_failed, error = "failed", "run_report", "see run_report task logs"
        else:
            notify_aborted, _, _ = _pull(ti, "run_notify")
            if notify_aborted:
                status, step_failed, error = "failed", "run_notify", "see run_notify task logs"
            else:
                status = "success"
                step_failed = None
                error = "cdd_sync failed (non-fatal)" if cdd_aborted else None

        write_audit_row(
            tenant=tenant,
            state_name=state_name,
            dag_run_id=dag_run.run_id,
            mode=mode,
            status=status,
            step_failed=step_failed,
            error=error,
            drive_link=(report_result or {}).get("docx"),
        )

        if cdd_aborted:
            raise AirflowException(
                f"[{state_name}] cdd_sync failed — see run_cdd_sync logs (report still generated)"
            )
    finally:
        release_tenant_lock(tenant, dag_run.run_id)


def dst_failure_slack_alert(context):
    """on_failure_callback — posts to Slack using the failing run's own
    group secrets (reuses pipeline/notify.py's _slack_post, not run.py).
    Reads group/row straight from dag_run.conf (this DAG is single/static,
    so unlike the old per-tenant DAG-factory design there's no dag.dst_group
    attribute to stamp — conf already carries everything needed)."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    conf = (context.get("dag_run").conf if context.get("dag_run") else {}) or {}
    group = conf.get("group")
    row = conf.get("row") or {}

    message = (
        f"DST PIPELINE FAILURE\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"State: {row.get('state_name', '?')}\n"
        f"Log: {context['task_instance'].log_url}"
    )
    if not group:
        log.error(f"[alert] no group context available — {message}")
        return
    try:
        with tenant_env(group) as secrets:
            from pipeline.notify import _slack_post
            channel = row.get("slack_channel") or secrets.get("SLACK_CHANNEL", "")
            token = secrets.get("SLACK_TOKEN", "")
            if channel and token:
                _slack_post(channel, message, token)
    except Exception as e:
        log.error(f"[alert] Slack alert post failed: {e}")
