"""Per-deployment-group configuration: which sheet tab to read and which
environment (ES credentials, index-prefix convention, Slack defaults) applies.

Groups exist because tenants are grouped BY CREDENTIAL SET, not by country:
all tenants sharing one ES login live on one sheet tab and form one group.

Configuration sources, resolved at TASK RUNTIME (never at DAG-parse time):
  - Airflow Variable "dst_groups": JSON list, e.g.
        [{"name": "nigeria_states", "sheet_tab": "Nigeria States",
          "env": {"ES_INDEX_PREFIX": null}},
         {"name": "togo", "sheet_tab": "togo",
          "env": {"ES_INDEX_PREFIX": "", "CDD_ROLE": "COMMUNITY_DISTRIBUTOR"}}]
    In "env": a null value REMOVES the variable (tenant-prefixed indices need
    ES_INDEX_PREFIX absent); "" sets it present-and-empty (Togo's un-prefixed
    cluster). This distinction is load-bearing — see pipeline/config.py.
  - Airflow Variable "dst_secrets_<name>": JSON dict of that group's secrets
    (ES_USER, ES_PASS, SLACK_TOKEN, ...), set via the Airflow UI, never in git.
  - With no dst_groups Variable, a single default group falls back to the
    process environment (.env) — local runs work with zero Airflow setup.
"""
import json
import logging
import os
from contextlib import contextmanager

log = logging.getLogger(__name__)


def _default_group():
    return {"name": "default",
            "sheet_tab": os.getenv("GOOGLE_SHEET_TAB", "Sheet1"),
            "env": {}}


def _get_airflow_variable(name):
    """Read an Airflow Variable from either execution context.

    Airflow 3 forbids direct metadata-DB access from task code, so inside a
    task only the Task SDK path works; outside tasks (CLI, dag-processor)
    only the ORM path works. Try both; return "" when unset everywhere.
    """
    try:
        from airflow.sdk import Variable as SdkVariable
        value = SdkVariable.get(name, default=None)
        if value is not None:
            return str(value)
    except Exception:
        pass
    try:
        from airflow.models import Variable as OrmVariable
        return OrmVariable.get(name, default_var="") or ""
    except Exception as e:
        log.warning(f"Airflow Variable '{name}' unreachable via SDK and ORM: {e}")
        return ""


def load_deployment_groups():
    """Return the configured groups, or the env-driven default group."""
    raw = _get_airflow_variable("dst_groups")
    if not raw.strip():
        log.info("dst_groups Variable not set — using the env-driven default group")
        return [_default_group()]

    groups = json.loads(raw)
    for group in groups:
        if not group.get("name") or not group.get("sheet_tab"):
            raise ValueError(f"dst_groups entry missing name/sheet_tab: {group}")
    return groups


def _load_group_secrets(group_name):
    raw = _get_airflow_variable(f"dst_secrets_{group_name}")
    if not raw.strip():
        log.warning(f"secrets Variable 'dst_secrets_{group_name}' is empty — "
                    f"tasks will rely on the process environment")
        return {}
    return json.loads(raw)


@contextmanager
def group_environment(group):
    """Apply one group's sheet tab, env overrides and secrets to os.environ,
    restoring the previous state afterwards — so one group's credentials can
    never leak into another group's task."""
    overrides = dict(group.get("env") or {})
    overrides["GOOGLE_SHEET_TAB"] = group.get("sheet_tab") or "Sheet1"
    if group.get("name") and group["name"] != "default":
        overrides.update(_load_group_secrets(group["name"]))

    snapshot = {key: os.environ.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)
        yield
    finally:
        for key, previous in snapshot.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def resolve_dst_mode():
    """The ONE universal, deployment-wide flag selecting how the system runs.

    DST_MODE=sheet (default)  — purely Google Sheet: config read from the tab,
                                run history on the Run Log tab, tenant lock and
                                retime guard in our own Airflow's Postgres.
    DST_MODE=mdms             — platform-integrated, ZERO database access:
                                config from the MDMS mirror (sheet fallback on
                                outage), run history as Kafka lifecycle events
                                for the platform's persister, no lock/guard
                                (deterministic run-ids prevent duplicates,
                                same trade the platform's own system makes).

    Set once per deployment in the environment — never per group or per DAG.
    """
    value = (os.getenv("DST_MODE") or "sheet").strip().lower()
    if value not in ("sheet", "mdms"):
        log.warning(f"unknown DST_MODE {value!r} — using sheet")
        return "sheet"
    return value
