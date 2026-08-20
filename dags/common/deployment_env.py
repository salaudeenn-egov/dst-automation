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


def mdms_enabled():
    """The ONE deployment-wide switch: is this an MDMS-backed deployment?

        DST_MDMS_ENABLED=false (default)  purely Google Sheet — config read from
                                          the tab, run history on the Run Log tab.
        DST_MDMS_ENABLED=true             platform-integrated — config from the
                                          MDMS mirror, run history as Kafka
                                          lifecycle events for the persister.

    Boolean rather than a mode string on purpose: the value space is two, so a
    typo cannot silently select a working-but-wrong mode. An unparseable value
    RAISES instead of defaulting, because the two modes read config from
    different places and write history to different places — quietly guessing
    is worse than a loud failure the operator can see in the task log.

    Either way the sheet is the fallback: if MDMS cannot be read the scheduler
    reads the tab for that tick, and if Kafka cannot be written the outcome is
    appended to the Run Log tab. A deployment with no MDMS write access still
    works end to end.

    Set once per deployment in the environment — never per group or per DAG.
    DST_MODE (sheet|mdms) is still honoured for deployments not yet migrated.
    """
    raw = os.getenv("DST_MDMS_ENABLED")
    if raw is None:
        legacy = (os.getenv("DST_MODE") or "").strip().lower()
        if legacy in ("mdms", "sheet"):
            log.info(f"using legacy DST_MODE={legacy}; prefer DST_MDMS_ENABLED")
            return legacy == "mdms"
        if legacy:
            raise ValueError(
                f"DST_MODE={legacy!r} is not 'sheet' or 'mdms'. Set "
                f"DST_MDMS_ENABLED=true|false instead.")
        return False

    value = str(raw).strip().lower()
    if value in ("true", "1", "yes", "y", "on"):
        return True
    if value in ("false", "0", "no", "n", "off", ""):
        return False
    raise ValueError(
        f"DST_MDMS_ENABLED={raw!r} is not a boolean. Use true or false — "
        f"refusing to guess, because the two modes read config from different "
        f"places and write run history to different places.")
