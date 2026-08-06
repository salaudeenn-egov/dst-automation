"""
env.py — per-deployment-group secrets + environment isolation.

Each "group" (central / taraba / togo / ...) maps to one Airflow Variable
`dst_secrets_<group>` holding the exact env vars pipeline/*.py reads
directly (confirmed by grep across pipeline/config.py, notify.py, report.py):

    GOOGLE_CREDENTIALS_PATH, GOOGLE_SHEET_ID, GOOGLE_SHEET_TAB,
    ES_URL, ES_USER, ES_PASS, ES_INDEX_PREFIX,
    GOOGLE_DRIVE_FOLDER_ID, SLACK_TOKEN,
    GROQ_API_KEY, GROQ_MODEL, GROQ_BASE_URL (optional)

Do NOT put TEST_EXTRACT_DATE in a secrets blob — it's a dev-only override
that freezes extract_date and would silently corrupt production day-counting.

Multiple tenants can share one group (e.g. all "Nigeria States" tab rows
share dst_secrets_central) when they already share one ES cluster/creds —
this mirrors today's reality where tenants are only split into their own
.env/tab when they need genuinely different ES credentials, not by country.
"""
import os
from contextlib import contextmanager

from airflow.sdk import Variable

# Never let a stale/dev override leak into a task's environment via a
# secrets blob someone copy-pasted from a local .env.
_FORBIDDEN_KEYS = {"TEST_EXTRACT_DATE"}

# Sheet "active" column truthy values — shared so common/tasks.py and
# dst_campaign_scheduler.py can't drift apart on what "active" means.
ACTIVE_VALUES = ("TRUE", "YES", "1", "Y")


def group_secrets(group: str) -> dict:
    """Fetch the dst_secrets_<group> Airflow Variable as a dict."""
    raw = Variable.get(f"dst_secrets_{group}", deserialize_json=True)
    return {k: v for k, v in raw.items() if k not in _FORBIDDEN_KEYS}


@contextmanager
def tenant_env(group: str):
    """
    Apply this group's secrets to os.environ for the duration of the
    block, then restore os.environ exactly as it was — a defense-in-depth
    layer on top of AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER
    (which already gives each task a fresh interpreter process). Belt and
    suspenders: if that cluster-wide setting is ever disabled, a leak is
    bounded to one task's duration rather than silent and permanent.
    """
    secrets = group_secrets(group)
    snapshot = dict(os.environ)
    try:
        os.environ.update({k: str(v) for k, v in secrets.items()})
        yield secrets
    finally:
        for k in secrets:
            if k in snapshot:
                os.environ[k] = snapshot[k]
            else:
                os.environ.pop(k, None)
