"""Slack alerting for DAG failures — the team must hear about a dead report
before the stakeholders ask where it is."""
import logging
import os

import requests

log = logging.getLogger(__name__)


def notify_slack_on_failure(context):
    """Airflow on_failure_callback: post what failed and why to Slack.
    Never raises — an alerting failure must not mask the original error."""
    try:
        ti = context.get("task_instance")
        exception = context.get("exception")
        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        row = conf.get("row") or {}

        state = row.get("state_name") or conf.get("state_name") or "?"
        channel = (str(row.get("slack_channel", "")).strip()
                   or os.getenv("SLACK_CHANNEL", ""))
        token = os.getenv("SLACK_TOKEN")

        message = (f"DST PIPELINE FAILURE [{state}] "
                   f"dag={getattr(ti, 'dag_id', '?')} task={getattr(ti, 'task_id', '?')}\n"
                   f"{type(exception).__name__ if exception else 'Failure'}: {exception}")
        log.error(message)

        if not token or not channel:
            log.warning("[alerts] SLACK_TOKEN/channel not set — alert logged only")
            return
        requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": message},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"[alerts] failure alert could not be sent: {e}")
