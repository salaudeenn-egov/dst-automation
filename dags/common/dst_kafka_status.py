"""Run lifecycle events to Kafka — the MDMS-mode audit path (zero database).

Adapted from the platform's common/kafka_status.py: same conventions (lazy
singleton producer, never raises, status_order ranking, tenant-prefixed topic
on central instances), our fields (mode, slot, Drive link instead of
FileStore id). Consumed by egov-persister via one platform-side config YAML
into a dst_report_metadata table — producers hold no DB credentials.

Env: KAFKA_BROKER (required to enable — silently skipped otherwise),
DST_RUNS_TOPIC (default save-dst-report-metadata),
IS_CENTRAL_INSTANCE_ENABLED (tenant-prefixes the topic when "true").
"""
import datetime
import json
import logging
import os
import uuid

log = logging.getLogger(__name__)

DEFAULT_TOPIC = "save-dst-report-metadata"

STATUS_ORDER = {
    "SCHEDULED": 10,
    "RUN_STARTED": 20,
    "SKIPPED": 40,
    "REPORT_COMPLETED": 40,
    "REPORT_FAILED": 40,
}

_producer = None
_producer_init_failed = False


def _get_producer():
    global _producer, _producer_init_failed
    if _producer is not None or _producer_init_failed:
        return _producer
    broker = os.getenv("KAFKA_BROKER", "").strip()
    if not broker:
        _producer_init_failed = True
        return None
    try:
        from kafka import KafkaProducer
        _producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"))
    except Exception:
        log.exception("[kafka] producer init failed — run events will be skipped")
        _producer_init_failed = True
        _producer = None
    return _producer


def _topic_for(tenant_id):
    base = os.getenv("DST_RUNS_TOPIC", DEFAULT_TOPIC)
    central = os.getenv("IS_CENTRAL_INSTANCE_ENABLED", "false").lower() == "true"
    return f"{tenant_id}-{base}" if central and tenant_id else base


def push_run_event(status, conf, dag_run_id, error_message=None,
                   drive_link="", day=""):
    """Publish one lifecycle event for a campaign run. Never raises —
    a Kafka hiccup must not fail a DAG task."""
    producer = _get_producer()
    if producer is None:
        log.info(f"[kafka] KAFKA_BROKER not set — skipping event {status}")
        return False

    row = conf.get("row") or {}
    tenant_id = conf.get("tenant", "")
    now = datetime.datetime.now(datetime.timezone.utc)
    event = {
        "event_id": str(uuid.uuid4()),
        "tenant_id": tenant_id,
        "state_name": conf.get("state_name", ""),
        "campaign_identifier": (row.get("campaign_number")
                                or row.get("project_type_id") or ""),
        "report_name": "dst_daily_report",
        "trigger_frequency": "DAILY",
        "mode": conf.get("mode", "both"),
        "slot_date": conf.get("slot_date", ""),
        "slot_time": conf.get("slot_time", ""),
        "day": str(day),
        "dag_run_id": dag_run_id,
        "dag_name": "dst_campaign_run",
        "status": status,
        "status_order": STATUS_ORDER.get(status, 0),
        "error_message": error_message,
        "drive_link": drive_link,
        "timestamp_ms": int(now.timestamp() * 1000),
        "timestamp": now.isoformat(),
    }
    topic = _topic_for(tenant_id)
    try:
        producer.send(topic, value=event)
        producer.flush(timeout=10)
        log.info(f"[kafka] pushed {status} for {tenant_id} -> {topic}")
        return True
    except Exception:
        log.exception(f"[kafka] failed to push {status} to {topic} (non-fatal)")
        return False
